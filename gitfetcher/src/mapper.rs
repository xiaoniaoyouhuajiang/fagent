use std::{collections::HashMap, convert::TryFrom, path::Path, sync::Arc};

use crate::readme::{chunk_readme, ReadmeChunkPiece};
use ast::lang::asg::NodeData;
use ast::lang::graphs::{BTreeMapGraph, EdgeType, Node as AstNode, NodeType};
use chrono::{DateTime, Utc};
use deltalake::arrow::{
    array::{Int64Array, StringArray, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
    record_batch::RecordBatch,
};
use fstorage::{
    embedding::EmbeddingProvider,
    errors::{Result as StorageResult, StorageError},
    fetch::Fetchable,
    fetch::GraphData,
    schemas::generated_schemas::{
        Calls, Class, Commit, Contains, DataModel, DependsOn, Endpoint, File, Function, Handler,
        HasVersion, Implements, Imports, IsCommit, Library, NestedIn, Operand, ParentOf, Project,
        ReadmeChunk, Test, Trait, Uses, Variable, Version,
    },
    utils::id::{stable_edge_id_u128, stable_node_id_u128},
};
use uuid::Uuid;

use crate::{
    code_workspace::{prepare_workspace, WorkspaceConfig},
    models::{RepoSnapshot, RepositoryInfo, SearchRepository},
    params::RepoSnapshotParams,
};

const README_MAX_LINES_PER_CHUNK: usize = 120;

pub async fn build_repo_snapshot_graph(
    snapshot: &RepoSnapshot,
    params: &RepoSnapshotParams,
    embedding_provider: Arc<dyn EmbeddingProvider>,
) -> StorageResult<GraphData> {
    let repo = &snapshot.repository;
    let commit = &snapshot.commit;
    let revision = &snapshot.revision;

    let project_url = repo.html_url.clone();

    let mut graph = GraphData::new();

    graph.add_entities(vec![Project {
        url: Some(project_url.clone()),
        name: Some(repo.name.clone()),
        description: repo.description.clone(),
        language: repo.language.clone(),
        stars: Some(repo.stargazers as i64),
        forks: Some(repo.forks as i64),
    }]);

    graph.add_entities(vec![Version {
        sha: Some(revision.sha.clone()),
        tag: params.rev.clone(),
        is_head: Some(revision.is_head),
        created_at: Some(commit.authored_at),
    }]);

    graph.add_entities(vec![Commit {
        sha: Some(commit.sha.clone()),
        message: Some(commit.message.clone()),
        committed_at: Some(commit.authored_at),
    }]);

    let project_node_id = uuid_from_node(Project::ENTITY_TYPE, &[("url", project_url.clone())]);
    let version_node_id = uuid_from_node(Version::ENTITY_TYPE, &[("sha", revision.sha.clone())]);
    let commit_node_id = uuid_from_node(Commit::ENTITY_TYPE, &[("sha", commit.sha.clone())]);

    graph.add_entities(vec![HasVersion {
        id: Some(uuid_from_edge(
            HasVersion::ENTITY_TYPE,
            &project_node_id,
            &version_node_id,
        )),
        from_node_id: Some(project_node_id.clone()),
        to_node_id: Some(version_node_id.clone()),
        from_node_type: Some(Project::ENTITY_TYPE.to_string()),
        to_node_type: Some(Version::ENTITY_TYPE.to_string()),
        created_at: Some(commit.authored_at),
        updated_at: Some(commit.authored_at),
    }]);

    graph.add_entities(vec![IsCommit {
        id: Some(uuid_from_edge(
            IsCommit::ENTITY_TYPE,
            &version_node_id,
            &commit_node_id,
        )),
        from_node_id: Some(version_node_id.clone()),
        to_node_id: Some(commit_node_id),
        from_node_type: Some(Version::ENTITY_TYPE.to_string()),
        to_node_type: Some(Commit::ENTITY_TYPE.to_string()),
        created_at: Some(commit.authored_at),
        updated_at: Some(commit.authored_at),
    }]);

    if params.include_readme {
        if let Some(readme) = &snapshot.readme {
            let chunk_pieces = chunk_readme(&readme.text, README_MAX_LINES_PER_CHUNK);
            let chunk_texts: Vec<String> = chunk_pieces
                .iter()
                .map(|piece| piece.text.clone())
                .collect();

            let embeddings: Vec<Vec<f32>> = if chunk_texts.is_empty() {
                Vec::new()
            } else {
                embedding_provider
                    .embed(chunk_texts)
                    .await?
                    .into_iter()
                    .map(|values| values.into_iter().map(|v| v as f32).collect())
                    .collect()
            };

            let embedding_model = detect_embedding_model_from_env();

            let mut readme_chunks = Vec::with_capacity(chunk_pieces.len());
            for (idx, piece) in chunk_pieces.into_iter().enumerate() {
                let ReadmeChunkPiece {
                    start_line,
                    end_line,
                    text: chunk_text,
                } = piece;

                let embedding = embeddings.get(idx).cloned().filter(|vec| !vec.is_empty());
                let embedding_model_value =
                    embedding.as_ref().and_then(|_| embedding_model.clone());
                let token_count = approximate_token_count(&chunk_text);

                let embedding_id =
                    embedding_identifier(&repo.full_name, &revision.sha, start_line, end_line, idx);

                readme_chunks.push(ReadmeChunk {
                    id: None,
                    project_url: Some(project_url.clone()),
                    revision_sha: Some(revision.sha.clone()),
                    source_file: Some(readme.source_file.clone()),
                    start_line: Some(start_line),
                    end_line: Some(end_line),
                    text: Some(chunk_text),
                    embedding,
                    embedding_model: embedding_model_value,
                    embedding_id: Some(embedding_id),
                    token_count,
                    chunk_order: Some(idx as i32),
                    created_at: Some(commit.authored_at),
                    updated_at: None,
                });
            }

            if !readme_chunks.is_empty() {
                graph.add_entities(readme_chunks);
            }
        }
    }

    if params.include_code {
        append_code_graph(&mut graph, snapshot, &version_node_id).await?;
    }

    Ok(graph)
}

async fn append_code_graph(
    graph: &mut GraphData,
    snapshot: &RepoSnapshot,
    version_node_id: &str,
) -> StorageResult<()> {
    let repo = &snapshot.repository;
    let clone_source = repo_clone_source(repo);
    let workspace = prepare_workspace(WorkspaceConfig {
        repo_url: &clone_source,
        display_name: &repo.full_name,
        revision: &snapshot.revision.sha,
        enable_incremental_filter: false,
    })
    .await?;

    let code_graph = workspace.build_graph().await?;
    let version_descriptor = NodeDescriptor::new(Version::ENTITY_TYPE, version_node_id.to_string());
    let repo_root = workspace.repo_root();
    translate_ast_graph(
        graph,
        &code_graph,
        snapshot.commit.authored_at,
        &snapshot.revision.sha,
        &version_descriptor,
        repo_root,
    )?;
    Ok(())
}

fn repo_clone_source(repo: &RepositoryInfo) -> String {
    let url = repo.html_url.trim();
    if url.starts_with("http://") || url.starts_with("https://") || url.starts_with("git@") {
        if url.ends_with(".git") {
            url.to_string()
        } else {
            format!("{url}.git")
        }
    } else {
        url.to_string()
    }
}

#[derive(Clone)]
struct NodeDescriptor {
    entity_type: &'static str,
    node_id: String,
}

impl NodeDescriptor {
    fn new(entity_type: &'static str, node_id: String) -> Self {
        Self {
            entity_type,
            node_id,
        }
    }
}

#[derive(Default)]
struct NodeBuckets {
    files: Vec<File>,
    classes: Vec<Class>,
    traits: Vec<Trait>,
    functions: Vec<Function>,
    data_models: Vec<DataModel>,
    variables: Vec<Variable>,
    tests: Vec<Test>,
    endpoints: Vec<Endpoint>,
    libraries: Vec<Library>,
}

impl NodeBuckets {
    fn flush(self, graph: &mut GraphData) {
        if !self.files.is_empty() {
            graph.add_entities(self.files);
        }
        if !self.classes.is_empty() {
            graph.add_entities(self.classes);
        }
        if !self.traits.is_empty() {
            graph.add_entities(self.traits);
        }
        if !self.functions.is_empty() {
            graph.add_entities(self.functions);
        }
        if !self.data_models.is_empty() {
            graph.add_entities(self.data_models);
        }
        if !self.variables.is_empty() {
            graph.add_entities(self.variables);
        }
        if !self.tests.is_empty() {
            graph.add_entities(self.tests);
        }
        if !self.endpoints.is_empty() {
            graph.add_entities(self.endpoints);
        }
        if !self.libraries.is_empty() {
            graph.add_entities(self.libraries);
        }
    }
}

#[derive(Default)]
struct EdgeBuckets {
    contains: Vec<Contains>,
    calls: Vec<Calls>,
    uses: Vec<Uses>,
    operand: Vec<Operand>,
    handler: Vec<Handler>,
    parent_of: Vec<ParentOf>,
    implements: Vec<Implements>,
    nested_in: Vec<NestedIn>,
    imports: Vec<Imports>,
    depends_on: Vec<DependsOn>,
}

impl EdgeBuckets {
    fn flush(self, graph: &mut GraphData) {
        if !self.contains.is_empty() {
            graph.add_entities(self.contains);
        }
        if !self.calls.is_empty() {
            graph.add_entities(self.calls);
        }
        if !self.uses.is_empty() {
            graph.add_entities(self.uses);
        }
        if !self.operand.is_empty() {
            graph.add_entities(self.operand);
        }
        if !self.handler.is_empty() {
            graph.add_entities(self.handler);
        }
        if !self.parent_of.is_empty() {
            graph.add_entities(self.parent_of);
        }
        if !self.implements.is_empty() {
            graph.add_entities(self.implements);
        }
        if !self.nested_in.is_empty() {
            graph.add_entities(self.nested_in);
        }
        if !self.imports.is_empty() {
            graph.add_entities(self.imports);
        }
        if !self.depends_on.is_empty() {
            graph.add_entities(self.depends_on);
        }
    }
}

enum MappedNode {
    File(File, NodeDescriptor),
    Class(Class, NodeDescriptor),
    Trait(Trait, NodeDescriptor),
    Function(Function, NodeDescriptor),
    DataModel(DataModel, NodeDescriptor),
    Variable(Variable, NodeDescriptor),
    Test(Test, NodeDescriptor),
    Endpoint(Endpoint, NodeDescriptor),
    Library(Library, NodeDescriptor),
}

fn translate_ast_graph(
    graph: &mut GraphData,
    code_graph: &BTreeMapGraph,
    commit_ts: DateTime<Utc>,
    version_sha: &str,
    version_descriptor: &NodeDescriptor,
    repo_root: &Path,
) -> StorageResult<()> {
    let mut descriptors: HashMap<String, NodeDescriptor> = HashMap::new();
    let mut nodes = NodeBuckets::default();

    for (key, node) in &code_graph.nodes {
        if let Some(mapped) = map_ast_node(node, version_sha, repo_root) {
            match mapped {
                MappedNode::File(value, descriptor) => {
                    descriptors.insert(key.clone(), descriptor);
                    nodes.files.push(value);
                }
                MappedNode::Class(value, descriptor) => {
                    descriptors.insert(key.clone(), descriptor);
                    nodes.classes.push(value);
                }
                MappedNode::Trait(value, descriptor) => {
                    descriptors.insert(key.clone(), descriptor);
                    nodes.traits.push(value);
                }
                MappedNode::Function(value, descriptor) => {
                    descriptors.insert(key.clone(), descriptor);
                    nodes.functions.push(value);
                }
                MappedNode::DataModel(value, descriptor) => {
                    descriptors.insert(key.clone(), descriptor);
                    nodes.data_models.push(value);
                }
                MappedNode::Variable(value, descriptor) => {
                    descriptors.insert(key.clone(), descriptor);
                    nodes.variables.push(value);
                }
                MappedNode::Test(value, descriptor) => {
                    descriptors.insert(key.clone(), descriptor);
                    nodes.tests.push(value);
                }
                MappedNode::Endpoint(value, descriptor) => {
                    descriptors.insert(key.clone(), descriptor);
                    nodes.endpoints.push(value);
                }
                MappedNode::Library(value, descriptor) => {
                    descriptors.insert(key.clone(), descriptor);
                    nodes.libraries.push(value);
                }
            }
        }
    }

    nodes.flush(graph);

    let mut edges = EdgeBuckets::default();

    for descriptor in descriptors.values() {
        if descriptor.entity_type == File::ENTITY_TYPE {
            edges
                .contains
                .push(make_contains(version_descriptor, descriptor, commit_ts));
        }
    }

    for (source_key, target_key, edge_type) in &code_graph.edges {
        let Some(source_desc) = descriptors.get(source_key) else {
            continue;
        };
        let Some(target_desc) = descriptors.get(target_key) else {
            continue;
        };
        match edge_type {
            EdgeType::Contains => {
                push_contains_edge(&mut edges, source_desc, target_desc, commit_ts);
            }
            EdgeType::Calls => {
                edges
                    .calls
                    .push(make_calls(source_desc, target_desc, commit_ts));
            }
            EdgeType::Uses => {
                edges
                    .uses
                    .push(make_uses(source_desc, target_desc, commit_ts));
            }
            EdgeType::Operand => {
                edges
                    .operand
                    .push(make_operand(source_desc, target_desc, commit_ts));
            }
            EdgeType::Handler => {
                edges
                    .handler
                    .push(make_handler(source_desc, target_desc, commit_ts));
            }
            EdgeType::ParentOf => {
                edges
                    .parent_of
                    .push(make_parent_of(source_desc, target_desc, commit_ts));
            }
            EdgeType::Implements => {
                edges
                    .implements
                    .push(make_implements(source_desc, target_desc, commit_ts));
            }
            EdgeType::NestedIn => {
                edges
                    .nested_in
                    .push(make_nested_in(source_desc, target_desc, commit_ts));
            }
            EdgeType::Imports => {
                edges
                    .imports
                    .push(make_imports(source_desc, target_desc, commit_ts));
            }
            _ => {}
        }
    }

    edges.flush(graph);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ast::lang::graphs::Edge;
    use ast::lang::Graph;
    use chrono::Utc;

    #[test]
    fn translate_ast_graph_maps_nodes_and_edges() {
        let mut graph = GraphData::new();
        let mut code_graph = BTreeMapGraph::default();

        let mut file_node = NodeData::default();
        file_node.name = "src/lib.rs".into();
        file_node.file = "src/lib.rs".into();

        let mut function_node = NodeData::default();
        function_node.name = "greet".into();
        function_node.file = "src/lib.rs".into();
        function_node.start = 0;
        function_node.end = 1;

        let mut datamodel_node = NodeData::default();
        datamodel_node.name = "User".into();
        datamodel_node.file = "src/lib.rs".into();

        let mut library_node = NodeData::default();
        library_node.name = "serde".into();

        code_graph.add_node(NodeType::File, file_node.clone());
        code_graph.add_node(NodeType::Function, function_node.clone());
        code_graph.add_node(NodeType::DataModel, datamodel_node.clone());
        code_graph.add_node(NodeType::Library, library_node.clone());
        let contains_edge = Edge::contains(
            NodeType::File,
            &file_node,
            NodeType::Function,
            &function_node,
        );
        let file_datamodel_edge = Edge::contains(
            NodeType::File,
            &file_node,
            NodeType::DataModel,
            &datamodel_node,
        );
        let function_datamodel_edge = Edge::contains(
            NodeType::Function,
            &function_node,
            NodeType::DataModel,
            &datamodel_node,
        );
        let file_library_edge =
            Edge::contains(NodeType::File, &file_node, NodeType::Library, &library_node);
        code_graph.add_edge(contains_edge);
        code_graph.add_edge(file_datamodel_edge);
        code_graph.add_edge(function_datamodel_edge);
        code_graph.add_edge(file_library_edge);

        let version_descriptor = NodeDescriptor::new(
            Version::ENTITY_TYPE,
            uuid_from_node(Version::ENTITY_TYPE, &[("sha", "deadbeef".to_string())]),
        );
        translate_ast_graph(
            &mut graph,
            &code_graph,
            Utc::now(),
            "deadbeef",
            &version_descriptor,
            Path::new("/dummy/repo"),
        )
        .expect("translate");

        let mut entity_types: Vec<_> = graph
            .entities
            .iter()
            .map(|entity| entity.entity_type_any())
            .collect();
        entity_types.sort();

        assert!(entity_types.contains(&File::ENTITY_TYPE));
        assert!(entity_types.contains(&Function::ENTITY_TYPE));
        assert!(entity_types.contains(&Contains::ENTITY_TYPE));

        use deltalake::arrow::array::{Array, StringArray};
        let mut contain_pairs = Vec::new();
        let mut depends_pairs = Vec::new();

        for entity in &graph.entities {
            let entity_type = entity.entity_type_any();
            if entity_type == Contains::ENTITY_TYPE {
                let batch = entity.to_record_batch_any().expect("contains batch");
                let schema = batch.schema();
                let from_idx = schema.index_of("from_node_type").expect("from_node_type");
                let to_idx = schema.index_of("to_node_type").expect("to_node_type");
                let from_col = batch
                    .column(from_idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("StringArray");
                let to_col = batch
                    .column(to_idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("StringArray");
                for i in 0..batch.num_rows() {
                    if from_col.is_null(i) || to_col.is_null(i) {
                        continue;
                    }
                    contain_pairs
                        .push((from_col.value(i).to_string(), to_col.value(i).to_string()));
                }
            } else if entity_type == DependsOn::ENTITY_TYPE {
                let batch = entity.to_record_batch_any().expect("depends_on batch");
                let schema = batch.schema();
                let from_idx = schema.index_of("from_node_type").expect("from_node_type");
                let to_idx = schema.index_of("to_node_type").expect("to_node_type");
                let from_col = batch
                    .column(from_idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("StringArray");
                let to_col = batch
                    .column(to_idx)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("StringArray");
                for i in 0..batch.num_rows() {
                    if from_col.is_null(i) || to_col.is_null(i) {
                        continue;
                    }
                    depends_pairs
                        .push((from_col.value(i).to_string(), to_col.value(i).to_string()));
                }
            }
        }

        assert!(
            contain_pairs.contains(&("version".to_string(), "file".to_string())),
            "expected version->file contains edge"
        );
        assert!(
            contain_pairs.contains(&("file".to_string(), "function".to_string())),
            "expected file->function contains edge"
        );
        assert!(
            contain_pairs.contains(&("file".to_string(), "datamodel".to_string())),
            "expected file->datamodel contains edge"
        );
        assert!(
            !contain_pairs.contains(&("function".to_string(), "datamodel".to_string())),
            "function->datamodel should not be emitted as CONTAINS"
        );
        assert!(
            depends_pairs.contains(&("file".to_string(), "library".to_string())),
            "expected file->library depends_on edge"
        );
    }
}

fn map_ast_node(node: &AstNode, version_sha: &str, repo_root: &Path) -> Option<MappedNode> {
    match node.node_type {
        NodeType::File => {
            let raw_path = optional_string(&node.node_data.file)
                .or_else(|| optional_string(&node.node_data.name))?;
            let language = meta_value(&node.node_data, "language");
            let version_sha_owned = version_sha.to_owned();
            let path = normalize_file_path(&raw_path, repo_root);
            let descriptor = NodeDescriptor::new(
                File::ENTITY_TYPE,
                uuid_from_node(
                    File::ENTITY_TYPE,
                    &[
                        ("version_sha", version_sha_owned.clone()),
                        ("path", path.clone()),
                    ],
                ),
            );
            Some(MappedNode::File(
                File {
                    version_sha: Some(version_sha_owned),
                    path: Some(path),
                    language,
                },
                descriptor,
            ))
        }
        NodeType::Class => {
            let name = optional_string(&node.node_data.name)?;
            let (start_line, end_line) = line_bounds(&node.node_data);
            let raw_file_path = node_file_path(&node.node_data)?;
            let file_path = normalize_file_path(&raw_file_path, repo_root);
            let version_sha_owned = version_sha.to_owned();
            let descriptor = NodeDescriptor::new(
                Class::ENTITY_TYPE,
                uuid_from_node(
                    Class::ENTITY_TYPE,
                    &[
                        ("version_sha", version_sha_owned.clone()),
                        ("file_path", file_path.clone()),
                        ("name", name.clone()),
                    ],
                ),
            );
            Some(MappedNode::Class(
                Class {
                    version_sha: Some(version_sha_owned),
                    file_path: Some(file_path),
                    name: Some(name),
                    start_line,
                    end_line,
                },
                descriptor,
            ))
        }
        NodeType::Trait => {
            let name = optional_string(&node.node_data.name)?;
            let (start_line, end_line) = line_bounds(&node.node_data);
            let raw_file_path = node_file_path(&node.node_data)?;
            let file_path = normalize_file_path(&raw_file_path, repo_root);
            let version_sha_owned = version_sha.to_owned();
            let descriptor = NodeDescriptor::new(
                Trait::ENTITY_TYPE,
                uuid_from_node(
                    Trait::ENTITY_TYPE,
                    &[
                        ("version_sha", version_sha_owned.clone()),
                        ("file_path", file_path.clone()),
                        ("name", name.clone()),
                    ],
                ),
            );
            Some(MappedNode::Trait(
                Trait {
                    version_sha: Some(version_sha_owned),
                    file_path: Some(file_path),
                    name: Some(name),
                    start_line,
                    end_line,
                },
                descriptor,
            ))
        }
        NodeType::Function => {
            let name = optional_string(&node.node_data.name)?;
            let (start_line, end_line) = line_bounds(&node.node_data);
            let signature = meta_value(&node.node_data, "signature")
                .or_else(|| meta_value(&node.node_data, "interface"));
            let is_component = bool_from_meta(&node.node_data, "component");
            let raw_file_path = node_file_path(&node.node_data)?;
            let file_path = normalize_file_path(&raw_file_path, repo_root);
            let version_sha_owned = version_sha.to_owned();
            let descriptor = NodeDescriptor::new(
                Function::ENTITY_TYPE,
                uuid_from_node(
                    Function::ENTITY_TYPE,
                    &[
                        ("version_sha", version_sha_owned.clone()),
                        ("file_path", file_path.clone()),
                        ("name", name.clone()),
                    ],
                ),
            );
            Some(MappedNode::Function(
                Function {
                    version_sha: Some(version_sha_owned),
                    file_path: Some(file_path),
                    name: Some(name),
                    signature,
                    start_line,
                    end_line,
                    is_component,
                },
                descriptor,
            ))
        }
        NodeType::DataModel => {
            let name = optional_string(&node.node_data.name)?;
            let (start_line, end_line) = line_bounds(&node.node_data);
            let construct = meta_value(&node.node_data, "construct")
                .or_else(|| meta_value(&node.node_data, "type"));
            let raw_file_path = node_file_path(&node.node_data)?;
            let file_path = normalize_file_path(&raw_file_path, repo_root);
            let version_sha_owned = version_sha.to_owned();
            let descriptor = NodeDescriptor::new(
                DataModel::ENTITY_TYPE,
                uuid_from_node(
                    DataModel::ENTITY_TYPE,
                    &[
                        ("version_sha", version_sha_owned.clone()),
                        ("file_path", file_path.clone()),
                        ("name", name.clone()),
                    ],
                ),
            );
            Some(MappedNode::DataModel(
                DataModel {
                    version_sha: Some(version_sha_owned),
                    file_path: Some(file_path),
                    name: Some(name),
                    construct,
                    start_line,
                    end_line,
                },
                descriptor,
            ))
        }
        NodeType::Var => {
            let name = optional_string(&node.node_data.name)?;
            let raw_file_path = node_file_path(&node.node_data)?;
            let file_path = normalize_file_path(&raw_file_path, repo_root);
            let version_sha_owned = version_sha.to_owned();
            let descriptor = NodeDescriptor::new(
                Variable::ENTITY_TYPE,
                uuid_from_node(
                    Variable::ENTITY_TYPE,
                    &[
                        ("version_sha", version_sha_owned.clone()),
                        ("file_path", file_path.clone()),
                        ("name", name.clone()),
                    ],
                ),
            );
            Some(MappedNode::Variable(
                Variable {
                    version_sha: Some(version_sha_owned),
                    file_path: Some(file_path),
                    name: Some(name),
                    data_type: node.node_data.data_type.clone(),
                },
                descriptor,
            ))
        }
        NodeType::UnitTest | NodeType::IntegrationTest | NodeType::E2eTest => {
            let name = optional_string(&node.node_data.name)?;
            let (start_line, end_line) = line_bounds(&node.node_data);
            let raw_file_path = node_file_path(&node.node_data)?;
            let file_path = normalize_file_path(&raw_file_path, repo_root);
            let version_sha_owned = version_sha.to_owned();
            let test_kind = match node.node_type {
                NodeType::UnitTest => "unit",
                NodeType::IntegrationTest => "integration",
                NodeType::E2eTest => "e2e",
                _ => unreachable!(),
            }
            .to_string();
            let descriptor = NodeDescriptor::new(
                Test::ENTITY_TYPE,
                uuid_from_node(
                    Test::ENTITY_TYPE,
                    &[
                        ("version_sha", version_sha_owned.clone()),
                        ("file_path", file_path.clone()),
                        ("name", name.clone()),
                    ],
                ),
            );
            Some(MappedNode::Test(
                Test {
                    version_sha: Some(version_sha_owned),
                    file_path: Some(file_path),
                    name: Some(name),
                    test_kind: Some(test_kind),
                    start_line,
                    end_line,
                },
                descriptor,
            ))
        }
        NodeType::Endpoint => {
            let path = meta_value(&node.node_data, "path")
                .or_else(|| optional_string(&node.node_data.name))?;
            let http_method = meta_value(&node.node_data, "verb");
            let raw_file_path = node_file_path(&node.node_data)?;
            let file_path = normalize_file_path(&raw_file_path, repo_root);
            let version_sha_owned = version_sha.to_owned();
            let descriptor = NodeDescriptor::new(
                Endpoint::ENTITY_TYPE,
                uuid_from_node(
                    Endpoint::ENTITY_TYPE,
                    &[
                        ("version_sha", version_sha_owned.clone()),
                        ("file_path", file_path.clone()),
                        ("path", path.clone()),
                    ],
                ),
            );
            Some(MappedNode::Endpoint(
                Endpoint {
                    version_sha: Some(version_sha_owned),
                    file_path: Some(file_path),
                    path: Some(path),
                    http_method,
                },
                descriptor,
            ))
        }
        NodeType::Library => {
            let name = optional_string(&node.node_data.name)?;
            let version = meta_value(&node.node_data, "version");
            let descriptor = NodeDescriptor::new(
                Library::ENTITY_TYPE,
                uuid_from_node(Library::ENTITY_TYPE, &[("name", name.clone())]),
            );
            Some(MappedNode::Library(
                Library {
                    name: Some(name),
                    version,
                },
                descriptor,
            ))
        }
        _ => None,
    }
}

fn line_bounds(data: &NodeData) -> (Option<i32>, Option<i32>) {
    (line_number(data.start), line_number(data.end))
}

fn line_number(value: usize) -> Option<i32> {
    value.checked_add(1).and_then(|v| i32::try_from(v).ok())
}

fn meta_value(data: &NodeData, key: &str) -> Option<String> {
    data.meta.get(key).cloned()
}

fn normalize_file_path(file_path: &str, repo_root: &Path) -> String {
    let path = Path::new(file_path);
    if let Ok(relative_path) = path.strip_prefix(repo_root) {
        relative_path.to_string_lossy().to_string()
    } else {
        file_path.to_string()
    }
}

fn node_file_path(data: &NodeData) -> Option<String> {
    optional_string(&data.file)
        .or_else(|| meta_value(data, "file_path"))
        .or_else(|| meta_value(data, "file"))
}

fn bool_from_meta(data: &NodeData, key: &str) -> Option<bool> {
    meta_value(data, key).map(|value| {
        let normalized = value.to_lowercase();
        matches!(normalized.as_str(), "1" | "true" | "yes" | "y")
    })
}

fn optional_string(value: &str) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value.to_string())
    }
}

struct EdgeBase {
    id: String,
    from_node_id: String,
    to_node_id: String,
    from_node_type: String,
    to_node_type: String,
    created_at: Option<DateTime<Utc>>,
}

fn edge_base(
    edge_entity_type: &str,
    from: &NodeDescriptor,
    to: &NodeDescriptor,
    created_at: DateTime<Utc>,
) -> EdgeBase {
    EdgeBase {
        id: uuid_from_edge(edge_entity_type, &from.node_id, &to.node_id),
        from_node_id: from.node_id.clone(),
        to_node_id: to.node_id.clone(),
        from_node_type: from.entity_type.to_string(),
        to_node_type: to.entity_type.to_string(),
        created_at: Some(created_at),
    }
}

fn push_contains_edge(
    edges: &mut EdgeBuckets,
    from: &NodeDescriptor,
    to: &NodeDescriptor,
    created_at: DateTime<Utc>,
) {
    let from_type = from.entity_type;
    let to_type = to.entity_type;

    if from_type == Version::ENTITY_TYPE && to_type == File::ENTITY_TYPE {
        edges.contains.push(make_contains(from, to, created_at));
        return;
    }

    if from_type == File::ENTITY_TYPE {
        if to_type == Class::ENTITY_TYPE
            || to_type == Function::ENTITY_TYPE
            || to_type == DataModel::ENTITY_TYPE
            || to_type == Variable::ENTITY_TYPE
            || to_type == Test::ENTITY_TYPE
            || to_type == Endpoint::ENTITY_TYPE
        {
            edges.contains.push(make_contains(from, to, created_at));
            return;
        }

        if to_type == Library::ENTITY_TYPE {
            edges.depends_on.push(make_depends_on(from, to, created_at));
            return;
        }
    }
}

fn make_contains(
    from: &NodeDescriptor,
    to: &NodeDescriptor,
    created_at: DateTime<Utc>,
) -> Contains {
    let base = edge_base(Contains::ENTITY_TYPE, from, to, created_at);
    Contains {
        id: Some(base.id),
        from_node_id: Some(base.from_node_id),
        to_node_id: Some(base.to_node_id),
        from_node_type: Some(base.from_node_type),
        to_node_type: Some(base.to_node_type),
        created_at: base.created_at,
        updated_at: None,
    }
}

fn make_depends_on(
    from: &NodeDescriptor,
    to: &NodeDescriptor,
    created_at: DateTime<Utc>,
) -> DependsOn {
    let base = edge_base(DependsOn::ENTITY_TYPE, from, to, created_at);
    DependsOn {
        id: Some(base.id),
        from_node_id: Some(base.from_node_id),
        to_node_id: Some(base.to_node_id),
        from_node_type: Some(base.from_node_type),
        to_node_type: Some(base.to_node_type),
        created_at: base.created_at,
        updated_at: None,
    }
}

fn make_calls(from: &NodeDescriptor, to: &NodeDescriptor, created_at: DateTime<Utc>) -> Calls {
    let base = edge_base(Calls::ENTITY_TYPE, from, to, created_at);
    Calls {
        id: Some(base.id),
        from_node_id: Some(base.from_node_id),
        to_node_id: Some(base.to_node_id),
        from_node_type: Some(base.from_node_type),
        to_node_type: Some(base.to_node_type),
        created_at: base.created_at,
        updated_at: None,
    }
}

fn make_uses(from: &NodeDescriptor, to: &NodeDescriptor, created_at: DateTime<Utc>) -> Uses {
    let base = edge_base(Uses::ENTITY_TYPE, from, to, created_at);
    Uses {
        id: Some(base.id),
        from_node_id: Some(base.from_node_id),
        to_node_id: Some(base.to_node_id),
        from_node_type: Some(base.from_node_type),
        to_node_type: Some(base.to_node_type),
        created_at: base.created_at,
        updated_at: None,
    }
}

fn make_operand(from: &NodeDescriptor, to: &NodeDescriptor, created_at: DateTime<Utc>) -> Operand {
    let base = edge_base(Operand::ENTITY_TYPE, from, to, created_at);
    Operand {
        id: Some(base.id),
        from_node_id: Some(base.from_node_id),
        to_node_id: Some(base.to_node_id),
        from_node_type: Some(base.from_node_type),
        to_node_type: Some(base.to_node_type),
        created_at: base.created_at,
        updated_at: None,
    }
}

fn make_handler(from: &NodeDescriptor, to: &NodeDescriptor, created_at: DateTime<Utc>) -> Handler {
    let base = edge_base(Handler::ENTITY_TYPE, from, to, created_at);
    Handler {
        id: Some(base.id),
        from_node_id: Some(base.from_node_id),
        to_node_id: Some(base.to_node_id),
        from_node_type: Some(base.from_node_type),
        to_node_type: Some(base.to_node_type),
        created_at: base.created_at,
        updated_at: None,
    }
}

fn make_parent_of(
    from: &NodeDescriptor,
    to: &NodeDescriptor,
    created_at: DateTime<Utc>,
) -> ParentOf {
    let base = edge_base(ParentOf::ENTITY_TYPE, from, to, created_at);
    ParentOf {
        id: Some(base.id),
        from_node_id: Some(base.from_node_id),
        to_node_id: Some(base.to_node_id),
        from_node_type: Some(base.from_node_type),
        to_node_type: Some(base.to_node_type),
        created_at: base.created_at,
        updated_at: None,
    }
}

fn make_implements(
    from: &NodeDescriptor,
    to: &NodeDescriptor,
    created_at: DateTime<Utc>,
) -> Implements {
    let base = edge_base(Implements::ENTITY_TYPE, from, to, created_at);
    Implements {
        id: Some(base.id),
        from_node_id: Some(base.from_node_id),
        to_node_id: Some(base.to_node_id),
        from_node_type: Some(base.from_node_type),
        to_node_type: Some(base.to_node_type),
        created_at: base.created_at,
        updated_at: None,
    }
}

fn make_nested_in(
    from: &NodeDescriptor,
    to: &NodeDescriptor,
    created_at: DateTime<Utc>,
) -> NestedIn {
    let base = edge_base(NestedIn::ENTITY_TYPE, from, to, created_at);
    NestedIn {
        id: Some(base.id),
        from_node_id: Some(base.from_node_id),
        to_node_id: Some(base.to_node_id),
        from_node_type: Some(base.from_node_type),
        to_node_type: Some(base.to_node_type),
        created_at: base.created_at,
        updated_at: None,
    }
}

fn make_imports(from: &NodeDescriptor, to: &NodeDescriptor, created_at: DateTime<Utc>) -> Imports {
    let base = edge_base(Imports::ENTITY_TYPE, from, to, created_at);
    Imports {
        id: Some(base.id),
        from_node_id: Some(base.from_node_id),
        to_node_id: Some(base.to_node_id),
        from_node_type: Some(base.from_node_type),
        to_node_type: Some(base.to_node_type),
        created_at: base.created_at,
        updated_at: None,
    }
}

fn uuid_from_node(entity_type: &str, keys: &[(&str, String)]) -> String {
    let id = stable_node_id_u128(entity_type, keys);
    Uuid::from_u128(id).to_string()
}

fn uuid_from_edge(edge_label: &str, from: &str, to: &str) -> String {
    let id = stable_edge_id_u128(edge_label, from, to);
    Uuid::from_u128(id).to_string()
}

fn embedding_identifier(
    repo_full_name: &str,
    revision_sha: &str,
    start_line: i32,
    end_line: i32,
    chunk_index: usize,
) -> String {
    let source = format!(
        "readme|{}|{}|{}|{}|{}",
        repo_full_name, revision_sha, start_line, end_line, chunk_index
    );
    let uuid = Uuid::new_v5(&Uuid::NAMESPACE_URL, source.as_bytes());
    uuid.to_string()
}

fn approximate_token_count(text: &str) -> Option<i32> {
    let tokens = text.split_whitespace().count();
    i32::try_from(tokens).ok()
}

fn detect_embedding_model_from_env() -> Option<String> {
    for key in [
        "FSTORAGE_EMBEDDING_MODEL",
        "OPENAI_EMBEDDING_MODEL",
        "EMBEDDING_MODEL",
    ] {
        if let Ok(value) = std::env::var(key) {
            if !value.trim().is_empty() {
                return Some(value);
            }
        }
    }
    None
}

pub fn build_search_panel(results: &[SearchRepository]) -> StorageResult<RecordBatch> {
    let schema = Schema::new(vec![
        Field::new("full_name", DataType::Utf8, false),
        Field::new("html_url", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, true),
        Field::new("language", DataType::Utf8, true),
        Field::new("stargazers", DataType::Int64, false),
        Field::new(
            "updated_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            true,
        ),
    ]);

    let full_name = StringArray::from(
        results
            .iter()
            .map(|row| row.full_name.clone())
            .collect::<Vec<_>>(),
    );
    let html_url = StringArray::from(
        results
            .iter()
            .map(|row| row.html_url.clone())
            .collect::<Vec<_>>(),
    );
    let description = StringArray::from(
        results
            .iter()
            .map(|row| row.description.clone())
            .collect::<Vec<_>>(),
    );
    let language = StringArray::from(
        results
            .iter()
            .map(|row| row.language.clone())
            .collect::<Vec<_>>(),
    );
    let stargazers = Int64Array::from(
        results
            .iter()
            .map(|row| row.stargazers as i64)
            .collect::<Vec<_>>(),
    );
    let updated_at = TimestampMicrosecondArray::from(
        results
            .iter()
            .map(|row| row.updated_at.map(|ts| ts.timestamp_micros()))
            .collect::<Vec<_>>(),
    )
    .with_timezone("UTC");

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(full_name),
            Arc::new(html_url),
            Arc::new(description),
            Arc::new(language),
            Arc::new(stargazers),
            Arc::new(updated_at),
        ],
    )
    .map_err(StorageError::Arrow)?;

    Ok(batch)
}
