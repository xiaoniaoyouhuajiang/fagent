// HelixDB Schema for a GitHub Analysis Platform
// Version 5: Extends project metadata with Issues/PRs, developers, and doc vectors while keeping code graph alignment.
schema::5 {
    // =====================================================================
    // Section 1: Project & Metadata Nodes (N::)
    // =====================================================================

    // Represents a software project/repository
    N::PROJECT {
        INDEX url: String,          // Unique URL of the repository
        name: String,               // Project name
        description: String,        // Project description
        language: String,           // Primary programming language
        stars: I64,
        forks: I64,
    }

    // Represents a developer or contributor
    N::DEVELOPER {
        INDEX platform: String,     // e.g. "github"
        INDEX account_id: String,   // Stable numeric/node id from the platform
        INDEX login: String,        // Current login/handle
        name: String,
        company: String,
        followers: I64,
        following: I64,
        location: String,
        email: String,
        created_at: Date,
        updated_at: Date,
    }

    // Represents a specific commit, the ground truth for a code state
    N::COMMIT {
        INDEX sha: String,          // The unique SHA hash of the commit
        message: String,
        committed_at: Date,
    }

    // NEW: Represents a version snapshot on the main branch (e.g., HEAD or a tag)
    N::VERSION {
        INDEX sha: String,          // The commit SHA this version points to
        tag: String,                // e.g., "v1.0.0". Can be empty for non-tagged commits.
        is_head: Boolean,           // Is this the latest version on the main branch?
        created_at: Date,           // The commit date of this version
    }

    // Represents an issue in a repository
    N::ISSUE {
        INDEX project_url: String,
        INDEX number: I64,
        title: String,
        body: String,
        state: String,
        author_login: String,
        author_id: String,
        created_at: Date,
        updated_at: Date,
        closed_at: Date,
        comments_count: I64,
        is_locked: Boolean,
        milestone: String,
        assignees: String,          // JSON array of assignee logins
        labels: String,             // JSON array of label names
        reactions_plus_one: I64,
        reactions_heart: I64,
        reactions_hooray: I64,
        reactions_eyes: I64,
        reactions_rocket: I64,
        reactions_confused: I64,
        representative_comment_ids: String, // JSON array of selected comment ids
        representative_digest_text: String,
    }

    // Represents a pull request in a repository
    N::PULL_REQUEST {
        INDEX project_url: String,
        INDEX number: I64,
        title: String,
        body: String,
        state: String,
        draft: Boolean,
        author_login: String,
        author_id: String,
        created_at: Date,
        updated_at: Date,
        closed_at: Date,
        merged: Boolean,
        merged_at: Date,
        merged_by: String,
        additions: I64,
        deletions: I64,
        changed_files: I64,
        commits: I64,
        base_ref: String,
        head_ref: String,
        base_sha: String,
        head_sha: String,
        is_cross_repo: Boolean,
        review_comments_count: I64,
        comments_count: I64,
        representative_comment_ids: String,
        representative_digest_text: String,
        related_issues: String,
    }

    // Represents a label that can be applied to issues or pull requests
    N::LABEL {
        INDEX project_url: String,
        INDEX name: String,
        color: String,
        description: String,
    }

    // =====================================================================
    // Section 2: Code-Level Nodes (N::) - Aligned with stackgraph-ast
    // =====================================================================

    // Represents a code file within a version snapshot
    N::FILE {
        INDEX version_sha: String,
        INDEX path: String,         // Relative path within the repo, e.g., "src/main.rs"
        language: String,
    }

    // Represents a library/dependency specified in a package file
    N::LIBRARY {
        INDEX name: String,         // e.g., "tokio"
        version: String,            // e.g., "1.35.1"
    }

    // Represents a class, module, or namespace
    N::CLASS {
        INDEX version_sha: String,
        INDEX file_path: String,
        INDEX name: String,
        start_line: I32,
        end_line: I32,
    }

    // Represents a language-level trait or interface
    N::TRAIT {
        INDEX version_sha: String,
        INDEX file_path: String,
        INDEX name: String,
        start_line: I32,
        end_line: I32,
    }

    // Represents a function or method
    N::FUNCTION {
        INDEX version_sha: String,
        INDEX file_path: String,
        INDEX name: String,
        signature: String,
        start_line: I32,
        end_line: I32,
        is_component: Boolean,      // Is it a UI component? (from stackgraph)
    }

    // Represents a struct, interface, enum, etc.
    N::DATA_MODEL {
        INDEX version_sha: String,
        INDEX file_path: String,
        INDEX name: String,
        construct: String,          // "struct", "interface", "enum"
        start_line: I32,
        end_line: I32,
    }

    // Represents a variable declaration
    N::VARIABLE {
        INDEX version_sha: String,
        INDEX file_path: String,
        INDEX name: String,
        data_type: String,
    }

    // Represents a test function
    N::TEST {
        INDEX version_sha: String,
        INDEX file_path: String,
        INDEX name: String,
        test_kind: String,          // "unit", "integration", "e2e"
        start_line: I32,
        end_line: I32,
    }

    // Represents a web API endpoint (for web projects)
    N::ENDPOINT {
        INDEX version_sha: String,
        INDEX file_path: String,
        INDEX path: String,         // API path, e.g., "/api/users/:id"
        http_method: String,        // "GET", "POST", etc.
    }


    // =====================================================================
    // Section 3: Vector Nodes (V::)
    // =====================================================================

    // Represents a chunk of a README file for semantic search
    V::README_CHUNK {
        id: String,
        project_url: String,
        revision_sha: String,
        source_file: String DEFAULT "README.md",
        start_line: I32,
        end_line: I32,
    }

    // Represents an embedding of a code entity's body/documentation
    V::CODE_CHUNK {
        id: String,
        project_url: String,
        revision_sha: String,
        source_file: String,
        source_node_key: String,    // Unique key of the source N::FUNCTION or N::CLASS
        source_node_id: String,
        language: String,
    }

    // Represents a synthesized document chunk for an issue thread
    V::ISSUE_DOC {
        id: String,
        project_url: String,
        issue_number: I64,
        source_updated_at: Date,
    }

    // Represents a synthesized document chunk for a pull request discussion
    V::PR_DOC {
        id: String,
        project_url: String,
        pr_number: I64,
        source_updated_at: Date,
    }


    // =====================================================================
    // Section 4: Edge Definitions (E::)
    // =====================================================================

    // --- Project, Version, and Metadata Edges ---
    E::HAS_VERSION { From: PROJECT, To: VERSION }
    E::IS_COMMIT { From: VERSION, To: COMMIT }
    E::CONTRIBUTES_TO { From: DEVELOPER, To: PROJECT }
    E::AUTHORED { From: DEVELOPER, To: COMMIT }
    E::HAS_ISSUE { From: PROJECT, To: ISSUE }
    E::HAS_PR { From: PROJECT, To: PULL_REQUEST }
    E::OPENED_ISSUE { From: DEVELOPER, To: ISSUE }
    E::OPENED_PR { From: DEVELOPER, To: PULL_REQUEST }
    E::RELATES_TO { From: PULL_REQUEST, To: ISSUE }
    E::IMPLEMENTS_PR { From: COMMIT, To: PULL_REQUEST }
    E::HAS_LABEL { From: ISSUE, To: LABEL }
    E::HAS_LABEL { From: PULL_REQUEST, To: LABEL }
    E::ASSIGNED_TO { From: ISSUE, To: DEVELOPER }
    E::ASSIGNED_TO { From: PULL_REQUEST, To: DEVELOPER }
    E::REVIEWED { From: DEVELOPER, To: PULL_REQUEST }
    E::MENTIONS { From: ISSUE, To: DEVELOPER }
    E::MENTIONS { From: PULL_REQUEST, To: DEVELOPER }

    // --- Code Hierarchy Edges (within a Version) ---
    E::CONTAINS { From: VERSION, To: FILE }
    E::CONTAINS { From: FILE, To: CLASS }
    E::CONTAINS { From: FILE, To: FUNCTION }
    E::CONTAINS { From: FILE, To: DATA_MODEL }
    E::CONTAINS { From: FILE, To: VARIABLE }
    E::CONTAINS { From: FILE, To: TEST }
    E::CONTAINS { From: FILE, To: ENDPOINT }
    E::DEPENDS_ON { From: FILE, To: LIBRARY }

    // --- Code-Level Relationship Edges (from stackgraph-ast) ---
    E::CALLS { From: FUNCTION, To: FUNCTION }
    E::USES { From: FUNCTION, To: FUNCTION }
    E::OPERAND { From: CLASS, To: FUNCTION } // A function is a method of a class
    E::HANDLER { From: ENDPOINT, To: FUNCTION }
    E::PARENT_OF { From: CLASS, To: CLASS } // Inheritance
    E::IMPLEMENTS { From: CLASS, To: TRAIT } // Class implements an interface
    E::NESTED_IN { From: FUNCTION, To: FUNCTION }
    E::IMPORTS { From: FILE, To: FILE }

    // --- Documentation and Content Edges ---
    E::CONTAINS_CONTENT { From: PROJECT, To: README_CHUNK }
    E::CONTAINS_CONTENT { From: PROJECT, To: ISSUE_DOC }
    E::CONTAINS_CONTENT { From: PROJECT, To: PR_DOC }
    E::DOCUMENTS { From: README_CHUNK, To: FUNCTION } // A doc chunk explaining a function
    E::DOCUMENTS { From: ISSUE, To: ISSUE_DOC }
    E::DOCUMENTS { From: PULL_REQUEST, To: PR_DOC }
    E::EMBEDS { From: FUNCTION, To: CODE_CHUNK }
    E::EMBEDS { From: CLASS, To: CODE_CHUNK }
}
