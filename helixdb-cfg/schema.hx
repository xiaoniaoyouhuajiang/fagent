// HelixDB Schema for a GitHub Analysis Platform
// Version 3: Implements the Snapshot Model for versioning and aligns code-level nodes with stackgraph-ast.
schema::3 {
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
        INDEX name: String,         // Developer's username
        followers: I64,
        location: String,
        email: String,
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
        number: I64,
        title: String,
        state: String,
        created_at: Date,
    }

    // Represents a pull request in a repository
    N::PULL_REQUEST {
        number: I64,
        title: String,
        state: String,
        created_at: Date,
    }

    // =====================================================================
    // Section 2: Code-Level Nodes (N::) - Aligned with stackgraph-ast
    // =====================================================================

    // Represents a code file within a version snapshot
    N::FILE {
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
        INDEX name: String,
        start_line: I32,
        end_line: I32,
    }

    // Represents a function or method
    N::FUNCTION {
        INDEX name: String,
        signature: String,
        start_line: I32,
        end_line: I32,
        is_component: Boolean,      // Is it a UI component? (from stackgraph)
    }

    // Represents a struct, interface, enum, etc.
    N::DATA_MODEL {
        INDEX name: String,
        construct: String,          // "struct", "interface", "enum"
        start_line: I32,
        end_line: I32,
    }

    // Represents a variable declaration
    N::VARIABLE {
        INDEX name: String,
        data_type: String,
    }

    // Represents a test function
    N::TEST {
        INDEX name: String,
        test_kind: String,          // "unit", "integration", "e2e"
        start_line: I32,
        end_line: I32,
    }

    // Represents a web API endpoint (for web projects)
    N::ENDPOINT {
        INDEX path: String,         // API path, e.g., "/api/users/:id"
        http_method: String,        // "GET", "POST", etc.
    }


    // =====================================================================
    // Section 3: Vector Nodes (V::)
    // =====================================================================

    // Represents a chunk of a README file for semantic search
    V::README_CHUNK {
        source_file: String DEFAULT "README.md",
        start_line: I32,
        end_line: I32,
    }

    // Represents an embedding of a code entity's body/documentation
    V::CODE_CHUNK {
        source_node_key: String,    // Unique key of the source N::FUNCTION or N::CLASS
        language: String,
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

    // --- Code Hierarchy Edges (within a Version) ---
    E::CONTAINS_CODE { From: VERSION, To: FILE }
    E::DEFINES { From: FILE, To: CLASS }
    E::DEFINES { From: FILE, To: FUNCTION }
    E::DEFINES { From: FILE, To: DATA_MODEL }
    E::DEFINES { From: FILE, To: VARIABLE }
    E::DEFINES { From: FILE, To: TEST }
    E::DEFINES { From: FILE, To: ENDPOINT }
    E::DEPENDS_ON { From: FILE, To: LIBRARY }

    // --- Code-Level Relationship Edges (from stackgraph-ast) ---
    E::CALLS { From: FUNCTION, To: FUNCTION }
    E::USES { From: FUNCTION, To: LIBRARY }
    E::OPERAND_OF { From: CLASS, To: FUNCTION } // A function is a method of a class
    E::HANDLED_BY { From: ENDPOINT, To: FUNCTION }
    E::PARENT_OF { From: CLASS, To: CLASS } // Inheritance
    E::IMPLEMENTS { From: CLASS, To: DATA_MODEL } // Class implements an interface
    E::INSTANTIATES { From: FUNCTION, To: CLASS }
    E::IMPORTS { From: FILE, To: FILE }

    // --- Testing Edges ---
    E::TESTS { From: TEST, To: FUNCTION }
    E::TESTS { From: TEST, To: CLASS }
    E::TESTS { From: TEST, To: ENDPOINT }

    // --- Documentation and Content Edges ---
    E::CONTAINS_CONTENT { From: PROJECT, To: README_CHUNK }
    E::DOCUMENTS { From: README_CHUNK, To: FUNCTION } // A doc chunk explaining a function
    E::HAS_EMBEDDING { From: FUNCTION, To: CODE_CHUNK }
    E::HAS_EMBEDDING { From: CLASS, To: CODE_CHUNK }
}