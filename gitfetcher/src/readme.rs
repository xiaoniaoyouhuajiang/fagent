#[derive(Debug, Clone)]
pub struct ReadmeChunkPiece {
    pub start_line: i32,
    pub end_line: i32,
    pub text: String,
}

pub fn chunk_readme(text: &str, max_lines_per_chunk: usize) -> Vec<ReadmeChunkPiece> {
    if max_lines_per_chunk == 0 {
        return Vec::new();
    }

    let lines: Vec<&str> = text.lines().collect();
    if lines.is_empty() {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut start = 0;

    while start < lines.len() {
        let end = (start + max_lines_per_chunk).min(lines.len());
        let chunk_lines = &lines[start..end];
        let chunk_text = chunk_lines.join("\n");
        chunks.push(ReadmeChunkPiece {
            start_line: start as i32 + 1,
            end_line: end as i32,
            text: chunk_text,
        });
        start = end;
    }

    chunks
}
