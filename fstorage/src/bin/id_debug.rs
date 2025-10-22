use fstorage::utils::id::stable_node_id_u128;
use uuid::Uuid;

fn main() {
    let id = stable_node_id_u128(
        "project",
        &[("url", "https://github.com/talent-plan/tinykv".to_string())],
    );
    println!("{}", Uuid::from_u128(id));
}
