use uuid::Uuid;

pub fn uuid_v5_u128(ns: Uuid, name: &str) -> u128 {
    Uuid::new_v5(&ns, name.as_bytes()).as_u128()
}

pub fn stable_node_id_u128(entity_type: &str, key_values: &[(&str, String)]) -> u128 {
    // name 形如 "Project|url=https://...|name=repo"
    let mut name = String::from(entity_type);
    for (k, v) in key_values {
        name.push('|');
        name.push_str(k);
        name.push('=');
        name.push_str(v);
    }
    uuid_v5_u128(Uuid::NAMESPACE_OID, &name)
}

pub fn stable_edge_id_u128(edge_label: &str, from: &str, to: &str) -> u128 {
    let name = format!("{}|{}|{}", edge_label, from, to);
    uuid_v5_u128(Uuid::NAMESPACE_OID, &name)
}
