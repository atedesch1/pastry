use super::setup::NetworkNode;

pub fn format_ids(vec: Vec<u64>) -> String {
    let str = vec
        .iter()
        .map(|&x| format!("{:x}", x))
        .collect::<Vec<String>>()
        .join(", ") as String;
    format!("[{}]", str)
}

pub fn find_responsible(nodes: &Vec<NetworkNode>, key: u64) -> usize {
    let mut position = match nodes.binary_search_by_key(&key, |f| f.info.id) {
        Ok(position) => position,
        Err(position) => position,
    };

    if position == nodes.len() {
        position -= 1;
    }

    if key < nodes[position].info.id {
        position = if position == 0 {
            nodes.len() - 1
        } else {
            position - 1
        };
    }

    position
}
