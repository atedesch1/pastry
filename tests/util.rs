pub fn format(vec: Vec<u64>) -> String {
    let str = vec
        .iter()
        .map(|&x| format!("{:x}", x))
        .collect::<Vec<String>>()
        .join(", ") as String;
    format!("[{}]", str)
}
