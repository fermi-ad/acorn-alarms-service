fn announce_self() -> String {
    String::from("Hello, world!")
}

fn main() {
    println!("{}", announce_self());
}

#[cfg(test)]
mod tests {
    use super::announce_self;

    #[test]
    fn test_hello_world() {
        assert_eq!(announce_self(), String::from("Hello, world!"));
    }
}
