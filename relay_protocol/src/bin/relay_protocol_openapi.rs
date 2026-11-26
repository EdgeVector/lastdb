//! Print the relay protocol's OpenAPI document, which `docs/designs/relay-protocol.openapi.json`
//! holds. CI runs this and refuses a committed file that differs.

fn main() {
    let document = relay_protocol::openapi::document();
    let text = serde_json::to_string_pretty(&document).expect("a JSON value encodes");
    println!("{text}");
}
