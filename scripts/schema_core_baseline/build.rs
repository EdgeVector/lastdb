fn main() {
    // The pre-SchemaConfig constructor is selected only for the one-shot
    // comparison against main before c08e7aa3. Normal runs leave the cfg off.
    println!("cargo::rustc-check-cfg=cfg(schema_core_pre_config)");
}
