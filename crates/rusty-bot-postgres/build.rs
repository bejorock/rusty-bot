fn main() {
    // sqlx::migrate! embeds migrations; ensure new files trigger recompilation.
    println!("cargo:rerun-if-changed=migrations");
}
