//! Build script for `fold_db`.
//!
//! (The face-detection model-pack fetch that used to live here was deleted
//! with the desktop node in the Mini-only cutover, 2026-07-12 — restore
//! point: branch `archive/desktop-dmg-pre-removal`.)

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
}
