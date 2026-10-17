//! Builds the kafka4php SAPI (every `csrc/*.c`) against the PHP found by `php-config`, links
//! `libphp` dynamically and generates bindings for our own header only (DESIGN.md §8).
//!
//! Env: `PHP_CONFIG` (default `php-config`) selects the PHP installation.

use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

fn php_config(php_config: &str, arg: &str) -> String {
    let out = Command::new(php_config)
        .arg(arg)
        .output()
        .unwrap_or_else(|e| {
            panic!(
                "k4p-php-sys: cannot run `{php_config} {arg}`: {e}. A ZTS PHP with the embed \
                 SAPI is required; build inside the toolchain container (scripts/dev.sh) or \
                 set PHP_CONFIG."
            )
        });
    assert!(
        out.status.success(),
        "k4p-php-sys: `{php_config} {arg}` failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout)
        .expect("php-config output is not UTF-8")
        .trim()
        .to_owned()
}

fn main() {
    println!("cargo:rerun-if-env-changed=PHP_CONFIG");
    println!("cargo:rerun-if-changed=csrc");
    let php_config_bin = env::var("PHP_CONFIG").unwrap_or_else(|_| "php-config".to_owned());

    let includes: Vec<PathBuf> = php_config(&php_config_bin, "--includes")
        .split_whitespace()
        .filter_map(|flag| flag.strip_prefix("-I"))
        .map(PathBuf::from)
        .collect();
    let prefix = PathBuf::from(php_config(&php_config_bin, "--prefix"));
    let lib_dir = prefix.join("lib");
    assert!(
        lib_dir.join("libphp.so").exists(),
        "k4p-php-sys: {} not found; PHP must be built with --enable-embed",
        lib_dir.join("libphp.so").display()
    );

    let csrc = Path::new("csrc");
    let mut sources: Vec<PathBuf> = std::fs::read_dir(csrc)
        .expect("k4p-php-sys: cannot read csrc/")
        .map(|e| e.expect("k4p-php-sys: cannot read csrc/").path())
        .filter(|p| p.extension().is_some_and(|ext| ext == "c"))
        .collect();
    sources.sort();
    let mut build = cc::Build::new();
    build
        .files(&sources)
        .include(csrc)
        .includes(&includes)
        .std("c11")
        // Thread-local cache of the TSRM pointer, as PHP extensions use it.
        .define("ZEND_ENABLE_STATIC_TSRMLS_CACHE", "1")
        .define("_GNU_SOURCE", None)
        .warnings(true)
        .extra_warnings(true)
        // PHP's own headers trip these.
        .flag_if_supported("-Wno-unused-parameter")
        .flag_if_supported("-Wno-missing-field-initializers")
        .flag_if_supported("-Wno-sign-compare")
        .warnings_into_errors(true);
    build.compile("k4p_sapi");

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=php");
    // For dependents' build scripts (`links = "php"`): DEP_PHP_LIB_DIR etc.
    println!("cargo:lib_dir={}", lib_dir.display());

    let bindings = bindgen::Builder::default()
        .header(csrc.join("k4p_sapi.h").to_string_lossy())
        .allowlist_item("k4p_.*")
        .allowlist_item("K4P_.*")
        .newtype_enum("k4p_outcome_kind")
        .newtype_enum("k4p_log_level")
        .derive_default(true)
        .derive_debug(true)
        .layout_tests(true)
        .generate()
        .expect("k4p-php-sys: bindgen failed on csrc/k4p_sapi.h");
    let out = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR")).join("bindings.rs");
    bindings
        .write_to_file(&out)
        .expect("k4p-php-sys: cannot write bindings");
}
