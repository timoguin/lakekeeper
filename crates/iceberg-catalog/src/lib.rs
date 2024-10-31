#![warn(
    missing_debug_implementations,
    rust_2018_idioms,
    unreachable_pub,
    clippy::pedantic
)]
#![allow(clippy::module_name_repetitions, clippy::large_enum_variant)]
#![forbid(unsafe_code)]

mod config;
pub mod modules;
pub mod service;
pub use modules::{ProjectIdent, SecretIdent, WarehouseIdent};

pub use config::{AuthZBackend, OpenFGAAuth, SecretBackend, CONFIG, DEFAULT_PROJECT_ID};

mod request_metadata;

pub mod rest;

#[cfg(feature = "router")]
pub mod metrics;
#[cfg(feature = "router")]
pub(crate) mod tracing;
