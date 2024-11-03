//! # traqq
//! 
//! High-performance event processing system for Redis analytics.
//! 
//! ## Overview
//! 
//! `traqq` transforms JSON events into optimized Redis commands for real-time analytics.
//! It provides a flexible configuration system for mapping event properties into different
//! types of Redis commands, enabling complex queries without post-processing.

pub mod utils;
pub mod constants;

// Re-export commonly used types
pub use crate::constants::*;