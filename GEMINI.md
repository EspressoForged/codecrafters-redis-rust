# GEMINI.md - Rust Project Guidelines

This document outlines the high-level guidelines and conventions for developing in this Rust project. Adhering to these principles ensures consistency, maintainability, and quality across the codebase.

## 1. Idiomatic Rust Programming

All code should strive to be idiomatic Rust. This includes:
*   **Ownership and Borrowing:** Leverage Rust's ownership system to ensure memory safety and prevent data races. Understand and apply borrowing rules correctly.
*   **Error Handling:** Prefer `Result<T, E>` for recoverable errors and `panic!` for unrecoverable errors (bugs). Utilize `thiserror` for defining clear, descriptive error types within libraries and `anyhow` for simplified application-level error reporting.
*   **Concurrency:** Use `tokio` for asynchronous programming and `rayon` for data parallelism where appropriate. Ensure proper synchronization and avoid common concurrency pitfalls.
*   **Option and Result:** Use `Option<T>` for potentially absent values and `Result<T, E>` for fallible operations, preferring combinators like `map`, `and_then`, `unwrap_or_else`, etc., over explicit `match` statements where clarity is maintained.
*   **Iterators:** Embrace Rust's powerful iterator API for collection processing.

## 2. External Crate Usage

When integrating external crates, follow these guidelines:
*   **Purpose-driven:** Only include crates that serve a clear, necessary purpose.
*   **Idiomatic Integration:** Integrate crates (e.g., `serde` for serialization/deserialization, `clap` for command-line argument parsing, `tokio` for async runtime, `rayon` for parallelism, `nom` for parsing) in an idiomatic way, following their recommended usage patterns and best practices.
*   **Version Management:** Specify appropriate version ranges in `Cargo.toml` and ensure `cargo update` is run periodically to keep dependencies up-to-date while maintaining compatibility.

## 3. Testing

Robust testing is crucial for code quality.
*   **Unit Tests:** Write comprehensive unit tests for individual functions and modules. Tests should be clear, concise, and cover various scenarios (e.g., happy path, edge cases, error conditions).
*   **Integration Tests:** Create integration tests for interactions between modules or with external systems.
*   **Test Execution:** All tests must pass when executed with `cargo test`.
*   **Failing Tests:** If a test fails, the source of the error must be identified and fixed. Valid tests should *never* be deleted or ignored just because they fail.

## 4. Code Quality & Style

Consistency and readability are paramount.
*   **Formatting:** All code must be formatted using `cargo fmt`.
*   **Linting:** The codebase must cleanly pass all checks enforced by `cargo clippy`. Address all warnings.
*   **Readability:** Prioritize clear, self-documenting code. Use meaningful names for variables, functions, and modules.

## 5. Documentation

Maintain accurate and up-to-date documentation.
*   **API Documentation:** Document all public items (functions, structs, enums, traits, macros) using Rust's `///` and `//!` comment styles. Provide clear explanations, examples, and usage notes.
*   **Module and Crate Documentation:** Provide high-level overviews for modules and the crate itself.
*   **`README.md`:** Keep the `README.md` file up-to-date with essential project information, setup instructions, and examples.