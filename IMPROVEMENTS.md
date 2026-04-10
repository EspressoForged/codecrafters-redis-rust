# Unified Improvement & Refactoring Roadmap

This document outlines a prioritized architectural and structural roadmap for the `codecrafters-redis-rust` project. It combines existing improvement goals with newly identified areas to enhance maintainability, reliability, and idiomatic Rust patterns.

---

## Phase 1: Foundational Architecture (High Criticality, High Benefit)
*These items are prerequisites for most other improvements and resolve the most significant "technical debt" in the codebase.*

### 1. Decouple the "God Object" Store
- **Issue:** `src/app/store.rs` contains low-level implementation details for every supported data type (Strings, Lists, Streams, Geo, etc.).
- **Improvement:** Move data-specific logic (e.g., `LPOP`, `GEOSEARCH`) out of `src/app/store.rs` and into module-specific logic. Refactor `Store` to be a type-safe wrapper for the underlying `DashMap`.
- **Benefit:** Enables independent development of data types and prevents `store.rs` from becoming unmanageable.

### 2. Consolidate Context Management (`AppContext`)
- **Issue:** Core functions pass many individual `Arc` handles (`store`, `config`, `replication`, etc.), leading to verbose and fragile function signatures.
- **Improvement:** Wrap all shared resources into a single `Arc<AppContext>` struct.
- **Benefit:** Simplifies signatures and makes it trivial to add new shared state (like a `ClientRegistry`) without refactoring dozens of handlers.

### 3. Unified Command Execution Path
- **Issue:** Replicas manually match and apply write commands, duplicating logic from the master's `handle_command`.
- **Improvement:** Create a single `apply_command` or `execute` entry point used by both the master (client requests) and replicas (propagated commands).
- **Benefit:** Eliminates logic duplication, ensuring replicas and masters behave identically for all write operations.

### 4. Command Dispatch Refactoring
- **Issue:** The `handle_command` function uses a large, manual `match` statement to route commands.
- **Improvement:** Replace the `match` statement with a trait-based registry (e.g., `HashMap<Command, Box<dyn CommandHandler>>`).
- **Benefit:** Decouples the main loop from individual command logic, allowing for cleaner organization and easier testing.

---

## Phase 2: Reliability & Data Integrity (High Criticality, Medium Complexity)
*Focuses on system stability and preventing common Redis-clone bugs.*

### 5. Robust Protocol Parsing (Panic Prevention)
- **Issue:** Several `.unwrap()` calls exist within `src/app/protocol.rs` during parsing (integer/string conversion).
- **Improvement:** Replace all `unwrap()` calls with proper `nom` error mapping and descriptive `AppError` variants.
- **Benefit:** Prevents the server from panicking when receiving malformed or malicious RESP data.

### 6. Consistent Store Safety (Expiration Logic)
- **Issue:** Commands using `DashMap::entry` (like `zadd`, `incr`, `lpush`) do not always check if an existing key is expired before mutation.
- **Improvement:** Standardize all `Store` entry points to perform an explicit expiration check (and potential eviction) before performing any mutation.
- **Benefit:** Prevents "stale" data from being modified or returned by commands that don't currently check TTLs consistently.

### 7. Formalize Connection State Machine
- **Issue:** Logic for transactions (`MULTI`) and subscriptions (`SUBSCRIBE`) is manually checked within the main loop, blurring the "happy path" logic.
- **Improvement:** Implement a formal State Pattern for connections (e.g., `NormalState`, `TransactionState`, `SubscribedState`).
- **Benefit:** Isolates complex behaviors like command queueing or dedicated subscription loops, making the main connection loop cleaner.

### 8. Declarative Command Argument Parsing
- **Issue:** Handlers manually check for arguments and perform type conversions, leading to repetitive and fragile boilerplate.
- **Improvement:** Implement a structured argument parser or a declarative system to automatically validate and extract typed arguments for each command.
- **Benefit:** Eliminates boilerplate and improves the robustness of argument validation.

---

## Phase 3: Operational Excellence (Medium Criticality, Medium Benefit)
*Improvements for production-readiness, resource management, and performance.*

### 9. Automate Replication Propagation
- **Issue:** Write command propagation relies on a manual `is_write_command` list in `handle_command`.
- **Improvement:** Define command properties (e.g., `is_write`) as metadata on the `Command` enum and use it to automatically trigger propagation.
- **Benefit:** Prevents bugs where a new write command is implemented but forgotten in the manual propagation list.

### 10. Robust Graceful Shutdown
- **Issue:** The server loop in `foundation::net::run` spawns tasks but does not track or await them during shutdown.
- **Improvement:** Use `tokio::task::JoinSet` to track connection tasks and broadcast signals to ensure they close cleanly before exit.
- **Benefit:** Ensures data integrity (e.g., during RDB persistence) and clean resource release.

### 11. Active Expiration Management (Background Sweeper)
- **Issue:** Keys with TTLs that are never accessed again linger in memory indefinitely (lazy expiration only).
- **Improvement:** Implement a background task that periodically samples a random subset of keys with TTLs and evicts expired ones.
- **Benefit:** Prevents memory leaks caused by "dead" keys.

### 12. Refactor Waiter Registry
- **Issue:** `wait.rs` uses $O(N)$ cleanup logic and a "dummy key" hack to support the `WAIT` command.
- **Improvement:** Refactor the registry for efficient multi-key waiting and provide a first-class notification mechanism for replication offsets.
- **Benefit:** Improves performance for blocking operations and removes architectural hacks.

---

## Phase 4: Advanced Features & Polish (Lower Criticality, High Complexity)
*Optimizations and extensibility for future growth.*

### 13. Formalized Client Management & Registry
- **Issue:** Client IDs are managed via a simple global atomic; there is no central registry for active connections.
- **Improvement:** Track active clients in a `ClientRegistry` within `AppContext`.
- **Benefit:** Enables administrative commands like `CLIENT LIST`, `CLIENT KILL`, and provides the foundation for `MONITOR`.

### 14. Internal Command Interceptors (Middleware)
- **Issue:** No clean way to implement cross-cutting concerns like logging or monitoring without modifying every handler.
- **Improvement:** Implement a hook system for commands (e.g., `pre_execute`, `post_execute`).
- **Benefit:** Enables features like `MONITOR`, `SLOWLOG`, and metrics collection without polluting core logic.

### 15. Propagation Efficiency (Worker Pattern)
- **Issue:** `propagate` spawns a new `tokio` task for every command, adding overhead during high-write volume.
- **Improvement:** Replace per-command spawning with a dedicated channel and background worker for each replica or a global propagation worker.
- **Benefit:** Reduces task-scheduling overhead.

### 16. Clarify Foundation vs. App Boundaries
- **Issue:** The boundary between `foundation` (generic utilities) and `app` (Redis logic) is slightly blurred.
- **Improvement:** Further generalize `foundation` to be entirely agnostic of Redis, using generic `Service` or `Handler` traits.
- **Benefit:** Improves code reuse and aligns the project with standard Rust ecosystem patterns.
