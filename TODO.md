## Completed

- [x] Documentation baseline
- [x] Function adapters: `sources.Func`, `actions.Func`, `destinations.Func`, `ifs.Func`
- [x] Action composition: `Chain`, `Chain3`, `Chain4`, `Chain5`
- [x] Action dispatchers: `RoundRobin`, `Random`
- [x] Destination dispatchers: `Fanout`, `RoundRobin`, `Random`
- [x] Destination wrappers: `FromChan`, `FromSlice`, `ToChan`, `ToSlice`
- [x] Dedup primitive: `ifs.Once` (event-id based + TTL)

## Next Up (High Priority)

- [ ] Debounce condition (`ifs.Debounce`)
- [ ] Rule priority/salience (deterministic execution order)
- [ ] Conflict resolution modes beyond current all-match (`first-match`, `best-score`)
- [ ] Rule groups/agendas (phase-based execution)
- [ ] Per-rule metrics (match/error/latency)
- [ ] Explain traces (why matched/skipped)
- [ ] Shadow mode (evaluate without side effects)

## Rule Semantics & Control

- [ ] Rule dependencies (`before`/`after`/prerequisite outcomes)
- [ ] Activation controls: cooldown (Once already exists)
- [ ] Branching flow for actions (if/else style pipelines)
- [ ] Retry/backoff policies for actions/destinations
- [ ] Circuit breaker + fallback destination

## Conditions & Expression Engine

- [ ] Nested field/path access in conditions
- [ ] Configurable type coercion policy (strict/permissive)
- [ ] Expression function registry (custom funcs)
- [ ] Temporal predicates (`within`, `before`, `after`)
- [ ] Window predicates (count/rate/distinct over interval)

## Stateful Stream Features

- [ ] Unified state-store abstraction with pluggable backends (redis/postgres, etc.)
- [ ] Windowing support (tumbling/sliding/session)
- [ ] Correlation/join rules across event types
- [ ] Dedup by configurable key (TTL already exists)
- [ ] Out-of-order handling + watermarking

## Operability & Governance

- [ ] Rule lint/validation pass for dead/ambiguous rules (basic struct validation already exists)
- [ ] Per-rule timeouts and cancellation guards
- [ ] Complexity limits for condition evaluation

## Testing & Developer Experience

- [ ] Rule test harness DSL (Given/When/Then)
- [ ] Event replay tooling from captured logs
- [ ] Deterministic clock injection for time-based features
- [ ] Golden explain-trace tests

## Product & Ecosystem

- [ ] Wrappers for popular libraries
- [ ] Website
- [ ] Logo

## Deferred (only if external/runtime-managed rule definitions are introduced)

- [ ] Rule versioning + migration compatibility
- [ ] Safe hot reload of rules/config
- [ ] Audit log for rule changes/decisions
