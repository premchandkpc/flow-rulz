# DSL Extensions

Add DSL tokens for: `loop`, `window`, `aggregate`, `filter`, `merge`, `join`, `cache`, `state`, `timer`, `delay`, `await`, `race`, `foreach`, `reduce`, `switch`, `try`/`catch`/`finally`. Each new token requires lexer, parser, AST, compiler, and VM handler changes.

**Files affected:**
- `rust/src/dsl/lexer.rs`
- `rust/src/dsl/parser.rs`
- `rust/src/dsl/compiler.rs`
- `rust/src/bytecode/opcode.rs`
- `rust/src/executor/mod.rs`

**Verification:** Each new DSL construct has unit tests covering lex → parse → compile → execute.
