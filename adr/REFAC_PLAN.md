# Refactoring Plan for dg_sqlmesh

Status: Draft
Owner: Core maintainers
Branch: feat/refactor-code-and-tests
Goal: Zero functional regressions; improve separation of concerns, readability, and unit-testability.

## 1) Non-Regression Guarantees

- Blocking vs non-blocking audits:
  - Blocking failure → AssetCheckResult severity=ERROR; block downstream immediately (short-circuit).
  - Non-blocking failure → AssetCheckResult severity=WARN; no downstream block.
- Unified metadata via  for PASS and FAIL.
- Use SQLMesh notifier exclusively (no console).
- Retries disabled per ADR-0004 through Dagster tags.
- Same public API in  package, , and .

Acceptance checks:
- All unit/integration tests to be rewritten/realigned but still verify the above behaviors.
- Smoke run on  passes with the above semantics.

## 2) High-Level Restructuring

- Modules to review:
  -  (large, mixed responsibilities)
  -  (long functions)
  -  (dead/redundant code suspected)
  -  (good centralization; keep)
  -  (keep; small and focused)
  -  (ensure thin orchestration; no heavy logic)
  -  (leave intact unless minor cleanup)

- Target structure:
  - Keep “what it does” vs “how it does it” separate:
    - Resource: configuration, context wiring, and orchestration only.
    - Execution utils: pure execution steps with micro-functions.
    - Check utils: pure metadata and audit details handling (already centralized).
    - Notifier: capturing only, no business logic.

## 3) Phased Refactor Steps

### Phase 0 — Hygiene and Baseline
- Translate logs/messages to English in code (alignment with code standards).
- Remove imports and code that refer to legacy console or dead paths.
- Add type hints and narrow types across public helper functions.

### Phase 1 — Split sqlmesh_asset_execution_utils.py
Current long functions to be decomposed into micro-functions:

- Keep orchestrators:
  - 
  - 
  - 

- Extract pure helpers:
  - Planning/selection
    -  (already in utils; verify signatures)
  - Notifier interaction
    - 
  - Downstream computation
    - 
    - 
  - Check result building (per model)
    - 
    - 
    - 
  - Result assembly
    - 
    - 
  - Utility glue
    - 
    - 

Benefits:
- Each function under ~30 lines.
- Easier unit tests per helper.

### Phase 2 — Prune and Slim resource.py
- Extract notifier registration and query paths to a small service:
  -  (optional module: )
    - 
    - 
- Extract context accessors to simple helpers:
  - 
- Keep  thin:
  - Config, context bootstrap, calling , compute downstream via utils.
- Remove any stale console/backward-compat code.

### Phase 3 — Consolidate and Deduplicate utilities
- :
  - Keep: , , , , , , .
  - Remove or annotate legacy helpers; avoid duplicate SQL extraction patterns.
- :
  - Remove unused helpers; merge overlapping logic into execution or check utils.

### Phase 4 — Logging and Exceptions
- Standardize log levels/messages to English and concise phrasing.
- Ensure domain exceptions are clear:
  - Keep  usage and docstring.
  - No silent exception swallowing unless defensive guard with a log.

### Phase 5 — Type Hints, Docstrings, Naming
- Add/complete docstrings for all public helpers (what/inputs/outputs/raises).
- Ensure consistent naming (no mixed languages).
- Prefer / for notifier failure record shape if feasible.

### Phase 6 — Public API surface check
- Validate  remains unchanged in signature.
- Check  exports; remove unused component imports or gate them properly.

### Phase 7 — Tests Refactor (Unit-First)
- Unit tests per micro-function:
  - Check severity selection.
  - Metadata building for PASS/FAIL cases.
  - Downstream computation from blocking failures.
  - Notifier failure record translation.
- Integration tests:
  - End-to-end run with mixed audits: blocking and non-blocking in same model.
  - Downstream short-circuit.
- Remove old tests tied to console or removed APIs.

### Phase 8 — Docs and ADRs
- Update README and examples to reflect notifier usage, no retry policy knob.
- Ensure ADR-0013 reference stands; add short “Migration Notes” section if needed.

## 4) Task Breakdown and Checklist

- [ ] Phase 0: Translate logs; strip dead imports/paths
- [ ] Phase 1: Split  into micro-functions
- [ ] Phase 2: Slim  and optionally add 
- [ ] Phase 3: Consolidate utils; remove duplicates
- [ ] Phase 4: Normalize logging and exceptions
- [ ] Phase 5: Type hints and docstrings
- [ ] Phase 6: Public API surface validation
- [ ] Phase 7: Rewrite unit tests; restore integration tests
- [ ] Phase 8: Docs/ADR sync

## 5) Risk Management

- Risk: Behavioral drift during splitting.
  - Mitigation: Add unit tests for each extracted helper before wiring changes broadly.
- Risk: Hidden coupling with tests.
  - Mitigation: Refactor tests concurrently with code; keep integration tests validating the non-regression guarantees.

## 6) Timeline (Indicative)

- Day 1–2: Phase 0, Phase 1
- Day 3: Phase 2–3
- Day 4: Phase 4–5
- Day 5–6: Phase 7–8, polish

## 7) Notes on Testability

- Micro-functions return plain values (no context dependence) wherever possible.
- Inject dependencies (e.g., , Usage: sqlmesh [OPTIONS] COMMAND [ARGS]...

  SQLMesh command line tool.

Options:
  --version            Show the version and exit.
  -p, --paths TEXT     Path(s) to the SQLMesh config/project.
  --config TEXT        Name of the config object. Only applicable to
                       configuration defined using Python script.
  --gateway TEXT       The name of the gateway.
  --ignore-warnings    Ignore warnings.
  --debug              Enable debug mode.
  --log-to-stdout      Display logs in stdout.
  --log-file-dir TEXT  The directory to write log files to.
  --dotenv PATH        Path to a custom .env file to load environment
                       variables.
  --help               Show this message and exit.

Commands:
  audit                   Run audits for the target model(s).
  check_intervals         Show missing intervals in an environment,...
  clean                   Clears the SQLMesh cache and any build artifacts.
  create_external_models  Create a schema file containing external model...
  create_test             Generate a unit test fixture for a given model.
  dag                     Render the DAG as an html file.
  destroy                 The destroy command removes all project resources.
  diff                    Show the diff between the local state and the...
  dlt_refresh             Attaches to a DLT pipeline with the option to...
  environments            Prints the list of SQLMesh environments with...
  evaluate                Evaluate a model and return a dataframe with a...
  fetchdf                 Run a SQL query and display the results.
  format                  Format all SQL models and audits.
  info                    Print information about a SQLMesh project.
  init                    Create a new SQLMesh repository.
  invalidate              Invalidate the target environment, forcing its...
  janitor                 Run the janitor process on-demand.
  lint                    Run the linter for the target model(s).
  migrate                 Migrate SQLMesh to the current running version.
  plan                    Apply local changes to the target environment.
  prompt                  Uses LLM to generate a SQL query from a prompt.
  render                  Render a model's query, optionally expanding...
  rewrite                 Rewrite a SQL expression with semantic...
  rollback                Rollback SQLMesh to the previous migration.
  run                     Evaluate missing intervals for the target...
  state                   Commands for interacting with state
  table_diff              Show the diff between two tables or a selection...
  table_name              Prints the name of the physical table for the...
  test                    Run model unit tests.
  ui                      Start a browser-based SQLMesh UI.) as parameters to simplify mocking.
- Use small fixtures for notifier failure records and audit specs.
