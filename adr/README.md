# Architecture Decision Records (ADR)

This directory contains Architecture Decision Records (ADRs) for the `dg-sqlmesh` project. ADRs document significant architectural decisions, their context, rationale, and consequences.

## What are ADRs?

Architecture Decision Records are documents that capture important architectural decisions made during the development of a project. They provide:

- **Context**: Why the decision was needed
- **Decision**: What was decided
- **Rationale**: Why this option was chosen over alternatives
- **Consequences**: Both positive and negative outcomes

## ADR Index

| ADR                                                    | Title                                           | Status   | Date       |
| ------------------------------------------------------ | ----------------------------------------------- | -------- | ---------- |
| [ADR-0001](./0001-individual-assets-vs-multi-asset.md) | Individual Assets vs Multi-Asset Pattern        | Accepted | 2025-08-05 |
| [ADR-0002](./0002-shared-sqlmesh-execution.md)         | Shared SQLMesh Execution per Dagster Run        | Accepted | 2025-08-05 |
| [ADR-0003](./0003-asset-check-integration.md)          | Asset Check Integration for SQLMesh Audits      | Accepted | 2025-08-05 |
| [ADR-0004](./0004-retry-policy-management.md)          | Retry Policy Management for SQLMesh Integration | Accepted | 2025-08-05 |
| [ADR-0005](./0005-custom-sqlmesh-console.md)           | Custom SQLMesh Console for Event Capture        | Accepted | 2025-08-05 |
| [ADR-0006](./0006-sqlmesh-dagster-tag-convention.md)   | SQLMesh to Dagster Tag Convention               | Accepted | 2025-08-05 |

## Key Architectural Patterns

### 1. Individual Asset Pattern

Each SQLMesh model becomes a separate Dagster asset, enabling granular control and better UI integration.

### 2. Shared Execution Pattern

Single SQLMesh execution per Dagster run, shared between all selected assets to respect SQLMesh's natural dependency management.

### 3. Event-Driven Architecture

Custom SQLMesh console captures execution events and converts them to Dagster concepts (asset checks, error messages, etc.).

### 4. Resource-Based State Management

`SQLMeshResultsResource` manages shared state between assets in the same run, ensuring consistency.

### 5. Tag-Based Property Mapping

`dagster:property_name:value` convention allows SQLMesh models to override Dagster asset properties via tags.

## Current Limitations

### Non-Blocking Audits

SQLMesh supports non-blocking audits, but current implementation only handles blocking audits. Future enhancement needed.

### Schedule Complexity

Adaptive schedules based on SQLMesh crons require complex logic to determine optimal execution frequency.

### Error Type Distinction

Distinguishing between transient and persistent failures for retry logic is complex and not fully implemented.

## Future Enhancements

1. **Non-blocking audit support** - Handle SQLMesh non-blocking audits in Dagster
2. **Enhanced error classification** - Better distinction between transient and persistent failures
3. **Advanced scheduling** - More sophisticated adaptive scheduling based on data dependencies
4. **Performance optimization** - Reduce memory usage and improve execution speed

## Contributing

When making significant architectural changes:

1. **Create a new ADR** following the template
2. **Update existing ADRs** if decisions change
3. **Link related ADRs** to show dependencies
4. **Include diagrams** using Mermaid for complex flows
5. **Document consequences** both positive and negative

## Template

```markdown
# ADR-XXXX: [Title]

## Status

**Proposed/Accepted/Rejected** - YYYY-MM-DD

## Context

[Describe the context and problem statement]

## Decision

[Describe the decision made]

## Rationale

[Explain why this decision was made over alternatives]

## Implementation

[Describe how the decision is implemented]

## Consequences

### Positive

- [List positive consequences]

### Negative

- [List negative consequences]

## Related Decisions

- [Links to related ADRs]
```
