# Claude Spec: Refactor Pegasus WMS from SQLAlchemy 1.3 to 1.4.x

## 1. Objective

Refactor the Pegasus WMS Python codebase to be fully
compatible with SQLAlchemy 1.4.x while preserving existing functionality
and preparing for eventual migration to SQLAlchemy 2.0-style patterns.

------------------------------------------------------------------------

## 2. Goals

1.  Upgrade SQLAlchemy dependency from 1.3.x to 1.4.x.
2.  Eliminate deprecated 1.3 patterns.
3.  Adopt 1.4-compatible patterns (preferably 2.0-style where
    practical).
4.  Ensure:
    -   No regression in business logic
    -   No performance degradation
    -   All tests pass
5.  Prepare codebase for easier future migration to SQLAlchemy 2.0.

------------------------------------------------------------------------

## 3. Non-Goals

-   Immediate upgrade to SQLAlchemy 2.0
-   Major architectural redesign
-   ORM model redesign
-   Database engine migration
-   Performance optimization unrelated to SQLAlchemy changes

------------------------------------------------------------------------

## 4. Scope

### In Scope

-   ORM model definitions
-   Query construction patterns
-   Session management
-   Engine configuration
-   Transaction handling
-   Relationship loading strategies
-   Raw SQL execution
-   Custom SQLAlchemy utilities

### Out of Scope

-   Schema redesign
-   Business logic changes unrelated to SQLAlchemy
-   API contract changes
-   Frontend modifications

------------------------------------------------------------------------

## 5. Migration Strategy

### Phase 1 -- Static Refactor

-   Update dependency to SQLAlchemy 1.4.x
-   Enable `future=True`
-   Replace deprecated APIs
-   Refactor query patterns

### Phase 2 -- Runtime Validation

Run full test suite using:

``` bash
tox -e py36
```

Fix runtime incompatibilities and validate transaction behavior.

### Phase 3 -- 2.0 Compatibility Check

Run with warnings enabled:

``` bash
SQLALCHEMY_WARN_20=1 tox -e py36
```

Remove 2.0 deprecation warnings where feasible.

------------------------------------------------------------------------

## 6. Testing Requirements

All changes must be validated using the project's tox configuration.

### 6.1 Primary Test Command

``` bash
tox -e py36
```

-   This runs the full unit and integration test suite.
-   Migration is not complete until this exits successfully (exit code
    0).

### 6.2 Required Validation Checks

1.  `tox -e py36` completes successfully.
2.  No SQLAlchemy 1.4 deprecation warnings appear.
3.  `SQLALCHEMY_WARN_20=1 tox -e py36` introduces no new warnings.
4.  CRUD flows behave identically.
5.  Relationship loading works correctly.
6.  Transaction commit/rollback behavior unchanged.
7.  No query count increase in critical workflows.
8.  No N+1 regressions introduced.

### 6.3 Failure Handling

If tests fail:

-   Identify root cause (query refactor, result handling, transaction
    changes).
-   Fix issue.
-   Re-run `tox -e py36` until fully green.

------------------------------------------------------------------------

## 7. Acceptance Criteria

-   Application runs without SQLAlchemy deprecation warnings.
-   No usage of deprecated APIs.
-   All tests pass.
-   Production behavior unchanged.
-   Code follows 1.4 future-style patterns.

------------------------------------------------------------------------

## 8. Deliverables

1.  Refactored codebase
2.  Migration summary report
3.  Test results report
4.  List of remaining 2.0 incompatibilities (if any)

------------------------------------------------------------------------

## 9. Claude Execution Instructions

1.  Prefer 2.0-style syntax compatible with 1.4.
2.  Preserve business logic exactly.
3.  Do not modify database schema.
4.  Add comments where behavior changes materially.
5.  Flag ambiguous refactors for human review.
6.  Maintain readability and consistency with existing conventions.

## 10. Importabnt

Only change code in packages/pegasus-python
Do not commit or push any changes.
