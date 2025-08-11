# ADR-0011: Error Handling Strategy

## Status

**Accepted** - 2025-01-27

## Context

The integration between SQLMesh and Dagster involves complex error scenarios that need to be handled gracefully:

1. **SQLMesh-specific errors**: PlanError, ConflictingPlanError, NodeAuditsErrors, etc.
2. **Audit failures**: Both blocking and non-blocking audit failures
3. **Upstream dependency failures**: When upstream assets fail, downstream assets should handle gracefully
4. **Resource initialization errors**: SQLMesh context, console, or translator failures
5. **External dependency errors**: Missing external assets or invalid configurations

We needed a comprehensive error handling strategy that:
- Distinguishes between different types of errors
- Provides meaningful error messages
- Handles errors gracefully without breaking the entire pipeline
- Integrates with Dagster's error reporting system

## Decision

We implemented a **Layered Error Handling Strategy** with:

1. **Custom Exception Classes**: Define specific exception types for different error scenarios
2. **Error Classification**: Distinguish between transient, persistent, and upstream errors
3. **Graceful Degradation**: Handle errors without breaking the entire pipeline
4. **Comprehensive Logging**: Provide detailed error information for debugging
5. **Dagster Integration**: Proper integration with Dagster's error reporting

## Implementation

### Custom Exception Classes

```python
class UpstreamAuditFailureError(Exception):
    """
    Custom exception for upstream audit failures that should be handled gracefully.
    This exception should not trigger retries or be re-raised by the factory.
    """
    pass
```

### Error Classification in Resource

```python
def _process_failed_models_events(self) -> list[AssetCheckResult]:
    """Process failed models events and create appropriate AssetCheckResults."""
    asset_check_results = []
    
    for error in self._console.get_failed_models_events():
        try:
            model_name, asset_key, error_type = self._extract_model_info(error)
            
            # Process different error types
            if error_type == "audit":
                result = self._create_failed_audit_check_result(error, model_name, asset_key)
            else:
                result = self._create_general_error_check_result(error, model_name, asset_key, error_type, str(error))
            
            if result:
                asset_check_results.append(result)
                
        except Exception as e:
            self._log_failed_error_processing(e)
    
    return self._deduplicate_asset_check_results(asset_check_results)
```

### SQLMesh Error Mapping

```python
from sqlmesh.utils.errors import (
    SQLMeshError,
    PlanError,
    ConflictingPlanError,
    NodeAuditsErrors,
    CircuitBreakerError,
    NoChangesPlanError,
    UncategorizedPlanError,
    AuditError,
    PythonModelEvalError,
    SignalEvalError,
)

def _classify_sqlmesh_error(self, error) -> str:
    """Classify SQLMesh errors into our error categories."""
    if isinstance(error, AuditError):
        return "audit"
    elif isinstance(error, (PlanError, ConflictingPlanError)):
        return "plan"
    elif isinstance(error, CircuitBreakerError):
        return "circuit_breaker"
    elif isinstance(error, PythonModelEvalError):
        return "python_model"
    else:
        return "general"
```

### Graceful Error Handling

```python
def handle_audit_failures(context, current_model_name, current_asset_spec, current_model_checks, failed_check_results):
    """Handle audit failures gracefully without breaking the pipeline."""
    
    # Find relevant audit failures for this model
    model_failures = [
        result for result in failed_check_results 
        if result.asset_key == current_asset_spec.key
    ]
    
    if model_failures:
        context.log.warning(f"⚠️ Model {current_model_name} has audit failures: {len(model_failures)}")
        
        # Create appropriate result based on audit type
        if any(result.metadata.get("audit_blocking", True) for result in model_failures):
            # Blocking audit failure - raise exception
            raise UpstreamAuditFailureError(f"Blocking audit failures for {current_model_name}")
        else:
            # Non-blocking audit failure - continue with warning
            context.log.info(f"ℹ️ Non-blocking audit failures for {current_model_name}, continuing...")
    
    return create_materialize_result(...)
```

## Rationale

### Why Custom Exceptions?

1. **Error Classification**: Distinguish between different types of errors
2. **Graceful Handling**: Some errors should not break the entire pipeline
3. **Debugging**: Provide specific error types for better debugging
4. **Integration**: Better integration with Dagster's error reporting

### Why Layered Error Handling?

1. **Error Isolation**: Errors in one asset don't affect others
2. **Graceful Degradation**: Pipeline continues even with some failures
3. **Comprehensive Coverage**: Handle all possible error scenarios
4. **User Experience**: Provide meaningful error messages

### Why SQLMesh Error Mapping?

1. **Error Understanding**: Map SQLMesh errors to our categories
2. **Appropriate Responses**: Different error types need different handling
3. **Debugging**: Better error messages for different scenarios
4. **Integration**: Proper integration with SQLMesh error system

### Alternative Approaches Considered

1. **Generic Exception Handling**: ❌ Would lose error specificity
2. **Fail Fast Strategy**: ❌ Would break entire pipeline on any error
3. **Silent Error Handling**: ❌ Would hide important error information
4. **External Error Handling**: ❌ Would not integrate with Dagster

## Consequences

### Positive

- ✅ **Error Classification**: Clear distinction between error types
- ✅ **Graceful Handling**: Pipeline continues despite some failures
- ✅ **Better Debugging**: Specific error types and messages
- ✅ **Dagster Integration**: Proper integration with Dagster error reporting
- ✅ **User Experience**: Meaningful error messages
- ✅ **Error Isolation**: Errors don't cascade across assets

### Negative

- ⚠️ **Complexity**: More complex error handling logic
- ⚠️ **Performance**: Additional error processing overhead
- ⚠️ **Maintenance**: Need to maintain error classification logic
- ⚠️ **Learning Curve**: Developers need to understand error types

### Trade-offs

1. **Complexity vs Robustness**: We prioritize robustness over simplicity
2. **Performance vs Error Handling**: We accept performance overhead for better error handling
3. **Simplicity vs Specificity**: We prioritize specific error handling over simple handling

## Error Categories

### 1. Audit Errors
- **Blocking**: Prevent asset success, trigger retries
- **Non-blocking**: Allow asset success with warnings

### 2. Plan Errors
- **Conflicting Plans**: Handle gracefully, retry
- **No Changes**: Continue normally
- **Circuit Breaker**: Stop execution, manual intervention

### 3. Resource Errors
- **Initialization**: Fail fast, clear error message
- **Configuration**: Provide helpful error message

### 4. External Dependency Errors
- **Missing Assets**: Handle gracefully, continue
- **Invalid Configuration**: Fail with clear message

## Implementation Guidelines

### Error Handling Patterns

1. **Try-Catch with Classification**: Classify errors before handling
2. **Graceful Degradation**: Continue execution when possible
3. **Comprehensive Logging**: Log all error details for debugging
4. **User-Friendly Messages**: Provide clear, actionable error messages

### Error Recovery Strategies

1. **Retry Logic**: For transient errors
2. **Skip Logic**: For non-critical errors
3. **Fallback Logic**: For missing dependencies
4. **Manual Intervention**: For persistent errors

## Related ADRs

- [ADR-0003](./0003-asset-check-integration.md) - Asset Check Integration for SQLMesh Audits
- [ADR-0004](./0004-retry-policy-management.md) - Retry Policy Management for SQLMesh Integration

## Future Considerations

1. **Error Metrics**: Add metrics for different error types
2. **Error Recovery**: Implement automatic error recovery strategies
3. **Error Notifications**: Add error notification system
4. **Error Documentation**: Document all possible error scenarios 