# 🔍 Test Coverage Gap Analysis

## Executive Summary

Current test suite has **397 tests** with **78% pass rate**. This analysis identifies missing test scenarios and suggests improvements to reach **comprehensive coverage**.

---

## 📊 Current Coverage Analysis

### ✅ Well-Covered Areas (100%)
- Basic filters (numeric, string, timestamp, boolean, date)
- Simple aggregates (COUNT, SUM, AVG, MIN, MAX)
- Basic JOINs (INNER JOIN)
- Grouping (single and composite)
- Ordering (ASC/DESC, multi-column)
- Pagination (LIMIT/OFFSET)
- SQL functions (DATE_TRUNC, EXTRACT, string ops)
- Edge cases and NULL handling
- JSON extraction
- HAVING clauses
- Context params
- Base SQL rewriting

---

## ⚠️ Missing or Incomplete Coverage

### 1. Window Functions - NOT TESTED ⚠️

**Status**: Code indicates support but NO tests exist

**Missing Tests**:
- ✗ ROW_NUMBER() OVER (PARTITION BY ...)
- ✗ RANK() / DENSE_RANK()
- ✗ NTILE(n)
- ✗ LEAD() / LAG()
- ✗ FIRST_VALUE() / LAST_VALUE() / NTH_VALUE()
- ✗ SUM/AVG/COUNT OVER (window)
- ✗ Window frame specifications (ROWS BETWEEN, RANGE BETWEEN)

**Priority**: HIGH - Window functions are common in analytics queries

---

### 2. JOIN Types - PARTIALLY TESTED ⚠️

**Current**: Only INNER JOIN tested  
**Missing**:
- ✗ LEFT JOIN / LEFT OUTER JOIN
- ✗ RIGHT JOIN / RIGHT OUTER JOIN
- ✗ FULL OUTER JOIN
- ✗ CROSS JOIN
- ✗ Self-joins
- ✗ JOIN with NULL keys
- ✗ JOIN with different data types

**Priority**: HIGH - Different JOIN types have different semantics

---

### 3. DISTINCT Operations - NOT TESTED ⚠️

**Missing**:
- ✗ SELECT DISTINCT
- ✗ DISTINCT with multiple columns
- ✗ DISTINCT with NULL values
- ✗ COUNT(DISTINCT) with multiple aggregates
- ✗ DISTINCT with GROUP BY

**Priority**: MEDIUM

---

### 4. Set Operations - NOT TESTED ⚠️

**Missing**:
- ✗ UNION
- ✗ UNION ALL
- ✗ INTERSECT
- ✗ EXCEPT
- ✗ Set operations with ORDER BY
- ✗ Set operations with different column types

**Priority**: LOW - Less commonly used in widget queries

---

### 5. Subqueries - PARTIALLY TESTED ⚠️

**Current**: Base SQL can contain subqueries  
**Missing**:
- ✗ WHERE EXISTS (subquery)
- ✗ WHERE NOT EXISTS (subquery)
- ✗ WHERE IN (subquery)
- ✗ Scalar subqueries in SELECT
- ✗ Correlated subqueries
- ✗ Multiple levels of nesting

**Priority**: HIGH - Subqueries are common in complex analytics

---

### 6. CASE Expressions - NOT SYSTEMATICALLY TESTED ⚠️

**Missing**:
- ✗ Simple CASE WHEN
- ✗ CASE with multiple WHEN clauses
- ✗ CASE with complex expressions
- ✗ CASE in SELECT, WHERE, ORDER BY
- ✗ Nested CASE expressions
- ✗ CASE with NULL handling

**Priority**: HIGH - Very common in business logic

---

### 7. Data Type Operations - PARTIALLY TESTED ⚠️

**Missing**:
- ✗ CAST between all type pairs
- ✗ Implicit type conversions
- ✗ Type coercion in comparisons
- ✗ INTERVAL arithmetic
- ✗ Array element access (`array[index]`)
- ✗ Struct field access
- ✗ JSON path expressions (beyond basic extraction)

**Priority**: MEDIUM

---

### 8. Mathematical Operations - NOT TESTED ⚠️

**Missing**:
- ✗ Advanced math functions (SQRT, POW, LOG, EXP)
- ✗ Trigonometric functions (SIN, COS, TAN)
- ✗ Rounding functions (ROUND, CEIL, FLOOR, TRUNC)
- ✗ Modulo and division operations
- ✗ Mathematical operations with NULL
- ✗ Overflow handling

**Priority**: LOW

---

### 9. String Operations - PARTIALLY TESTED ⚠️

**Current**: Basic string functions tested  
**Missing**:
- ✗ LIKE patterns with escape characters
- ✗ SIMILAR TO (SQL regex)
- ✗ REGEXP_MATCHES / REGEXP_REPLACE
- ✗ String splitting
- ✗ POSITION, STRPOS
- ✗ LPAD, RPAD
- ✗ REPLACE
- ✗ REVERSE
- ✗ Multi-byte character handling

**Priority**: MEDIUM

---

### 10. Date/Time Operations - PARTIALLY TESTED ⚠️

**Current**: Basic date functions tested  
**Missing**:
- ✗ INTERVAL arithmetic (date + INTERVAL '1 day')
- ✗ AGE function
- ✗ Date/time precision conversions
- ✗ Timezone handling
- ✗ CURRENT_DATE, CURRENT_TIME, NOW()
- ✗ TO_TIMESTAMP, TO_DATE
- ✗ Date part extraction (EPOCH, DOY, QUARTER)
- ✗ Date overlap calculations

**Priority**: HIGH - Important for time-series analysis

---

### 11. NULL Handling - PARTIALLY TESTED ⚠️

**Current**: Basic IS NULL / IS NOT NULL tested  
**Missing**:
- ✗ COALESCE with multiple arguments
- ✗ NULLIF detailed scenarios
- ✗ NULL in arithmetic (result propagation)
- ✗ NULL in comparisons (three-valued logic)
- ✗ NULL in aggregates (COUNT vs COUNT(*))
- ✗ NULL in JOINs (NULL != NULL)
- ✗ NULL in window functions

**Priority**: MEDIUM

---

### 12. Conditional Logic - PARTIALLY TESTED ⚠️

**Missing**:
- ✗ IIF function (IF inline)
- ✗ Complex boolean logic (multiple AND/OR)
- ✗ Short-circuit evaluation
- ✗ Boolean operator precedence

**Priority**: LOW

---

### 13. Query Optimization Patterns - NOT TESTED ⚠️

**Missing**:
- ✗ Query with indexes
- ✗ Query plan analysis
- ✗ Large result set handling (>10M rows)
- ✗ Memory-intensive operations
- ✗ Concurrent query execution

**Priority**: MEDIUM - Important for performance

---

### 14. Error Scenarios - MINIMALLY TESTED ⚠️

**Current**: Some edge cases tested  
**Missing**:
- ✗ Malformed queries
- ✗ Type mismatches
- ✗ Division by zero
- ✗ Out-of-range values
- ✗ Invalid date/time values
- ✗ Circular references
- ✗ Too many JOINs
- ✗ Stack overflow (deeply nested queries)

**Priority**: MEDIUM - Important for robustness

---

### 15. Security Patterns - NOT TESTED ⚠️

**Missing**:
- ✗ SQL injection prevention in context params
- ✗ Row-level security patterns
- ✗ Column-level security
- ✗ Query size limits
- ✗ Execution time limits

**Priority**: LOW - Likely handled at higher level

---

## 🎯 Recommended Test Additions

### Phase 1: High-Value Tests (Immediate)

#### 1. Window Functions Suite (~25 tests)
```typescript
- ROW_NUMBER() basic and with PARTITION BY
- RANK() / DENSE_RANK() comparisons
- LEAD/LAG with offsets and defaults
- Window aggregates (SUM OVER, AVG OVER)
- Window frames (ROWS BETWEEN)
```

#### 2. JOIN Types Suite (~15 tests)
```typescript
- LEFT JOIN with NULL handling
- RIGHT JOIN scenarios
- FULL OUTER JOIN
- Self-joins
- CROSS JOIN (Cartesian product)
```

#### 3. CASE Expressions Suite (~15 tests)
```typescript
- Simple CASE WHEN
- Multiple WHEN clauses
- CASE in different clauses
- Nested CASE
- CASE with NULL
```

#### 4. Subquery Suite (~20 tests)
```typescript
- WHERE EXISTS
- WHERE NOT EXISTS
- WHERE IN (subquery)
- Scalar subqueries
- Correlated subqueries
```

---

### Phase 2: Medium-Value Tests

#### 5. DISTINCT Operations Suite (~10 tests)
```typescript
- SELECT DISTINCT
- DISTINCT with GROUP BY
- DISTINCT on multiple columns
- DISTINCT with NULL
```

#### 6. Advanced Date/Time Suite (~15 tests)
```typescript
- INTERVAL arithmetic
- Timezone handling
- Current date/time functions
- Date part extraction
```

#### 7. Advanced String Operations (~10 tests)
```typescript
- LIKE with escape
- REGEXP operations
- String splitting
- Multi-byte characters
```

---

### Phase 3: Completeness Tests

#### 8. Mathematical Operations Suite (~10 tests)
#### 9. Type Conversion Suite (~10 tests)
#### 10. Error Handling Suite (~15 tests)
#### 11. Performance Suite (~10 tests)

---

## 📈 Coverage Improvement Plan

### Current State
```
Total Tests: 397
Coverage: ~70% of engine features
Missing: ~30% of features
```

### Target State (Phase 1)
```
Total Tests: ~472 (add 75 tests)
Coverage: ~85% of engine features
Focus: High-value analytics features
```

### Target State (Phase 2)
```
Total Tests: ~507 (add 35 tests)
Coverage: ~90% of engine features
Focus: Advanced operations
```

### Target State (Phase 3)
```
Total Tests: ~552 (add 45 tests)
Coverage: ~95% of engine features
Focus: Robustness and edge cases
```

---

## 🔍 Existing Test Permutation Analysis

### Filter Tests - Need More Combinations
**Current**: Each data type tested individually  
**Improvement Needed**:
- ✗ All numeric types with all operators (systematic matrix)
- ✗ Cross-type comparisons (BIGINT vs NUMERIC)
- ✗ Boundary values (MIN, MAX, 0, -1)
- ✗ Large vs small values

### Aggregate Tests - Missing Combinations
**Current**: Basic aggregates tested  
**Improvement Needed**:
- ✗ Multiple aggregates in single query
- ✗ Aggregates with DISTINCT
- ✗ Aggregates with FILTER WHERE (SQL:2003)
- ✗ Nested aggregates (where allowed)
- ✗ Aggregates with CASE

### JOIN Tests - Need More Scenarios
**Current**: Basic INNER JOIN tested  
**Improvement Needed**:
- ✗ Chained JOINs (3+ tables)
- ✗ JOIN with different join conditions
- ✗ JOIN with OR conditions
- ✗ JOIN with computed columns
- ✗ JOIN with aggregates on both sides

---

## 🎯 Recommendations Summary

### Immediate Actions (This Session)
1. ✅ Implement Window Functions Suite (25 tests)
2. ✅ Implement Extended JOIN Types Suite (15 tests)
3. ✅ Implement CASE Expressions Suite (15 tests)
4. ✅ Implement DISTINCT Operations Suite (10 tests)

**Total Added**: ~65 tests  
**New Total**: ~462 tests  
**Expected Pass Rate**: ~80-82% (similar blockers apply)

### Next Session
5. Subqueries Suite (20 tests)
6. Advanced Date/Time Suite (15 tests)
7. Advanced String Operations (10 tests)

---

## 💡 Key Insights

1. **Window Functions**: Critical gap - widely used in analytics
2. **JOIN Types**: Important gap - different semantics needed
3. **CASE Expressions**: Common pattern - needs systematic testing
4. **Current Tests**: Well-structured but focused on basics
5. **Systematic Coverage**: Need more permutation matrices for existing features

---

**Generated**: November 20, 2025  
**Analysis Version**: 1.0  
**Current Tests**: 397  
**Identified Gaps**: 14 major areas

