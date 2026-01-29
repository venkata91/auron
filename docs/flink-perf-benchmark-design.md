# Auron Flink Performance Benchmarking Framework

## Overview
Lightweight framework to compare Flink native execution vs Auron native execution performance across different SQL operations and data scales.

## Goals
- ✅ **Simple**: Easy to add new benchmarks, minimal boilerplate
- ✅ **Comparative**: Side-by-side Flink vs Auron execution metrics
- ✅ **Scalable**: Test with different data sizes (1K, 10K, 100K, 1M rows)
- ✅ **Comprehensive**: Cover common SQL patterns (scan, filter, project, UDF, aggregation)
- ❌ **Not**: Full microbenchmarking suite (use JMH for that), not for CI/CD regression testing

## Architecture

### 1. Test Data Generator
```
DataGenerator
├── generateParquetData(schema, rowCount, outputPath)
│   ├── Configurable schemas (primitives, strings, dates, decimals)
│   ├── Realistic data distribution (nulls, duplicates, skew)
│   └── Multiple scales: SMALL(1K), MEDIUM(10K), LARGE(100K), XLARGE(1M)
└── Reusable datasets cached on disk
```

**Schema Templates:**
- `basic`: (id INT, name STRING, amount DOUBLE, date DATE)
- `wide`: 20+ columns of mixed types
- `string_heavy`: Multiple VARCHAR columns for UDF testing
- `numeric`: INT, BIGINT, DOUBLE, DECIMAL for aggregations

### 2. Benchmark Executor
```
BenchmarkRunner
├── runWithFlink(sql, tableName) → Metrics
├── runWithAuron(sql, tableName) → Metrics
└── compare(flinkMetrics, auronMetrics) → ComparisonReport
```

**Metrics Collected:**
- Execution time (ms)
- Rows processed
- Memory usage (optional)
- Speedup ratio (Auron vs Flink)

### 3. SQL Test Cases

| Category | SQL Pattern | Purpose |
|----------|-------------|---------|
| **Scan** | `SELECT * FROM table` | Baseline I/O performance |
| **Project** | `SELECT col1, col2, col3 FROM table` | Column pruning efficiency |
| **Filter** | `SELECT * FROM table WHERE amount > 100` | Filter pushdown performance |
| **Calc** | `SELECT LOWER(name), UPPER(name) FROM table` | Scalar function execution |
| **Complex Filter** | `SELECT * WHERE amount > 100 AND date > '2024-01-01'` | Multi-predicate pushdown |
| **Project + Filter** | `SELECT id, name WHERE amount > 100` | Combined pushdown |
| **Aggregation** | `SELECT COUNT(*), SUM(amount) FROM table` | Aggregation performance |
| **GroupBy** | `SELECT name, COUNT(*) FROM table GROUP BY name` | Grouping efficiency |
| **Calc + Filter** | `SELECT UPPER(name) WHERE amount > 100` | UDF + filter combo |

### 4. Output Format

**Console Output:**
```
========================================
Benchmark: Project + Filter (MEDIUM - 10K rows)
========================================
Query: SELECT id, name FROM table WHERE amount > 100

Flink Native:     152 ms  (6,667 rows)
Auron Native:      48 ms  (6,667 rows)
Speedup:          3.17x
Memory Δ:         +12 MB

✅ PASSED - Results match
========================================
```

**CSV Export (optional):**
```csv
benchmark,scale,rows,flink_ms,auron_ms,speedup,memory_mb
scan,SMALL,1000,45,15,3.0,8
project,SMALL,1000,42,12,3.5,8
filter,MEDIUM,10000,156,52,3.0,24
```

## Implementation Plan

### Phase 1: Core Framework (1-2 hours)
```java
// File: AuronFlinkBenchmarkBase.java
- Setup Flink environment with/without Auron
- Data generator with configurable schemas
- Benchmark runner with timing logic
- Result comparison and validation
```

### Phase 2: Test Cases (1-2 hours)
```java
// File: AuronFlinkBenchmarkSuite.java
- Implement 9 benchmark test cases
- Use @Test with @Tag("benchmark") for optional execution
- Parameterized tests for different scales
```

### Phase 3: Reporting (30 mins)
```java
// File: BenchmarkReporter.java
- Console output formatter
- Optional CSV export
- Summary statistics (avg speedup, etc.)
```

## File Structure
```
auron-flink-planner/src/test/java/
└── org/apache/auron/flink/benchmark/
    ├── AuronFlinkBenchmarkBase.java       # Base class with setup/teardown
    ├── AuronFlinkBenchmarkSuite.java      # Actual benchmark tests
    ├── DataGenerator.java                  # Parquet data generation
    ├── BenchmarkMetrics.java               # Metrics data class
    └── BenchmarkReporter.java              # Output formatting
```

## Usage

### Run All Benchmarks
```bash
./build/mvn test -Pflink -pl :auron-flink-planner \
    -Dtest=AuronFlinkBenchmarkSuite \
    -Dgroups=benchmark
```

### Run Specific Scale
```bash
# Only SMALL scale (fast, for dev)
./build/mvn test -Pflink -pl :auron-flink-planner \
    -Dtest=AuronFlinkBenchmarkSuite \
    -Dbenchmark.scale=SMALL

# Only LARGE scale (comprehensive)
./build/mvn test -Pflink -pl :auron-flink-planner \
    -Dtest=AuronFlinkBenchmarkSuite \
    -Dbenchmark.scale=LARGE
```

### Run Specific Benchmark
```bash
./build/mvn test -Pflink -pl :auron-flink-planner \
    -Dtest=AuronFlinkBenchmarkSuite#benchmarkProjectWithFilter
```

## Key Design Decisions

### 1. **Not Using JMH**
- **Why**: JMH is heavyweight, requires separate module, complex setup
- **Trade-off**: Less precise (no warmup, GC control), but good enough for comparative testing
- **Mitigation**: Run each query 3x, take median to reduce variance

### 2. **Test Data Caching**
- Generate Parquet files once, reuse across benchmarks
- Store in `target/benchmark-data/` (gitignored)
- Regenerate only if schema changes

### 3. **Result Validation**
- Always compare Flink vs Auron results for correctness
- Fail benchmark if results don't match
- Performance is secondary to correctness

### 4. **Optional Execution**
- Use `@Tag("benchmark")` to exclude from regular test runs
- Benchmarks are opt-in, not part of CI/CD
- Developers run manually when needed

## Non-Goals
- ❌ Microbenchmarking (use JMH for that)
- ❌ CI/CD regression testing (too flaky)
- ❌ Production performance monitoring
- ❌ Distributed/cluster benchmarks
- ❌ Memory profiling (use JProfiler/YourKit)

## Success Criteria
- ✅ Can run 9 SQL patterns across 4 scales in < 5 minutes
- ✅ Clear speedup metrics (Auron vs Flink)
- ✅ Results are reproducible (±10% variance)
- ✅ Easy to add new benchmarks (< 10 lines of code)
- ✅ Validates correctness (results match)

## Future Enhancements (Out of Scope)
- Multi-threaded execution benchmarks
- Join operation benchmarks
- Window function benchmarks
- Complex nested query benchmarks
- Comparison with other engines (Spark, DuckDB)
