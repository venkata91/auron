# Auron Flink Performance Benchmark Framework

## Overview

Lightweight benchmarking framework to measure and compare Flink native execution vs Auron native execution performance across different SQL operations.

## Architecture

```
AuronFlinkBenchmarkSuite (12 tests)
    ↓
AuronFlinkBenchmarkBase
    ├── setupFlinkEnvironment()    → Flink native (Auron disabled)
    ├── setupAuronEnvironment()    → Auron native (Auron enabled)
    ├── runWithFlink(sql)          → Execute & time
    ├── runWithAuron(sql)          → Execute & time
    └── runBenchmark()             → Compare & report

DataGenerator → Parquet test data (cached)
BenchmarkReporter → Formatted comparison output
```

## Test Coverage (12 Benchmarks)

| Category | Tests | SQL Patterns |
|----------|-------|--------------|
| **Scan** | Full Scan | `SELECT *` |
| **Projection** | Project Columns, Project + Filter | `SELECT cols`, `SELECT cols WHERE` |
| **Filter** | Simple, Complex, With Aggregation | `WHERE X`, `WHERE X AND Y` |
| **CALC** | LOWER(), UPPER(), Multiple, With Filter | String UDFs |
| **Aggregation** | COUNT(*), COUNT + Filter, SUM | Aggregations |

## Data Scales

- **SMALL** (1K rows) - Quick validation, ~30 sec
- **MEDIUM** (10K rows) - Standard testing, ~2 min
- **LARGE** (100K rows) - Comprehensive, ~10 min
- **XLARGE** (1M rows) - Stress testing, ~30 min

## Usage

### Run All Benchmarks
```bash
./build/mvn test -Pflink -pl :auron-flink-planner -am \
    -Dtest=AuronFlinkBenchmarkSuite \
    -Dbenchmark.scale=SMALL \
    -Dspotless.apply.skip=true
```

### Run Specific Test
```bash
./build/mvn test -Pflink -pl :auron-flink-planner -am \
    -Dtest=AuronFlinkBenchmarkSuite#benchmark01_FullScan \
    -Dspotless.apply.skip=true
```

## Sample Output

```
================================================================================
Benchmark: Full Scan (SMALL - 1,000 rows)
================================================================================
Query: SELECT * FROM benchmark_table

Flink Native:   3,088 ms  (1,000 rows)
Auron Native:     734 ms  (1,000 rows)
Speedup:       4.21x faster

✅ PASSED - Row counts match
================================================================================

BENCHMARK SUMMARY
================================================================================
Full Scan                      SMALL   : 4.21x speedup
Project Columns                SMALL   : 3.50x speedup
Simple Filter                  SMALL   : 3.80x speedup
...
--------------------------------------------------------------------------------
Average Speedup: 3.84x (12/12 benchmarks passed)
================================================================================
```

## Key Features

✅ **Dual Execution** - Runs each query with both Flink and Auron  
✅ **Data Caching** - Generated Parquet files reused across runs  
✅ **Result Validation** - Verifies row counts match between executions  
✅ **Scalable** - Test from 1K to 1M rows  
✅ **Tagged Tests** - Excluded from regular CI/CD with `@Tag("benchmark")`  
✅ **Easy Extension** - Add new benchmark in <10 lines of code

## Implementation Details

- **Base Class**: `AuronFlinkBenchmarkBase` extends `AuronFlinkTableTestBase`
- **Test Suite**: `AuronFlinkBenchmarkSuite` with 12 `@Test` methods
- **Data Location**: `target/benchmark-data/` (cached)
- **Execution Mode**: BATCH mode for consistent timing
- **Environment Isolation**: Separate environments for Flink vs Auron

## Design Decisions

1. **Lightweight over JMH** - Simple, easy to extend, minimal boilerplate
2. **Cached Data** - Faster subsequent runs, consistent datasets
3. **Correctness First** - Always validates results match before reporting speedup
4. **Opt-in Execution** - Tagged to avoid slowing down regular test runs

## Files Added

```
auron-flink-planner/src/test/java/org/apache/auron/flink/benchmark/
├── AuronFlinkBenchmarkBase.java       (193 lines) - Base test infrastructure
├── AuronFlinkBenchmarkSuite.java      (214 lines) - 12 benchmark tests
├── BenchmarkMetrics.java              (75 lines)  - Metrics data class
├── BenchmarkReporter.java             (115 lines) - Output formatting
├── DataGenerator.java                 (174 lines) - Test data generation
├── DataScale.java                     (47 lines)  - Scale enum
└── README.md                          - User guide

docs/flink-perf-benchmark-design.md    - Design document
```

**Total**: ~818 lines of code

## Next Steps

1. ✅ Framework implemented and tested
2. ⏭️ Run comprehensive benchmarks (MEDIUM/LARGE scale)
3. ⏭️ Document performance improvements in PR
4. ⏭️ Add join/window function benchmarks (future)
5. ⏭️ CSV export for results tracking (future)
