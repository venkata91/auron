# Auron Flink Performance Benchmark Suite

Lightweight framework to compare Flink native execution vs Auron native execution performance.

## Quick Start

### Run All Benchmarks (SMALL scale - 1K rows)
```bash
cd /Users/argoyal/Documents/Work/auron

./build/mvn test -Pflink -pl :auron-flink-planner \
    -Dtest=AuronFlinkBenchmarkSuite \
    -Dgroups=benchmark \
    -Dspotless.apply.skip=true
```

### Run with Different Scales

**SMALL (1K rows) - Fast, for development:**
```bash
./build/mvn test -Pflink -pl :auron-flink-planner \
    -Dtest=AuronFlinkBenchmarkSuite \
    -Dbenchmark.scale=SMALL \
    -Dgroups=benchmark \
    -Dspotless.apply.skip=true
```

**MEDIUM (10K rows) - Balanced:**
```bash
./build/mvn test -Pflink -pl :auron-flink-planner \
    -Dtest=AuronFlinkBenchmarkSuite \
    -Dbenchmark.scale=MEDIUM \
    -Dgroups=benchmark \
    -Dspotless.apply.skip=true
```

**LARGE (100K rows) - Comprehensive:**
```bash
./build/mvn test -Pflink -pl :auron-flink-planner \
    -Dtest=AuronFlinkBenchmarkSuite \
    -Dbenchmark.scale=LARGE \
    -Dgroups=benchmark \
    -Dspotless.apply.skip=true
```

**XLARGE (1M rows) - Stress test:**
```bash
./build/mvn test -Pflink -pl :auron-flink-planner \
    -Dtest=AuronFlinkBenchmarkSuite \
    -Dbenchmark.scale=XLARGE \
    -Dgroups=benchmark \
    -Dspotless.apply.skip=true
```

### Run Specific Benchmark

```bash
# Run only the CALC benchmarks
./build/mvn test -Pflink -pl :auron-flink-planner \
    -Dtest=AuronFlinkBenchmarkSuite#benchmark06_CalcLower \
    -Dgroups=benchmark \
    -Dspotless.apply.skip=true

# Run only filter benchmarks
./build/mvn test -Pflink -pl :auron-flink-planner \
    -Dtest=AuronFlinkBenchmarkSuite#benchmark03_SimpleFilter,benchmark04_ProjectWithFilter \
    -Dgroups=benchmark \
    -Dspotless.apply.skip=true
```

## Benchmark Categories

| Benchmark | SQL Pattern | Tests |
|-----------|-------------|-------|
| **benchmark01_FullScan** | `SELECT * FROM table` | Baseline I/O |
| **benchmark02_ProjectColumns** | `SELECT col1, col2 FROM table` | Column pruning |
| **benchmark03_SimpleFilter** | `SELECT * WHERE amount > X` | Filter pushdown |
| **benchmark04_ProjectWithFilter** | `SELECT cols WHERE condition` | Combined pushdown |
| **benchmark05_ComplexFilter** | `WHERE cond1 AND cond2` | Multi-predicate |
| **benchmark06_CalcLower** | `SELECT LOWER(name)` | String UDF |
| **benchmark07_CalcUpper** | `SELECT UPPER(name)` | String UDF |
| **benchmark08_CalcWithFilter** | `SELECT UPPER(name) WHERE X` | UDF + filter |
| **benchmark09_MultipleCalc** | `SELECT LOWER(x), UPPER(x)` | Multiple UDFs |
| **benchmark10_Count** | `SELECT COUNT(*)` | Aggregation |
| **benchmark11_CountWithFilter** | `SELECT COUNT(*) WHERE X` | Agg + filter |
| **benchmark12_SumAggregation** | `SELECT SUM(amount)` | Sum aggregation |

## Output Format

### Individual Benchmark Output
```
================================================================================
Benchmark: Project + Filter (MEDIUM - 10,000 rows)
================================================================================
Query: SELECT id, name, amount FROM benchmark_table WHERE amount > 3333

Flink Native:     152 ms  (6,667 rows)
Auron Native:      48 ms  (6,667 rows)
Speedup:       3.17x faster

✅ PASSED - Row counts match
================================================================================
```

### Summary Output
```
================================================================================
BENCHMARK SUMMARY
================================================================================
Full Scan                      MEDIUM  : 2.85x speedup
Project Columns                MEDIUM  : 3.12x speedup
Simple Filter                  MEDIUM  : 3.45x speedup
Project + Filter               MEDIUM  : 3.17x speedup
Complex Filter                 MEDIUM  : 3.28x speedup
CALC - LOWER()                 MEDIUM  : 2.95x speedup
CALC - UPPER()                 MEDIUM  : 2.98x speedup
CALC + Filter                  MEDIUM  : 3.05x speedup
Multiple CALC Functions        MEDIUM  : 2.87x speedup
COUNT(*)                       MEDIUM  : 4.12x speedup
COUNT(*) with Filter           MEDIUM  : 3.89x speedup
SUM Aggregation                MEDIUM  : 4.05x speedup

--------------------------------------------------------------------------------
Average Speedup: 3.23x (12/12 benchmarks passed)
================================================================================
```

## Data Caching

Test data is cached in `target/benchmark-data/` to avoid regeneration:

```
target/benchmark-data/
├── benchmark_table_small/    # 1K rows
├── benchmark_table_medium/   # 10K rows
├── benchmark_table_large/    # 100K rows
└── benchmark_table_xlarge/   # 1M rows
```

To regenerate data, delete the cache directory:
```bash
rm -rf target/benchmark-data/
```

## Framework Architecture

```
AuronFlinkBenchmarkBase
├── setupFlinkEnvironment()    # Flink native execution
├── setupAuronEnvironment()    # Auron native execution
├── prepareTestData()          # Generate/cache Parquet data
├── runWithFlink(sql)          # Execute with Flink
├── runWithAuron(sql)          # Execute with Auron
└── runBenchmark()             # Compare and report

DataGenerator
├── generateBasicData()        # Standard schema
├── generateDataWithNulls()    # NULL handling tests
├── generateStringHeavyData()  # UDF-heavy tests
└── generateSkewedData()       # Aggregation tests

BenchmarkReporter
├── printComparison()          # Individual results
└── printSummary()             # Overall summary
```

## Adding New Benchmarks

1. Add a new test method in `AuronFlinkBenchmarkSuite.java`:

```java
@Test
public void benchmark13_MyNewBenchmark() {
    if (!isAuronAvailable()) return;

    String sql = "SELECT ... FROM " + TABLE_NAME + " WHERE ...";
    runBenchmark("My New Benchmark", scale, sql);
}
```

2. Run it:
```bash
./build/mvn test -Pflink -pl :auron-flink-planner \
    -Dtest=AuronFlinkBenchmarkSuite#benchmark13_MyNewBenchmark \
    -Dgroups=benchmark
```

## Notes

- **Not for CI/CD**: Benchmarks are tagged with `@Tag("benchmark")` and excluded from regular test runs
- **Requires Auron Library**: Tests are skipped if `libauron` is not available
- **Batch Mode**: All benchmarks run in BATCH mode for consistent timing
- **Median of 1 Run**: Each query runs once (not JMH-style warmup/iterations)
- **Correctness First**: Benchmarks fail if Flink and Auron results don't match

## Troubleshooting

### Auron library not found
```
⚠️  Auron native library not available - benchmarks will be skipped
```
**Solution**: Build Auron native library first:
```bash
cd auron-core
cargo build --release
```

### Out of Memory
**Solution**: Increase heap size:
```bash
export MAVEN_OPTS="-Xmx4g"
./build/mvn test ...
```

### Slow Performance
**Solution**: Use smaller scale for development:
```bash
-Dbenchmark.scale=SMALL
```

## Performance Tips

1. **Warm up JVM**: Run benchmarks twice, use second run results
2. **Close other apps**: Minimize background processes
3. **Use LARGE scale**: For meaningful comparisons (100K+ rows)
4. **Check GC**: Add `-XX:+PrintGCDetails` to see GC impact
5. **Disable logging**: Set log level to WARN/ERROR

## Future Enhancements

- [ ] CSV export for results
- [ ] Comparison charts/graphs
- [ ] Multi-threaded execution tests
- [ ] Join operation benchmarks
- [ ] Window function benchmarks
- [ ] Memory profiling integration
