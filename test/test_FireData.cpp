// Fire Data Benchmark
// Runs in unoptimized or optimized mode by compiling with or without -fopenmp


#include <cstdio>
#include <string>
#include "firedata/fireData.hpp"
#include "test/benchmark.hpp"
#include "utils.hpp"

// Constants for number of iterations to run for averaging
const int LOAD_ITERATIONS = 3;
const int QUERY_ITERATIONS = 5;


int main(int argc, char** argv) {
    printf("\n========================================\n");
    printf("Fire Data Benchmark\n");
#ifdef _OPENMP
    printf("Mode: OPTIMIZED (OpenMP enabled, threads=%d)\n", numThreads());
#else
    printf("Mode: SERIAL (no OpenMP)\n");
#endif
    printf("========================================\n\n");

    // Default data path
    std::string dataPath = "../datasets/2020-fire/data/20200810/20200810-01.csv";
    if (argc > 1) {
        dataPath = argv[1];
    }

    printf("Data path: %s\n\n", dataPath.c_str());

    // Load the benchmark data iteratively to get average load time
    // Creates BenchmarkStats object to track timing statistics
    BenchmarkStats loadStats("Load");
    for (int i = 0; i < LOAD_ITERATIONS; ++i) {
        // Create new FireData object for each iteration
        FireData fireData;
        Timer timer;

        timer.start();
        fireData.loadFromDirectory(dataPath);
        timer.stop();

        double elapsed = timer.elapsed_ms();
        loadStats.addTiming(elapsed);
        // %zu is format specifier for size_t, i + 1 shows iteration number
        printf("Load %d: %.3f ms (%zu records)\n", i + 1, elapsed, fireData.size());
    }
    loadStats.printStatistics();

    // Load once for queries (reuse same data for all query tests)
    FireData fireData;
    fireData.loadFromDirectory(dataPath);
    printf("Loaded %zu records for query tests\n\n", fireData.size());

    // Run queries iteratively to get average query time
    BenchmarkStats pollutantStats("Pollutant Query");
    for (int i = 0; i < QUERY_ITERATIONS; ++i) {
        Timer timer;
        timer.start();
        // Query for PM2.5 (fine particulate matter) records using index lookup
        auto results = fireData.queryByPollutant("PM2.5");
        timer.stop();

        double elapsed = timer.elapsed_ms();
        pollutantStats.addTiming(elapsed);
        printf("Pollutant query %d: %.3f ms (%zu results)\n", i + 1, elapsed, results.size());
    }
    pollutantStats.printStatistics();

    // Run value range queries iteratively to get average query time
    BenchmarkStats valueStats("Value Range Query");
    for (int i = 0; i < QUERY_ITERATIONS; ++i) {
        Timer timer;
        timer.start();
        // Query records where value is between 5.0 and 15.0 (requires scanning all records)
        auto results = fireData.queryByValueRange(5.0, 15.0);
        timer.stop();

        double elapsed = timer.elapsed_ms();
        valueStats.addTiming(elapsed);
        printf("Value range query %d: %.3f ms (%zu results)\n", i + 1, elapsed, results.size());
    }
    valueStats.printStatistics();

    printf("========================================\n");
    printf("Benchmark Complete\n");
    printf("========================================\n\n");


    return 0;
}
