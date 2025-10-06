// benchmark test for fire data
// compares serial vs parallel (OpenMP)

#include <cstdio>
#include <string>
#include "firedata/fireData.hpp"
#include "test/benchmark.hpp"
#include "utils.hpp"

const int LOAD_ITERATIONS = 3;
const int QUERY_ITERATIONS = 5;

const ParallelStrategy STRATEGIES[] = {
    ParallelStrategy::SERIAL,
    ParallelStrategy::OPENMP,
    ParallelStrategy::CENTRALIZED_QUEUE,
    ParallelStrategy::ROUND_ROBIN
};
const int NUM_STRATEGIES = 4;

int main(int argc, char** argv) {
    printf("\n========================================\n");
    printf("Fire Data Benchmark\n");
#ifdef _OPENMP
    printf("Mode: PARALLEL (OpenMP enabled, threads=%d)\n", numThreads());
#else
    printf("Mode: SERIAL (no OpenMP)\n");
#endif
    printf("========================================\n\n");

    std::string dataPath = "/Users/khushnaidu/Downloads/data";
    if (argc > 1) {
        dataPath = argv[1];
    }

    printf("Data path: %s\n\n", dataPath.c_str());

    // load benchmark
    BenchmarkStats loadStats("Load");
    for (int i = 0; i < LOAD_ITERATIONS; ++i) {
        FireData fireData;
        Timer timer;

        timer.start();
        fireData.loadFromDirectory(dataPath);
        timer.stop();

        double elapsed = timer.elapsed_ms();
        loadStats.addTiming(elapsed);
        printf("Load %d: %.3f ms (%zu records)\n", i + 1, elapsed, fireData.size());
    }
    loadStats.printStatistics();

    // query benchmarks
    FireData fireData;
    fireData.loadFromDirectory(dataPath);
    printf("Loaded %zu records for query tests\n\n", fireData.size());

    // pollutant query (uses index)
    BenchmarkStats pollutantStats("Pollutant Query (PM2.5)");
    for (int i = 0; i < QUERY_ITERATIONS; ++i) {
        Timer timer;
        timer.start();
        auto results = fireData.queryByPollutant("PM2.5");
        timer.stop();

        double elapsed = timer.elapsed_ms();
        pollutantStats.addTiming(elapsed);
        printf("Pollutant query %d: %.3f ms (%zu results)\n", i + 1, elapsed, results.size());
    }
    pollutantStats.printStatistics();

    // value range query (full scan)
    BenchmarkStats valueStats("Value Range Query (5.0-15.0)");
    for (int i = 0; i < QUERY_ITERATIONS; ++i) {
        Timer timer;
        timer.start();
        auto results = fireData.queryByValueRange(5.0, 15.0);
        timer.stop();

        double elapsed = timer.elapsed_ms();
        valueStats.addTiming(elapsed);
        printf("Value range query %d: %.3f ms (%zu results)\n", i + 1, elapsed, results.size());
    }
    valueStats.printStatistics();

    printf("\n========================================\n");
    printf("Benchmark Complete\n");
    printf("========================================\n\n");

    return 0;
}
