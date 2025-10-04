// fire data benchmark test
// compares three parallelization strategies
// 1. openmp - data parallelism with pragma omp parallel for
// 2. leader-worker centralized queue - dynamic load balancing
// 3. leader-worker round robin - static task distribution


#include <cstdio>
#include <string>
#include "firedata/fireData.hpp"
#include "common/parallelStrategy.hpp"
#include "test/benchmark.hpp"
#include "utils.hpp"

// number of iterations for averaging
const int LOAD_ITERATIONS = 3;
const int QUERY_ITERATIONS = 5;

// test all three strategies
const ParallelStrategy STRATEGIES[] = {
    ParallelStrategy::OPENMP,
    ParallelStrategy::CENTRALIZED_QUEUE,
    ParallelStrategy::ROUND_ROBIN
};
const int NUM_STRATEGIES = 3;


int main(int argc, char** argv) {
    printf("\n========================================\n");
    printf("Fire Data Benchmark\n");
    printf("Comparing Parallelization Strategies\n");
    printf("========================================\n\n");

    // default path to data
    std::string dataPath = "../datasets/2020-fire/data";
    if (argc > 1) {
        dataPath = argv[1];
    }

    printf("Data path: %s\n\n", dataPath.c_str());

    // ========================================================================
    // benchmark loading with each strategy
    // ========================================================================
    for (int s = 0; s < NUM_STRATEGIES; ++s) {
        ParallelStrategy strategy = STRATEGIES[s];

        printf("\n========================================\n");
        printf("Strategy: %s\n", strategyToString(strategy));
        printf("========================================\n\n");

        // benchmark load times
        BenchmarkStats loadStats("Load");
        for (int i = 0; i < LOAD_ITERATIONS; ++i) {
            // new object each iteration
            FireData fireData;
            Timer timer;

            timer.start();
            fireData.loadFromDirectory(dataPath, strategy);
            timer.stop();

            double elapsed = timer.elapsed_ms();
            loadStats.addTiming(elapsed);
            printf("Load %d: %.3f ms (%zu records)\n", i + 1, elapsed, fireData.size());
        }
        loadStats.printStatistics();
    }

    // ========================================================================
    // query benchmarks - compare all strategies
    // ========================================================================
    printf("\n========================================\n");
    printf("Query Performance Tests\n");
    printf("========================================\n\n");

    // load once for all query tests
    FireData fireData;
    fireData.loadFromDirectory(dataPath, ParallelStrategy::OPENMP);
    printf("Loaded %zu records for query tests\n\n", fireData.size());

    // test each query with each strategy
    for (int s = 0; s < NUM_STRATEGIES; ++s) {
        ParallelStrategy strategy = STRATEGIES[s];

        printf("\n--- Strategy: %s ---\n\n", strategyToString(strategy));

        // pollutant query test
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

        // value range query test
        BenchmarkStats rangeStats("Value Range Query (5.0-15.0)");
        for (int i = 0; i < QUERY_ITERATIONS; ++i) {
            Timer timer;
            timer.start();
            auto results = fireData.queryByValueRange(5.0, 15.0, strategy);
            timer.stop();

            double elapsed = timer.elapsed_ms();
            rangeStats.addTiming(elapsed);
            printf("Value range query %d: %.3f ms (%zu results)\n", i + 1, elapsed, results.size());
        }
        rangeStats.printStatistics();
    }

    printf("========================================\n");
    printf("Benchmark Complete\n");
    printf("========================================\n\n");


    return 0;
}
