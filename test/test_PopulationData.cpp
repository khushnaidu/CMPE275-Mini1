// population data benchmark test
// compares three parallelization strategies
// 1. openmp - data parallelism with pragma omp parallel for
// 2. leader-worker centralized queue - dynamic load balancing
// 3. leader-worker round robin - static task distribution


#include <cstdio>
#include <string>
#include "PopulationData/populationData.hpp"
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
    printf("Population Data Benchmark\n");
    printf("Comparing Parallelization Strategies\n");
    printf("========================================\n\n");

    // default path to data
    std::string dataPath = "/Users/khushnaidu/mini1/API_SP.POP.TOTL_DS2_en_csv_v2_3401680.csv";
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
            PopulationData populationData;
            Timer timer;

            timer.start();
            populationData.loadFromDirectory(dataPath, strategy);
            timer.stop();

            double elapsed = timer.elapsed_ms();
            loadStats.addTiming(elapsed);
            printf("Load %d: %.3f ms (%zu records)\n", i + 1, elapsed, populationData.size());
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
    PopulationData populationData;
    populationData.loadFromDirectory(dataPath, ParallelStrategy::OPENMP);
    printf("Loaded %zu records for query tests\n\n", populationData.size());

    // test each query with each strategy
    for (int s = 0; s < NUM_STRATEGIES; ++s) {
        ParallelStrategy strategy = STRATEGIES[s];
        
        printf("\n--- Strategy: %s ---\n\n", strategyToString(strategy));
        
        // year range query test
        BenchmarkStats yearRangeStats("Year Range Query (1960-2020)");
        for (int i = 0; i < QUERY_ITERATIONS; ++i) {
            Timer timer;
            timer.start();
            auto results = populationData.queryByYearRange(1960, 2020, strategy);
            timer.stop();

            double elapsed = timer.elapsed_ms();
            yearRangeStats.addTiming(elapsed);
            printf("Year range query %d: %.3f ms (%zu results)\n", i + 1, elapsed, results.size());
        }
        yearRangeStats.printStatistics();

        // population range query test
        BenchmarkStats rangeStats("Population Range Query (100M-1B in 2020)");
        for (int i = 0; i < QUERY_ITERATIONS; ++i) {
            Timer timer;
            timer.start();
            auto results = populationData.queryByPopulationRange(100000000, 1000000000, 2020, strategy);
            timer.stop();

            double elapsed = timer.elapsed_ms();
            rangeStats.addTiming(elapsed);
            printf("Population range query %d: %.3f ms (%zu results)\n", i + 1, elapsed, results.size());
        }
        rangeStats.printStatistics();
    }

    printf("========================================\n");
    printf("Benchmark Complete\n");
    printf("========================================\n\n");


    return 0;
}
