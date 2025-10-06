#ifndef BENCHMARK_HPP
#define BENCHMARK_HPP

#include <chrono>
#include <iostream>
#include <vector>
#include <numeric>
#include <algorithm>
#include <cmath>

class Timer {
private:
    std::chrono::high_resolution_clock::time_point start_time;
    std::chrono::high_resolution_clock::time_point end_time;
    bool running;

public:
    Timer() : running(false) {}

    void start() {
        start_time = std::chrono::high_resolution_clock::now();
        running = true;
    }

    void stop() {
        end_time = std::chrono::high_resolution_clock::now();
        running = false;
    }

    double elapsed_ms() const {
        auto end = running ? std::chrono::high_resolution_clock::now() : end_time;
        return std::chrono::duration<double, std::milli>(end - start_time).count();
    }
};

class BenchmarkStats {
private:
    std::vector<double> timings;
    std::string name;

public:
    BenchmarkStats(const std::string& benchName) : name(benchName) {}

    void addTiming(double ms) {
        timings.push_back(ms);
    }

    void printStatistics() const {
        if (timings.empty()) {
            printf("%s: No timings recorded.\n", name.c_str());
            return;
        }

        double sum = std::accumulate(timings.begin(), timings.end(), 0.0);
        double mean = sum / timings.size();

        // sort to get median/min/max
        std::vector<double> sorted = timings;
        std::sort(sorted.begin(), sorted.end());
        double median = sorted[sorted.size() / 2];
        double min = sorted.front();
        double max = sorted.back();

        // calculate standard deviation
        double variance = 0.0;
        for (double t : timings) {
            variance += (t - mean) * (t - mean);
        }
        variance /= timings.size();
        double stddev = std::sqrt(variance);

        printf("\n=== %s ===\n", name.c_str());
        printf("Iterations: %zu\n", timings.size());
        printf("Mean:       %.3f ms\n", mean);
        printf("Median:     %.3f ms\n", median);
        printf("Min:        %.3f ms\n", min);
        printf("Max:        %.3f ms\n", max);
        printf("Std Dev:    %.3f ms\n", stddev);
        printf("================================\n\n");
    }
};

#endif
