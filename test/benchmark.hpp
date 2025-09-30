// Benchmarking utility for measuring execution time of code blocks

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
    // Stores the time point when timer starts (high precision clock for accurate measurements)
    std::chrono::high_resolution_clock::time_point start_time;
    // Stores the time point when timer stops
    std::chrono::high_resolution_clock::time_point end_time;
    bool running;

public:
    // Constructor initializes timer in stopped state using initializer list
    Timer() : running(false) {}

    void start() {
        // Captures current time with high precision
        start_time = std::chrono::high_resolution_clock::now();
        running = true;
    }

    void stop() {
        // Captures current time to mark end of measurement
        end_time = std::chrono::high_resolution_clock::now();
        running = false;
    }

    // Returns elapsed time in milliseconds (const means this doesn't modify the object)
    double elapsed_ms() const {
        // Ternary operator: if still running use current time, otherwise use stored end_time
        auto end = running ? std::chrono::high_resolution_clock::now() : end_time;
        // Converts duration to milliseconds and extracts the numeric value with count()
        return std::chrono::duration<double, std::milli>(end - start_time).count();
    }
};

class BenchmarkStats {
private:
    // Dynamic array storing all timing measurements in milliseconds
    std::vector<double> timings;
    std::string name;

public:
    // Constructor takes const reference to avoid copying the string (more efficient)
    BenchmarkStats(const std::string& benchName) : name(benchName) {}

    void addTiming(double ms) {
        timings.push_back(ms);
    }

    void printStatistics() const {
        if (timings.empty()) {
            // .c_str() converts C++ string to C-style char* for printf
            printf("%s: No timings recorded.\n", name.c_str());
            return;
        }

        // std::accumulate sums all values in the range, starting from 0.0
        double sum = std::accumulate(timings.begin(), timings.end(), 0.0);
        double mean = sum / timings.size();

        // Creates a copy to sort (don't modify original timings vector)
        std::vector<double> sorted = timings;
        // Sorts in ascending order for median/min/max calculations
        std::sort(sorted.begin(), sorted.end());
        double median = sorted[sorted.size() / 2];
        // .front() gets first element, .back() gets last element
        double min = sorted.front();
        double max = sorted.back();

        // Calculate variance (average of squared differences from mean)
        double variance = 0.0;
        // Range-based for loop iterates through each timing value
        for (double t : timings) {
            variance += (t - mean) * (t - mean);
        }
        variance /= timings.size();
        // Standard deviation is square root of variance
        double stddev = std::sqrt(variance);

        printf("\n=== %s ===\n", name.c_str());
        // %zu is the format specifier for size_t type
        printf("Iterations: %zu\n", timings.size());
        // %.3f formats double with 3 decimal places
        printf("Mean:       %.3f ms\n", mean);
        printf("Median:     %.3f ms\n", median);
        printf("Min:        %.3f ms\n", min);
        printf("Max:        %.3f ms\n", max);
        printf("Std Dev:    %.3f ms\n", stddev);
        printf("================================\n\n");
    }
};

#endif