// Parallelization Strategy Definitions and Thread Utilities
#ifndef PARALLEL_STRATEGY_HPP
#define PARALLEL_STRATEGY_HPP

#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <vector>
#include <functional>

// Enum defining different parallelization strategies
enum class ParallelStrategy {
    OPENMP,              
    CENTRALIZED_QUEUE, 
    ROUND_ROBIN  
};

// Convert strategy enum to string for printing
inline const char* strategyToString(ParallelStrategy strategy) {
    switch (strategy) {
        case ParallelStrategy::OPENMP: return "OpenMP";
        case ParallelStrategy::CENTRALIZED_QUEUE: return "Leader-Worker (Centralized Queue)";
        case ParallelStrategy::ROUND_ROBIN: return "Leader-Worker (Round-Robin)";
        default: return "Unknown";
    }
}

// ============================================================================
// Task Queue for Centralized Leader-Worker Pattern
// ============================================================================
template<typename TaskType>
class TaskQueue {
private:
    std::queue<TaskType> tasks;
    mutable std::mutex mtx;
    std::condition_variable cv;
    bool finished;

public:
    TaskQueue() : finished(false) {}

    // Leader pushes tasks into the queue
    void push(const TaskType& task) {
        std::lock_guard<std::mutex> lock(mtx);
        tasks.push(task);
        cv.notify_one();  // Wake up one waiting worker
    }

    // Worker tries to pop a task 
    bool pop(TaskType& task) {
        std::unique_lock<std::mutex> lock(mtx);
        // wating for queue to be finished and not empty
        cv.wait(lock, [this]() { return !tasks.empty() || finished; });
        
        if (tasks.empty()) {
            return false;  // No more tasks and we're done
        }
        
        task = tasks.front();
        tasks.pop();
        return true;
    }

    // Leader signals that no more tasks will be added
    void markFinished() {
        std::lock_guard<std::mutex> lock(mtx);
        finished = true;
        cv.notify_all();  // Wake up all workers to exit
    }

    // Get current queue size (for monitoring)
    size_t size() const {
        std::lock_guard<std::mutex> lock(mtx);
        return tasks.size();
    }
};

// ============================================================================
// Per-Worker Queue for Round-Robin Leader-Worker Pattern
// ============================================================================
template<typename TaskType>
class WorkerQueue {
private:
    std::queue<TaskType> tasks;
    mutable std::mutex mtx;
    std::condition_variable cv;
    bool finished;

public:
    WorkerQueue() : finished(false) {}

    // Leader pushes task to this specific worker's queue
    void push(const TaskType& task) {
        std::lock_guard<std::mutex> lock(mtx);
        tasks.push(task);
        cv.notify_one();
    }

    // Worker pops from its own queue (no contention with other workers!)
    bool pop(TaskType& task) {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [this]() { return !tasks.empty() || finished; });
        
        if (tasks.empty()) {
            return false;
        }
        
        task = tasks.front();
        tasks.pop();
        return true;
    }

    void markFinished() {
        std::lock_guard<std::mutex> lock(mtx);
        finished = true;
        cv.notify_one();
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mtx);
        return tasks.size();
    }
};

// ============================================================================
// Helper function to get optimal thread count
// ============================================================================
inline unsigned int getOptimalThreadCount() {
    unsigned int hwThreads = std::thread::hardware_concurrency();
    return hwThreads > 0 ? hwThreads : 4;  // Default to 4 if detection fails
}

#endif 

