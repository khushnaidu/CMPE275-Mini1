#ifndef PARALLEL_STRATEGY_HPP
#define PARALLEL_STRATEGY_HPP

#include <queue>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <vector>
#include <functional>

enum class ParallelStrategy {
    SERIAL,  
    OPENMP,              
    CENTRALIZED_QUEUE, 
    ROUND_ROBIN  
};

inline const char* strategyToString(ParallelStrategy strategy) {
    switch (strategy) {
        case ParallelStrategy::SERIAL: return "Serial (No Threading)";
        case ParallelStrategy::OPENMP: return "OpenMP";
        case ParallelStrategy::CENTRALIZED_QUEUE: return "Leader-Worker (Centralized Queue)";
        case ParallelStrategy::ROUND_ROBIN: return "Leader-Worker (Round-Robin)";
        default: return "Unknown";
    }
}

// ============================================================================
// TaskQueue for centralized leader-worker pattern
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

    void push(const TaskType& task) {
        std::lock_guard<std::mutex> lock(mtx);
        tasks.push(task);
        cv.notify_one();
    }

    // worker tries to pop a task
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
        cv.notify_all();
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mtx);
        return tasks.size();
    }
};

// ============================================================================
// WorkerQueue for round-robin pattern (per-worker queues)
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

    void push(const TaskType& task) {
        std::lock_guard<std::mutex> lock(mtx);
        tasks.push(task);
        cv.notify_one();
    }

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

// get optimal thread count based on hardware
inline unsigned int getOptimalThreadCount() {
    unsigned int hwThreads = std::thread::hardware_concurrency();
    return hwThreads > 0 ? hwThreads : 4;
}

#endif 
