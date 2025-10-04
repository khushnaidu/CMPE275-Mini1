// implementation of firedata class with serial and parallel versions
// supports openmp, leader-worker centralized queue, and round-robin strategies

#include "firedata/fireData.hpp"
#include "common/csvParser.hpp"
#include "common/parallelStrategy.hpp"
#include <iostream>
#include <filesystem>
#include <mutex>
#include <thread>

// only include openmp if we compiled with it
#ifdef _OPENMP
#include <omp.h>
#endif

// namespace alias so we dont have to type std::filesystem every time
namespace fs = std::filesystem;

FireData::FireData() : recordCount(0) {}

FireData::~FireData() {
    clear();
}

// main load function, handles both single files and directories
void FireData::loadFromDirectory(const std::string& dirpath, ParallelStrategy strategy) {
    std::vector<std::string> csvFiles;

    // make filesystem path object to work with the path easier
    fs::path inputPath(dirpath);

    // check if its just a single file
    if (fs::is_regular_file(inputPath)) {
        // if its a csv file add it to our list
        std::string filename = inputPath.string();
        // get file extension by finding the last dot
        if (filename.substr(filename.find_last_of(".") + 1) == "csv") {
            csvFiles.push_back(filename);
        }
    }
    // otherwise its probably a directory
    else if (fs::is_directory(inputPath)) {
        // recursively go through all subdirectories to find csvs
        for (const auto& entry : fs::recursive_directory_iterator(dirpath)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().string();
                if (filename.substr(filename.find_last_of(".") + 1) == "csv") {
                    csvFiles.push_back(filename);
                }
            }
        }
    }

    printf("Found %zu CSV files to load using %s strategy...\n",
           csvFiles.size(), strategyToString(strategy));

    // pick which loading method based on strategy
    switch (strategy) {
        case ParallelStrategy::OPENMP:
            loadWithOpenMP(csvFiles);
            break;
        case ParallelStrategy::CENTRALIZED_QUEUE:
            loadWithCentralizedQueue(csvFiles);
            break;
        case ParallelStrategy::ROUND_ROBIN:
            loadWithRoundRobin(csvFiles);
            break;
    }

    recordCount = records.size();
    // build indexes now that all data is loaded, makes queries faster
    buildIndexes();
}

// ============================================================================
// strategy 1: openmp implementation
// ============================================================================
void FireData::loadWithOpenMP(const std::vector<std::string>& csvFiles) {
#ifdef _OPENMP
    // parallel file loading using openmp
    std::mutex recordsMutex;

    // openmp automatically splits loop iterations across threads
    #pragma omp parallel for
    for (size_t f = 0; f < csvFiles.size(); ++f) {
        auto data = CSVParser::readFile(csvFiles[f], false, ',');
        // each thread gets its own vector to avoid race conditions
        std::vector<FireRecord> localRecords;

        for (const auto& row : data) {
            // skip rows without enough columns, need at least 13
            if (row.size() < 13) continue;

            FireRecord record;
            // row[0] is first column, row[1] is second, etc.
            record.setLatitude(CSVParser::toDouble(row[0]));
            record.setLongitude(CSVParser::toDouble(row[1]));
            record.setUTC(row[2]);
            record.setPollutantType(row[3]);
            record.setConcentration(CSVParser::toDouble(row[4]));
            record.setUnit(row[5]);
            record.setRawConcentration(CSVParser::toDouble(row[6]));
            record.setAqi(CSVParser::toInt(row[7]));
            record.setCategory(CSVParser::toInt(row[8]));
            record.setSiteName(row[9]);
            record.setAgencyName(row[10]);
            record.setAqsId(row[11]);
            record.setFullAqsId(row[12]);

            localRecords.push_back(record);
        }

        // critical section so only one thread writes at a time
        #pragma omp critical
        {
            // merge local results into main vector
            records.insert(records.end(), localRecords.begin(), localRecords.end());
        }
    }
#else
    // serial version if openmp isnt available
    for (const auto& filename : csvFiles) {
        auto data = CSVParser::readFile(filename, false, ',');

        for (const auto& row : data) {
            if (row.size() < 13) continue;

            FireRecord record;
            record.setLatitude(CSVParser::toDouble(row[0]));
            record.setLongitude(CSVParser::toDouble(row[1]));
            record.setUTC(row[2]);
            record.setPollutantType(row[3]);
            record.setConcentration(CSVParser::toDouble(row[4]));
            record.setUnit(row[5]);
            record.setRawConcentration(CSVParser::toDouble(row[6]));
            record.setAqi(CSVParser::toInt(row[7]));
            record.setCategory(CSVParser::toInt(row[8]));
            record.setSiteName(row[9]);
            record.setAgencyName(row[10]);
            record.setAqsId(row[11]);
            record.setFullAqsId(row[12]);

            records.push_back(record);
        }
    }
#endif
}

// ============================================================================
// strategy 2: leader-worker with centralized queue
// ============================================================================
void FireData::loadWithCentralizedQueue(const std::vector<std::string>& csvFiles) {
    // one shared queue that all workers pull from
    TaskQueue<std::string> taskQueue;
    std::mutex recordsMutex;

    unsigned int numWorkers = getOptimalThreadCount();
    printf("Using %u worker threads with centralized queue\n", numWorkers);

    // worker function, each worker pulls from same queue
    auto workerFunc = [&](int workerId) {
        std::string filename;
        std::vector<FireRecord> localRecords;

        // keep getting tasks until queue is done
        while (taskQueue.pop(filename)) {
            auto data = CSVParser::readFile(filename, false, ',');

            for (const auto& row : data) {
                if (row.size() < 13) continue;

                FireRecord record;
                record.setLatitude(CSVParser::toDouble(row[0]));
                record.setLongitude(CSVParser::toDouble(row[1]));
                record.setUTC(row[2]);
                record.setPollutantType(row[3]);
                record.setConcentration(CSVParser::toDouble(row[4]));
                record.setUnit(row[5]);
                record.setRawConcentration(CSVParser::toDouble(row[6]));
                record.setAqi(CSVParser::toInt(row[7]));
                record.setCategory(CSVParser::toInt(row[8]));
                record.setSiteName(row[9]);
                record.setAgencyName(row[10]);
                record.setAqsId(row[11]);
                record.setFullAqsId(row[12]);

                localRecords.push_back(record);
            }
        }

        // done processing, merge results back
        std::lock_guard<std::mutex> lock(recordsMutex);
        records.insert(records.end(), localRecords.begin(), localRecords.end());
    };

    // leader creates all the worker threads
    std::vector<std::thread> workers;
    for (unsigned int i = 0; i < numWorkers; ++i) {
        workers.emplace_back(workerFunc, i);
    }

    // leader pushes all files to queue
    for (const auto& file : csvFiles) {
        taskQueue.push(file);
    }
    taskQueue.markFinished();

    // wait for workers to finish
    for (auto& worker : workers) {
        worker.join();
    }
}

// ============================================================================
// strategy 3: leader-worker with round-robin
// ============================================================================
void FireData::loadWithRoundRobin(const std::vector<std::string>& csvFiles) {
    unsigned int numWorkers = getOptimalThreadCount();
    printf("Using %u worker threads with round-robin distribution\n", numWorkers);

    // each worker gets their own queue so no contention
    std::vector<WorkerQueue<std::string>> workerQueues(numWorkers);
    std::mutex recordsMutex;

    // worker only reads from its own queue
    auto workerFunc = [&](int workerId) {
        std::string filename;
        std::vector<FireRecord> localRecords;

        // no contention since each worker has own queue
        while (workerQueues[workerId].pop(filename)) {
            auto data = CSVParser::readFile(filename, false, ',');

            for (const auto& row : data) {
                if (row.size() < 13) continue;

                FireRecord record;
                record.setLatitude(CSVParser::toDouble(row[0]));
                record.setLongitude(CSVParser::toDouble(row[1]));
                record.setUTC(row[2]);
                record.setPollutantType(row[3]);
                record.setConcentration(CSVParser::toDouble(row[4]));
                record.setUnit(row[5]);
                record.setRawConcentration(CSVParser::toDouble(row[6]));
                record.setAqi(CSVParser::toInt(row[7]));
                record.setCategory(CSVParser::toInt(row[8]));
                record.setSiteName(row[9]);
                record.setAgencyName(row[10]);
                record.setAqsId(row[11]);
                record.setFullAqsId(row[12]);

                localRecords.push_back(record);
            }
        }

        // merge results
        std::lock_guard<std::mutex> lock(recordsMutex);
        records.insert(records.end(), localRecords.begin(), localRecords.end());
    };

    // create worker threads
    std::vector<std::thread> workers;
    for (unsigned int i = 0; i < numWorkers; ++i) {
        workers.emplace_back(workerFunc, i);
    }

    // distribute files round robin style to each worker queue
    for (size_t i = 0; i < csvFiles.size(); ++i) {
        int targetWorker = i % numWorkers;  // goes 0,1,2...n-1,0,1,2...
        workerQueues[targetWorker].push(csvFiles[i]);
    }

    // tell all queues were done adding work
    for (auto& queue : workerQueues) {
        queue.markFinished();
    }

    // wait for all workers
    for (auto& worker : workers) {
        worker.join();
    }
}

void FireData::buildIndexes() {
    pollutantIndex.clear();

    #ifdef _OPENMP
        #pragma omp parallel for
        for (size_t i = 0; i < records.size(); ++i) {
            #pragma omp critical
            {
                // map pollutant type to index for fast lookup
                pollutantIndex.insert({records[i].getPollutantType(), i});
            }
        }
    #else
        for (size_t i = 0; i < records.size(); ++i) {
            pollutantIndex.insert({records[i].getPollutantType(), i});
        }
    #endif
}

std::vector<FireRecord> FireData::queryByPollutant(const std::string& pollutantType) const {
    std::vector<FireRecord> results;
    // equal_range gets all matching records from index
    auto range = pollutantIndex.equal_range(pollutantType);
    // iterate through matches
    for (auto it = range.first; it != range.second; ++it) {
        // it->second has the index
        results.push_back(records[it->second]);
    }
    return results;
}

// ============================================================================
// query by concentration range using different strategies
// ============================================================================
std::vector<FireRecord> FireData::queryByValueRange(
    double minValue, double maxValue, ParallelStrategy strategy) const {

    std::vector<FireRecord> results;

    switch (strategy) {
        case ParallelStrategy::OPENMP: {
#ifdef _OPENMP
            std::mutex resultsMutex;
            // parallelize search with openmp
            #pragma omp parallel for
            for (size_t i = 0; i < records.size(); ++i) {
                double concentration = records[i].getConcentration();
                if (concentration >= minValue && concentration <= maxValue) {
                    #pragma omp critical
                    {
                        results.push_back(records[i]);
                    }
                }
            }
#else
            // serial version
            for (const auto& record : records) {
                double concentration = record.getConcentration();
                if (concentration >= minValue && concentration <= maxValue) {
                    results.push_back(record);
                }
            }
#endif
            break;
        }

        case ParallelStrategy::CENTRALIZED_QUEUE: {
            // centralized queue approach, split records into chunks
            TaskQueue<std::pair<size_t, size_t>> taskQueue;  // <start, end>
            std::mutex resultsMutex;

            unsigned int numWorkers = getOptimalThreadCount();
            size_t chunkSize = records.size() / (numWorkers * 4);  // make more chunks for load balancing
            if (chunkSize == 0) chunkSize = 1;

            // Worker function
            auto workerFunc = [&]() {
                std::pair<size_t, size_t> chunk;
                std::vector<FireRecord> localResults;

                while (taskQueue.pop(chunk)) {
                    for (size_t i = chunk.first; i < chunk.second && i < records.size(); ++i) {
                        double concentration = records[i].getConcentration();
                        if (concentration >= minValue && concentration <= maxValue) {
                            localResults.push_back(records[i]);
                        }
                    }
                }

                // Merge local results
                std::lock_guard<std::mutex> lock(resultsMutex);
                results.insert(results.end(), localResults.begin(), localResults.end());
            };

            // Start workers
            std::vector<std::thread> workers;
            for (unsigned int i = 0; i < numWorkers; ++i) {
                workers.emplace_back(workerFunc);
            }

            // Push chunks to queue
            for (size_t start = 0; start < records.size(); start += chunkSize) {
                size_t end = std::min(start + chunkSize, records.size());
                taskQueue.push({start, end});
            }
            taskQueue.markFinished();

            // Wait for workers
            for (auto& worker : workers) {
                worker.join();
            }
            break;
        }

        case ParallelStrategy::ROUND_ROBIN: {
            // Round-robin: each worker gets its own subset
            unsigned int numWorkers = getOptimalThreadCount();
            std::vector<WorkerQueue<std::pair<size_t, size_t>>> workerQueues(numWorkers);
            std::mutex resultsMutex;

            size_t chunkSize = records.size() / (numWorkers * 4);
            if (chunkSize == 0) chunkSize = 1;

            // Worker function
            auto workerFunc = [&](int workerId) {
                std::pair<size_t, size_t> chunk;
                std::vector<FireRecord> localResults;

                while (workerQueues[workerId].pop(chunk)) {
                    for (size_t i = chunk.first; i < chunk.second && i < records.size(); ++i) {
                        double concentration = records[i].getConcentration();
                        if (concentration >= minValue && concentration <= maxValue) {
                            localResults.push_back(records[i]);
                        }
                    }
                }

                // Merge local results
                std::lock_guard<std::mutex> lock(resultsMutex);
                results.insert(results.end(), localResults.begin(), localResults.end());
            };

            // Start workers
            std::vector<std::thread> workers;
            for (unsigned int i = 0; i < numWorkers; ++i) {
                workers.emplace_back(workerFunc, i);
            }

            // Distribute chunks in round-robin
            size_t chunkIdx = 0;
            for (size_t start = 0; start < records.size(); start += chunkSize) {
                size_t end = std::min(start + chunkSize, records.size());
                int targetWorker = chunkIdx % numWorkers;
                workerQueues[targetWorker].push({start, end});
                chunkIdx++;
            }

            // Mark queues as finished
            for (auto& queue : workerQueues) {
                queue.markFinished();
            }

            // Wait for workers
            for (auto& worker : workers) {
                worker.join();
            }
            break;
        }
    }

    return results;
}

// ============================================================================
// query by geographic bounds using different strategies
// ============================================================================
std::vector<FireRecord> FireData::queryByGeographicBounds(
    double minLat, double maxLat, double minLon, double maxLon, ParallelStrategy strategy) const {

    std::vector<FireRecord> results;

    switch (strategy) {
        case ParallelStrategy::OPENMP: {
#ifdef _OPENMP
            std::mutex resultsMutex;
            #pragma omp parallel for
            for (size_t i = 0; i < records.size(); ++i) {
                double lat = records[i].getLatitude();
                double lon = records[i].getLongitude();
                if (lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon) {
                    #pragma omp critical
                    {
                        results.push_back(records[i]);
                    }
                }
            }
#else
            // Serial fallback
            for (const auto& record : records) {
                double lat = record.getLatitude();
                double lon = record.getLongitude();
                if (lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon) {
                    results.push_back(record);
                }
            }
#endif
            break;
        }

        case ParallelStrategy::CENTRALIZED_QUEUE: {
            TaskQueue<std::pair<size_t, size_t>> taskQueue;
            std::mutex resultsMutex;

            unsigned int numWorkers = getOptimalThreadCount();
            size_t chunkSize = records.size() / (numWorkers * 4);
            if (chunkSize == 0) chunkSize = 1;

            auto workerFunc = [&]() {
                std::pair<size_t, size_t> chunk;
                std::vector<FireRecord> localResults;

                while (taskQueue.pop(chunk)) {
                    for (size_t i = chunk.first; i < chunk.second && i < records.size(); ++i) {
                        double lat = records[i].getLatitude();
                        double lon = records[i].getLongitude();
                        if (lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon) {
                            localResults.push_back(records[i]);
                        }
                    }
                }

                std::lock_guard<std::mutex> lock(resultsMutex);
                results.insert(results.end(), localResults.begin(), localResults.end());
            };

            std::vector<std::thread> workers;
            for (unsigned int i = 0; i < numWorkers; ++i) {
                workers.emplace_back(workerFunc);
            }

            for (size_t start = 0; start < records.size(); start += chunkSize) {
                size_t end = std::min(start + chunkSize, records.size());
                taskQueue.push({start, end});
            }
            taskQueue.markFinished();

            for (auto& worker : workers) {
                worker.join();
            }
            break;
        }

        case ParallelStrategy::ROUND_ROBIN: {
            unsigned int numWorkers = getOptimalThreadCount();
            std::vector<WorkerQueue<std::pair<size_t, size_t>>> workerQueues(numWorkers);
            std::mutex resultsMutex;

            size_t chunkSize = records.size() / (numWorkers * 4);
            if (chunkSize == 0) chunkSize = 1;

            auto workerFunc = [&](int workerId) {
                std::pair<size_t, size_t> chunk;
                std::vector<FireRecord> localResults;

                while (workerQueues[workerId].pop(chunk)) {
                    for (size_t i = chunk.first; i < chunk.second && i < records.size(); ++i) {
                        double lat = records[i].getLatitude();
                        double lon = records[i].getLongitude();
                        if (lat >= minLat && lat <= maxLat && lon >= minLon && lon <= maxLon) {
                            localResults.push_back(records[i]);
                        }
                    }
                }

                std::lock_guard<std::mutex> lock(resultsMutex);
                results.insert(results.end(), localResults.begin(), localResults.end());
            };

            std::vector<std::thread> workers;
            for (unsigned int i = 0; i < numWorkers; ++i) {
                workers.emplace_back(workerFunc, i);
            }

            size_t chunkIdx = 0;
            for (size_t start = 0; start < records.size(); start += chunkSize) {
                size_t end = std::min(start + chunkSize, records.size());
                int targetWorker = chunkIdx % numWorkers;
                workerQueues[targetWorker].push({start, end});
                chunkIdx++;
            }

            for (auto& queue : workerQueues) {
                queue.markFinished();
            }

            for (auto& worker : workers) {
                worker.join();
            }
            break;
        }
    }

    return results;
}

// ============================================================================
// query by AQI category using different strategies
// ============================================================================
std::vector<FireRecord> FireData::queryByAQICategory(int category, ParallelStrategy strategy) const {

    std::vector<FireRecord> results;

    switch (strategy) {
        case ParallelStrategy::OPENMP: {
#ifdef _OPENMP
            std::mutex resultsMutex;
            #pragma omp parallel for
            for (size_t i = 0; i < records.size(); ++i) {
                if (records[i].getCategory() == category) {
                    #pragma omp critical
                    {
                        results.push_back(records[i]);
                    }
                }
            }
#else
            // Serial fallback
            for (const auto& record : records) {
                if (record.getCategory() == category) {
                    results.push_back(record);
                }
            }
#endif
            break;
        }

        case ParallelStrategy::CENTRALIZED_QUEUE: {
            TaskQueue<std::pair<size_t, size_t>> taskQueue;
            std::mutex resultsMutex;

            unsigned int numWorkers = getOptimalThreadCount();
            size_t chunkSize = records.size() / (numWorkers * 4);
            if (chunkSize == 0) chunkSize = 1;

            auto workerFunc = [&]() {
                std::pair<size_t, size_t> chunk;
                std::vector<FireRecord> localResults;

                while (taskQueue.pop(chunk)) {
                    for (size_t i = chunk.first; i < chunk.second && i < records.size(); ++i) {
                        if (records[i].getCategory() == category) {
                            localResults.push_back(records[i]);
                        }
                    }
                }

                std::lock_guard<std::mutex> lock(resultsMutex);
                results.insert(results.end(), localResults.begin(), localResults.end());
            };

            std::vector<std::thread> workers;
            for (unsigned int i = 0; i < numWorkers; ++i) {
                workers.emplace_back(workerFunc);
            }

            for (size_t start = 0; start < records.size(); start += chunkSize) {
                size_t end = std::min(start + chunkSize, records.size());
                taskQueue.push({start, end});
            }
            taskQueue.markFinished();

            for (auto& worker : workers) {
                worker.join();
            }
            break;
        }

        case ParallelStrategy::ROUND_ROBIN: {
            unsigned int numWorkers = getOptimalThreadCount();
            std::vector<WorkerQueue<std::pair<size_t, size_t>>> workerQueues(numWorkers);
            std::mutex resultsMutex;

            size_t chunkSize = records.size() / (numWorkers * 4);
            if (chunkSize == 0) chunkSize = 1;

            auto workerFunc = [&](int workerId) {
                std::pair<size_t, size_t> chunk;
                std::vector<FireRecord> localResults;

                while (workerQueues[workerId].pop(chunk)) {
                    for (size_t i = chunk.first; i < chunk.second && i < records.size(); ++i) {
                        if (records[i].getCategory() == category) {
                            localResults.push_back(records[i]);
                        }
                    }
                }

                std::lock_guard<std::mutex> lock(resultsMutex);
                results.insert(results.end(), localResults.begin(), localResults.end());
            };

            std::vector<std::thread> workers;
            for (unsigned int i = 0; i < numWorkers; ++i) {
                workers.emplace_back(workerFunc, i);
            }

            size_t chunkIdx = 0;
            for (size_t start = 0; start < records.size(); start += chunkSize) {
                size_t end = std::min(start + chunkSize, records.size());
                int targetWorker = chunkIdx % numWorkers;
                workerQueues[targetWorker].push({start, end});
                chunkIdx++;
            }

            for (auto& queue : workerQueues) {
                queue.markFinished();
            }

            for (auto& worker : workers) {
                worker.join();
            }
            break;
        }
    }

    return results;
}

// ============================================================================
// query by site name using different strategies
// ============================================================================
std::vector<FireRecord> FireData::queryBySiteName(
    const std::string& siteName, ParallelStrategy strategy) const {

    std::vector<FireRecord> results;

    switch (strategy) {
        case ParallelStrategy::OPENMP: {
#ifdef _OPENMP
            std::mutex resultsMutex;
            #pragma omp parallel for
            for (size_t i = 0; i < records.size(); ++i) {
                if (records[i].getSiteName() == siteName) {
                    #pragma omp critical
                    {
                        results.push_back(records[i]);
                    }
                }
            }
#else
            // Serial fallback
            for (const auto& record : records) {
                if (record.getSiteName() == siteName) {
                    results.push_back(record);
                }
            }
#endif
            break;
        }

        case ParallelStrategy::CENTRALIZED_QUEUE: {
            TaskQueue<std::pair<size_t, size_t>> taskQueue;
            std::mutex resultsMutex;

            unsigned int numWorkers = getOptimalThreadCount();
            size_t chunkSize = records.size() / (numWorkers * 4);
            if (chunkSize == 0) chunkSize = 1;

            auto workerFunc = [&]() {
                std::pair<size_t, size_t> chunk;
                std::vector<FireRecord> localResults;

                while (taskQueue.pop(chunk)) {
                    for (size_t i = chunk.first; i < chunk.second && i < records.size(); ++i) {
                        if (records[i].getSiteName() == siteName) {
                            localResults.push_back(records[i]);
                        }
                    }
                }

                std::lock_guard<std::mutex> lock(resultsMutex);
                results.insert(results.end(), localResults.begin(), localResults.end());
            };

            std::vector<std::thread> workers;
            for (unsigned int i = 0; i < numWorkers; ++i) {
                workers.emplace_back(workerFunc);
            }

            for (size_t start = 0; start < records.size(); start += chunkSize) {
                size_t end = std::min(start + chunkSize, records.size());
                taskQueue.push({start, end});
            }
            taskQueue.markFinished();

            for (auto& worker : workers) {
                worker.join();
            }
            break;
        }

        case ParallelStrategy::ROUND_ROBIN: {
            unsigned int numWorkers = getOptimalThreadCount();
            std::vector<WorkerQueue<std::pair<size_t, size_t>>> workerQueues(numWorkers);
            std::mutex resultsMutex;

            size_t chunkSize = records.size() / (numWorkers * 4);
            if (chunkSize == 0) chunkSize = 1;

            auto workerFunc = [&](int workerId) {
                std::pair<size_t, size_t> chunk;
                std::vector<FireRecord> localResults;

                while (workerQueues[workerId].pop(chunk)) {
                    for (size_t i = chunk.first; i < chunk.second && i < records.size(); ++i) {
                        if (records[i].getSiteName() == siteName) {
                            localResults.push_back(records[i]);
                        }
                    }
                }

                std::lock_guard<std::mutex> lock(resultsMutex);
                results.insert(results.end(), localResults.begin(), localResults.end());
            };

            std::vector<std::thread> workers;
            for (unsigned int i = 0; i < numWorkers; ++i) {
                workers.emplace_back(workerFunc, i);
            }

            size_t chunkIdx = 0;
            for (size_t start = 0; start < records.size(); start += chunkSize) {
                size_t end = std::min(start + chunkSize, records.size());
                int targetWorker = chunkIdx % numWorkers;
                workerQueues[targetWorker].push({start, end});
                chunkIdx++;
            }

            for (auto& queue : workerQueues) {
                queue.markFinished();
            }

            for (auto& worker : workers) {
                worker.join();
            }
            break;
        }
    }

    return results;
}

// ============================================================================
// aggregation: calculate average concentration using different strategies
// ============================================================================
double FireData::calculateAverageConcentrationByPollutant(
    const std::string& pollutantType, ParallelStrategy strategy) const {

    double sum = 0.0;
    size_t count = 0;

    switch (strategy) {
        case ParallelStrategy::OPENMP: {
#ifdef _OPENMP
            // parallel reduction - openmp automatically combines partial sums
            #pragma omp parallel for reduction(+:sum,count)
            for (size_t i = 0; i < records.size(); ++i) {
                if (records[i].getPollutantType() == pollutantType) {
                    sum += records[i].getConcentration();
                    count++;
                }
            }
#else
            for (const auto& record : records) {
                if (record.getPollutantType() == pollutantType) {
                    sum += record.getConcentration();
                    count++;
                }
            }
#endif
            break;
        }

        case ParallelStrategy::CENTRALIZED_QUEUE: {
            TaskQueue<std::pair<size_t, size_t>> taskQueue;
            std::mutex resultsMutex;

            unsigned int numWorkers = getOptimalThreadCount();
            size_t chunkSize = records.size() / (numWorkers * 4);
            if (chunkSize == 0) chunkSize = 1;

            auto workerFunc = [&]() {
                std::pair<size_t, size_t> chunk;
                double localSum = 0.0;
                size_t localCount = 0;

                while (taskQueue.pop(chunk)) {
                    for (size_t i = chunk.first; i < chunk.second && i < records.size(); ++i) {
                        if (records[i].getPollutantType() == pollutantType) {
                            localSum += records[i].getConcentration();
                            localCount++;
                        }
                    }
                }

                std::lock_guard<std::mutex> lock(resultsMutex);
                sum += localSum;
                count += localCount;
            };

            std::vector<std::thread> workers;
            for (unsigned int i = 0; i < numWorkers; ++i) {
                workers.emplace_back(workerFunc);
            }

            for (size_t start = 0; start < records.size(); start += chunkSize) {
                size_t end = std::min(start + chunkSize, records.size());
                taskQueue.push({start, end});
            }
            taskQueue.markFinished();

            for (auto& worker : workers) {
                worker.join();
            }
            break;
        }

        case ParallelStrategy::ROUND_ROBIN: {
            unsigned int numWorkers = getOptimalThreadCount();
            std::vector<WorkerQueue<std::pair<size_t, size_t>>> workerQueues(numWorkers);
            std::mutex resultsMutex;

            size_t chunkSize = records.size() / (numWorkers * 4);
            if (chunkSize == 0) chunkSize = 1;

            auto workerFunc = [&](int workerId) {
                std::pair<size_t, size_t> chunk;
                double localSum = 0.0;
                size_t localCount = 0;

                while (workerQueues[workerId].pop(chunk)) {
                    for (size_t i = chunk.first; i < chunk.second && i < records.size(); ++i) {
                        if (records[i].getPollutantType() == pollutantType) {
                            localSum += records[i].getConcentration();
                            localCount++;
                        }
                    }
                }

                std::lock_guard<std::mutex> lock(resultsMutex);
                sum += localSum;
                count += localCount;
            };

            std::vector<std::thread> workers;
            for (unsigned int i = 0; i < numWorkers; ++i) {
                workers.emplace_back(workerFunc, i);
            }

            size_t chunkIdx = 0;
            for (size_t start = 0; start < records.size(); start += chunkSize) {
                size_t end = std::min(start + chunkSize, records.size());
                int targetWorker = chunkIdx % numWorkers;
                workerQueues[targetWorker].push({start, end});
                chunkIdx++;
            }

            for (auto& queue : workerQueues) {
                queue.markFinished();
            }

            for (auto& worker : workers) {
                worker.join();
            }
            break;
        }
    }

    return count > 0 ? sum / count : 0.0;
}

// ============================================================================
// aggregation: count records by category using different strategies
// ============================================================================
std::map<int, size_t> FireData::countRecordsByCategory(ParallelStrategy strategy) const {

    std::map<int, size_t> categoryCounts;

    switch (strategy) {
        case ParallelStrategy::OPENMP: {
#ifdef _OPENMP
            // each thread maintains local counts, then merge
            #pragma omp parallel
            {
                std::map<int, size_t> localCounts;

                #pragma omp for nowait
                for (size_t i = 0; i < records.size(); ++i) {
                    int category = records[i].getCategory();
                    localCounts[category]++;
                }

                // merge local counts into global map
                #pragma omp critical
                {
                    for (const auto& pair : localCounts) {
                        categoryCounts[pair.first] += pair.second;
                    }
                }
            }
#else
            for (const auto& record : records) {
                categoryCounts[record.getCategory()]++;
            }
#endif
            break;
        }

        case ParallelStrategy::CENTRALIZED_QUEUE: {
            TaskQueue<std::pair<size_t, size_t>> taskQueue;
            std::mutex resultsMutex;

            unsigned int numWorkers = getOptimalThreadCount();
            size_t chunkSize = records.size() / (numWorkers * 4);
            if (chunkSize == 0) chunkSize = 1;

            auto workerFunc = [&]() {
                std::pair<size_t, size_t> chunk;
                std::map<int, size_t> localCounts;

                while (taskQueue.pop(chunk)) {
                    for (size_t i = chunk.first; i < chunk.second && i < records.size(); ++i) {
                        int category = records[i].getCategory();
                        localCounts[category]++;
                    }
                }

                std::lock_guard<std::mutex> lock(resultsMutex);
                for (const auto& pair : localCounts) {
                    categoryCounts[pair.first] += pair.second;
                }
            };

            std::vector<std::thread> workers;
            for (unsigned int i = 0; i < numWorkers; ++i) {
                workers.emplace_back(workerFunc);
            }

            for (size_t start = 0; start < records.size(); start += chunkSize) {
                size_t end = std::min(start + chunkSize, records.size());
                taskQueue.push({start, end});
            }
            taskQueue.markFinished();

            for (auto& worker : workers) {
                worker.join();
            }
            break;
        }

        case ParallelStrategy::ROUND_ROBIN: {
            unsigned int numWorkers = getOptimalThreadCount();
            std::vector<WorkerQueue<std::pair<size_t, size_t>>> workerQueues(numWorkers);
            std::mutex resultsMutex;

            size_t chunkSize = records.size() / (numWorkers * 4);
            if (chunkSize == 0) chunkSize = 1;

            auto workerFunc = [&](int workerId) {
                std::pair<size_t, size_t> chunk;
                std::map<int, size_t> localCounts;

                while (workerQueues[workerId].pop(chunk)) {
                    for (size_t i = chunk.first; i < chunk.second && i < records.size(); ++i) {
                        int category = records[i].getCategory();
                        localCounts[category]++;
                    }
                }

                std::lock_guard<std::mutex> lock(resultsMutex);
                for (const auto& pair : localCounts) {
                    categoryCounts[pair.first] += pair.second;
                }
            };

            std::vector<std::thread> workers;
            for (unsigned int i = 0; i < numWorkers; ++i) {
                workers.emplace_back(workerFunc, i);
            }

            size_t chunkIdx = 0;
            for (size_t start = 0; start < records.size(); start += chunkSize) {
                size_t end = std::min(start + chunkSize, records.size());
                int targetWorker = chunkIdx % numWorkers;
                workerQueues[targetWorker].push({start, end});
                chunkIdx++;
            }

            for (auto& queue : workerQueues) {
                queue.markFinished();
            }

            for (auto& worker : workers) {
                worker.join();
            }
            break;
        }
    }

    return categoryCounts;
}

void FireData::clear() {
    // free memory by clearing all containers
    records.clear();
    pollutantIndex.clear();
    recordCount = 0;
}
