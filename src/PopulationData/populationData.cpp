// implementation of populationdata class with serial and parallel versions
// supports openmp, leader-worker centralized queue, and round-robin strategies

#include "PopulationData/populationData.hpp"
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

PopulationData::PopulationData() : recordCount(0) {}

PopulationData::~PopulationData() { 
    clear(); 
}

// main load function, handles both single files and directories
void PopulationData::loadFromDirectory(const std::string& dirpath, ParallelStrategy strategy) {
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
void PopulationData::loadWithOpenMP(const std::vector<std::string>& csvFiles) {
#ifdef _OPENMP
    // parallel file loading using openmp
    std::mutex recordsMutex;

    // openmp automatically splits loop iterations across threads
    #pragma omp parallel for
    for (size_t f = 0; f < csvFiles.size(); ++f) {
        // skip metadata files, we only want the actual data
        if (csvFiles[f].find("Metadata_") != std::string::npos) {
            continue;
        }
        
        auto data = CSVParser::readFile(csvFiles[f], false, ',');
        // each thread gets its own vector to avoid race conditions
        std::vector<PopulationRecord> localRecords;

        for (const auto& row : data) {
            // skip rows without enough columns, need at least 4
            if (row.size() < 4) continue;
            
            // skip header and empty rows
            if (row[0] == "Data Source" || row[0] == "Country Name" || row[0].empty()) {
                continue;
            }

            PopulationRecord record;
            
            // set the basic info from first 4 columns
            record.setCountryName(row[0]);
            record.setCountryCode(row[1]);
            record.setIndicatorName(row[2]);
            record.setIndicatorCode(row[3]);

            // parse the yearly values starting at column 4, goes from 1960-2023
            std::vector<double> yearlyValues;
            for (size_t i = 4; i < row.size() && i < 68; ++i) { // 64 years total
                double value = CSVParser::toDouble(row[i]);
                yearlyValues.push_back(value);
            }
            record.setYearlyValues(yearlyValues);

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
        // skip metadata files
        if (filename.find("Metadata_") != std::string::npos) {
            continue;
        }
        
        auto data = CSVParser::readFile(filename, false, ',');

        for (const auto& row : data) {
            // need at least 4 columns
            if (row.size() < 4) continue;
            
            // skip headers and empty rows
            if (row[0] == "Data Source" || row[0] == "Country Name" || row[0].empty()) {
                continue;
            }

            PopulationRecord record;
            
            // basic info
            record.setCountryName(row[0]);
            record.setCountryCode(row[1]);
            record.setIndicatorName(row[2]);
            record.setIndicatorCode(row[3]);

            // yearly values 1960-2023
            std::vector<double> yearlyValues;
            for (size_t i = 4; i < row.size() && i < 68; ++i) {
                double value = CSVParser::toDouble(row[i]);
                yearlyValues.push_back(value);
            }
            record.setYearlyValues(yearlyValues);

            records.push_back(record);
        }
    }
#endif
}

// ============================================================================
// strategy 2: leader-worker with centralized queue
// ============================================================================
void PopulationData::loadWithCentralizedQueue(const std::vector<std::string>& csvFiles) {
    // one shared queue that all workers pull from
    TaskQueue<std::string> taskQueue;
    std::mutex recordsMutex;
    
    unsigned int numWorkers = getOptimalThreadCount();
    printf("Using %u worker threads with centralized queue\n", numWorkers);
    
    // worker function, each worker pulls from same queue
    auto workerFunc = [&](int workerId) {
        std::string filename;
        std::vector<PopulationRecord> localRecords;
        
        // keep getting tasks until queue is done
        while (taskQueue.pop(filename)) {
            // skip metadata files
            if (filename.find("Metadata_") != std::string::npos) {
                continue;
            }
            
            auto data = CSVParser::readFile(filename, false, ',');
            
            for (const auto& row : data) {
                if (row.size() < 4) continue;
                if (row[0] == "Data Source" || row[0] == "Country Name" || row[0].empty()) {
                    continue;
                }

                PopulationRecord record;
                record.setCountryName(row[0]);
                record.setCountryCode(row[1]);
                record.setIndicatorName(row[2]);
                record.setIndicatorCode(row[3]);

                std::vector<double> yearlyValues;
                for (size_t i = 4; i < row.size() && i < 68; ++i) {
                    double value = CSVParser::toDouble(row[i]);
                    yearlyValues.push_back(value);
                }
                record.setYearlyValues(yearlyValues);
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
void PopulationData::loadWithRoundRobin(const std::vector<std::string>& csvFiles) {
    unsigned int numWorkers = getOptimalThreadCount();
    printf("Using %u worker threads with round-robin distribution\n", numWorkers);
    
    // each worker gets their own queue so no contention
    std::vector<WorkerQueue<std::string>> workerQueues(numWorkers);
    std::mutex recordsMutex;
    
    // worker only reads from its own queue
    auto workerFunc = [&](int workerId) {
        std::string filename;
        std::vector<PopulationRecord> localRecords;
        
        // no contention since each worker has own queue
        while (workerQueues[workerId].pop(filename)) {
            // skip metadata
            if (filename.find("Metadata_") != std::string::npos) {
                continue;
            }
            
            auto data = CSVParser::readFile(filename, false, ',');
            
            for (const auto& row : data) {
                if (row.size() < 4) continue;
                if (row[0] == "Data Source" || row[0] == "Country Name" || row[0].empty()) {
                    continue;
                }

                PopulationRecord record;
                record.setCountryName(row[0]);
                record.setCountryCode(row[1]);
                record.setIndicatorName(row[2]);
                record.setIndicatorCode(row[3]);

                std::vector<double> yearlyValues;
                for (size_t i = 4; i < row.size() && i < 68; ++i) {
                    double value = CSVParser::toDouble(row[i]);
                    yearlyValues.push_back(value);
                }
                record.setYearlyValues(yearlyValues);
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

void PopulationData::buildIndexes() {
    countryIndex.clear();
    regionIndex.clear();
    incomeGroupIndex.clear();

    #ifdef _OPENMP
        #pragma omp parallel for
        for (size_t i = 0; i < records.size(); ++i) {
            #pragma omp critical
            {
                // map country code to index for fast lookup
                countryIndex.insert({records[i].getCountryCode(), i});
                // region and income indexes
                regionIndex.insert({records[i].getRegion(), i});
                incomeGroupIndex.insert({records[i].getIncomeGroup(), i});
            }
        }
    #else
        for (size_t i = 0; i < records.size(); ++i) {
            // build all the indexes
            countryIndex.insert({records[i].getCountryCode(), i});
            regionIndex.insert({records[i].getRegion(), i});
            incomeGroupIndex.insert({records[i].getIncomeGroup(), i});
        }
    #endif
}

std::vector<PopulationRecord> PopulationData::queryByCountry(const std::string& countryCode) const {
    std::vector<PopulationRecord> results;
    // equal_range gets all matching records from index
    auto range = countryIndex.equal_range(countryCode);
    // iterate through matches
    for (auto it = range.first; it != range.second; ++it) {
        // it->second has the index
        results.push_back(records[it->second]);
    }
    return results;
}

std::vector<PopulationRecord> PopulationData::queryByRegion(const std::string& region) const {
    std::vector<PopulationRecord> results;
    auto range = regionIndex.equal_range(region);
    for (auto it = range.first; it != range.second; ++it) {
        results.push_back(records[it->second]);
    }
    return results;
}

std::vector<PopulationRecord> PopulationData::queryByIncomeGroup(const std::string& incomeGroup) const {
    std::vector<PopulationRecord> results;
    auto range = incomeGroupIndex.equal_range(incomeGroup);
    for (auto it = range.first; it != range.second; ++it) {
        results.push_back(records[it->second]);
    }
    return results;
}

// ============================================================================
// query by population range using different strategies
// ============================================================================
std::vector<PopulationRecord> PopulationData::queryByPopulationRange(
    double minPopulation, double maxPopulation, int year, ParallelStrategy strategy) const {
    
    std::vector<PopulationRecord> results;
    
    switch (strategy) {
        case ParallelStrategy::OPENMP: {
#ifdef _OPENMP
            std::mutex resultsMutex;
            // parallelize search with openmp
            #pragma omp parallel for
            for (size_t i = 0; i < records.size(); ++i) {
                double population = records[i].getPopulationForYear(year);
                if (population >= minPopulation && population <= maxPopulation) {
                    #pragma omp critical
                    {
                        results.push_back(records[i]);
                    }
                }
            }
#else
            // serial version
            for (const auto& record : records) {
                double population = record.getPopulationForYear(year);
                if (population >= minPopulation && population <= maxPopulation) {
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
                std::vector<PopulationRecord> localResults;
                
                while (taskQueue.pop(chunk)) {
                    for (size_t i = chunk.first; i < chunk.second && i < records.size(); ++i) {
                        double population = records[i].getPopulationForYear(year);
                        if (population >= minPopulation && population <= maxPopulation) {
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
                std::vector<PopulationRecord> localResults;
                
                while (workerQueues[workerId].pop(chunk)) {
                    for (size_t i = chunk.first; i < chunk.second && i < records.size(); ++i) {
                        double population = records[i].getPopulationForYear(year);
                        if (population >= minPopulation && population <= maxPopulation) {
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
// Query: Year Range with Multiple Strategies
// ============================================================================
std::vector<PopulationRecord> PopulationData::queryByYearRange(
    int startYear, int endYear, ParallelStrategy strategy) const {
    
    std::vector<PopulationRecord> results;
    
    switch (strategy) {
        case ParallelStrategy::OPENMP: {
#ifdef _OPENMP
            std::mutex resultsMutex;
            #pragma omp parallel for
            for (size_t i = 0; i < records.size(); ++i) {
                // Check if record has data for the specified year range
                bool hasData = false;
                for (int year = startYear; year <= endYear; year++) {
                    if (records[i].getPopulationForYear(year) > 0) {
                        hasData = true;
                        break;
                    }
                }
                if (hasData) {
                    #pragma omp critical
                    {
                        results.push_back(records[i]);
                    }
                }
            }
#else
            // Serial fallback
            for (const auto& record : records) {
                bool hasData = false;
                for (int year = startYear; year <= endYear; year++) {
                    if (record.getPopulationForYear(year) > 0) {
                        hasData = true;
                        break;
                    }
                }
                if (hasData) {
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
                std::vector<PopulationRecord> localResults;
                
                while (taskQueue.pop(chunk)) {
                    for (size_t i = chunk.first; i < chunk.second && i < records.size(); ++i) {
                        bool hasData = false;
                        for (int year = startYear; year <= endYear; year++) {
                            if (records[i].getPopulationForYear(year) > 0) {
                                hasData = true;
                                break;
                            }
                        }
                        if (hasData) {
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
                std::vector<PopulationRecord> localResults;
                
                while (workerQueues[workerId].pop(chunk)) {
                    for (size_t i = chunk.first; i < chunk.second && i < records.size(); ++i) {
                        bool hasData = false;
                        for (int year = startYear; year <= endYear; year++) {
                            if (records[i].getPopulationForYear(year) > 0) {
                                hasData = true;
                                break;
                            }
                        }
                        if (hasData) {
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

void PopulationData::clear() {
    // Free memory by clearing all containers
    records.clear();
    countryIndex.clear();
    regionIndex.clear();
    incomeGroupIndex.clear();
    recordCount = 0;
}