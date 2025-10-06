#include "PopulationData/populationData.hpp"
#include "common/csvParser.hpp"
#include "common/parallelStrategy.hpp"
#include <iostream>
#include <filesystem>
#include <mutex>
#include <thread>

#ifdef _OPENMP
#include <omp.h>
#endif

namespace fs = std::filesystem;

PopulationData::PopulationData() : recordCount(0) {}

PopulationData::~PopulationData() { 
    clear(); 
}

void PopulationData::loadFromDirectory(const std::string& dirpath, ParallelStrategy strategy) {
    std::vector<std::string> csvFiles;
    fs::path inputPath(dirpath);

    // check if single file or directory
    if (fs::is_regular_file(inputPath)) {
        std::string filename = inputPath.string();
        if (filename.substr(filename.find_last_of(".") + 1) == "csv") {
            csvFiles.push_back(filename);
        }
    }
    else if (fs::is_directory(inputPath)) {
        // recursively find all csv files
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

    switch (strategy) {
        case ParallelStrategy::SERIAL:
            loadSerial(csvFiles);
            break;
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
    buildIndexes();
}

// ============================================================================
// SERIAL (baseline - no parallelization)
// ============================================================================
void PopulationData::loadSerial(const std::vector<std::string>& csvFiles) {
    for (const auto& filename : csvFiles) {
        if (filename.find("Metadata_") != std::string::npos) {
            continue;
        }
        
        auto data = CSVParser::readFile(filename, false, ',');

        for (const auto& row : data) {
            if (row.size() < 4) continue;
            
            // skip header rows
            if (row[0] == "Data Source" || row[0] == "Country Name" || row[0].empty()) {
                continue;
            }

            PopulationRecord record;
            record.setCountryName(row[0]);
            record.setCountryCode(row[1]);
            record.setIndicatorName(row[2]);
            record.setIndicatorCode(row[3]);

            // columns 4-67 are yearly values from 1960-2023
            std::vector<double> yearlyValues;
            for (size_t i = 4; i < row.size() && i < 68; ++i) {
                double value = CSVParser::toDouble(row[i]);
                yearlyValues.push_back(value);
            }
            record.setYearlyValues(yearlyValues);

            records.push_back(record);
        }
    }
}

// ============================================================================
// OPENMP (data parallelism with #pragma omp)
// ============================================================================
void PopulationData::loadWithOpenMP(const std::vector<std::string>& csvFiles) {
#ifdef _OPENMP
    std::mutex recordsMutex;

    #pragma omp parallel for
    for (size_t f = 0; f < csvFiles.size(); ++f) {
        if (csvFiles[f].find("Metadata_") != std::string::npos) {
            continue;
        }
        
        auto data = CSVParser::readFile(csvFiles[f], false, ',');
        std::vector<PopulationRecord> localRecords;  // each thread gets its own vector

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

        // merge local results into main vector
        #pragma omp critical
        {
            records.insert(records.end(), localRecords.begin(), localRecords.end());
        }
    }
#else
    // fallback to serial if openmp not available
    for (const auto& filename : csvFiles) {
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

            records.push_back(record);
        }
    }
#endif
}

// ============================================================================
// CENTRALIZED QUEUE (leader-worker with shared queue)
// ============================================================================
void PopulationData::loadWithCentralizedQueue(const std::vector<std::string>& csvFiles) {
    TaskQueue<std::string> taskQueue;
    std::mutex recordsMutex;
    
    unsigned int numWorkers = getOptimalThreadCount();
    printf("Using %u worker threads with centralized queue\n", numWorkers);
    
    auto workerFunc = [&](int workerId) {
        std::string filename;
        std::vector<PopulationRecord> localRecords;
        
        while (taskQueue.pop(filename)) {
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
        
        // merge results back
        std::lock_guard<std::mutex> lock(recordsMutex);
        records.insert(records.end(), localRecords.begin(), localRecords.end());
    };
    
    std::vector<std::thread> workers;
    for (unsigned int i = 0; i < numWorkers; ++i) {
        workers.emplace_back(workerFunc, i);
    }
    
    // leader pushes all files to queue
    for (const auto& file : csvFiles) {
        taskQueue.push(file);
    }
    taskQueue.markFinished();
    
    for (auto& worker : workers) {
        worker.join();
    }
}

// ============================================================================
// ROUND-ROBIN (leader-worker with per-worker queues)
// ============================================================================
void PopulationData::loadWithRoundRobin(const std::vector<std::string>& csvFiles) {
    unsigned int numWorkers = getOptimalThreadCount();
    printf("Using %u worker threads with round-robin distribution\n", numWorkers);
    
    std::vector<WorkerQueue<std::string>> workerQueues(numWorkers);
    std::mutex recordsMutex;
    
    auto workerFunc = [&](int workerId) {
        std::string filename;
        std::vector<PopulationRecord> localRecords;
        
        // each worker only reads from its own queue
        while (workerQueues[workerId].pop(filename)) {
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
        
        std::lock_guard<std::mutex> lock(recordsMutex);
        records.insert(records.end(), localRecords.begin(), localRecords.end());
    };
    
    std::vector<std::thread> workers;
    for (unsigned int i = 0; i < numWorkers; ++i) {
        workers.emplace_back(workerFunc, i);
    }
    
    // distribute files round-robin style
    for (size_t i = 0; i < csvFiles.size(); ++i) {
        int targetWorker = i % numWorkers;  
        workerQueues[targetWorker].push(csvFiles[i]);
    }
    
    for (auto& queue : workerQueues) {
        queue.markFinished();
    }
    
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
                countryIndex.insert({records[i].getCountryCode(), i});
                regionIndex.insert({records[i].getRegion(), i});
                incomeGroupIndex.insert({records[i].getIncomeGroup(), i});
            }
        }
    #else
        for (size_t i = 0; i < records.size(); ++i) {
            countryIndex.insert({records[i].getCountryCode(), i});
            regionIndex.insert({records[i].getRegion(), i});
            incomeGroupIndex.insert({records[i].getIncomeGroup(), i});
        }
    #endif
}

std::vector<PopulationRecord> PopulationData::queryByCountry(const std::string& countryCode) const {
    std::vector<PopulationRecord> results;
    auto range = countryIndex.equal_range(countryCode);
    for (auto it = range.first; it != range.second; ++it) {
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

// query by population range 
std::vector<PopulationRecord> PopulationData::queryByPopulationRange(
    double minPopulation, double maxPopulation, int year, ParallelStrategy strategy) const {
    
    std::vector<PopulationRecord> results;
    
    switch (strategy) {
        case ParallelStrategy::SERIAL: {
            for (const auto& record : records) {
                double population = record.getPopulationForYear(year);
                if (population >= minPopulation && population <= maxPopulation) {
                    results.push_back(record);
                }
            }
            break;
        }

        case ParallelStrategy::OPENMP: {
#ifdef _OPENMP
            std::mutex resultsMutex;
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
            TaskQueue<std::pair<size_t, size_t>> taskQueue; 
            std::mutex resultsMutex;
            
            unsigned int numWorkers = getOptimalThreadCount();
            size_t chunkSize = records.size() / (numWorkers * 4);  // create more chunks for better load balancing
            if (chunkSize == 0) chunkSize = 1;
            
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
                        double population = records[i].getPopulationForYear(year);
                        if (population >= minPopulation && population <= maxPopulation) {
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

// query by year range
std::vector<PopulationRecord> PopulationData::queryByYearRange(
    int startYear, int endYear, ParallelStrategy strategy) const {
    
    std::vector<PopulationRecord> results;
    
    switch (strategy) {
        case ParallelStrategy::SERIAL: {
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
            break;
        }

        case ParallelStrategy::OPENMP: {
#ifdef _OPENMP
            std::mutex resultsMutex;
            #pragma omp parallel for
            for (size_t i = 0; i < records.size(); ++i) {
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
    records.clear();
    countryIndex.clear();
    regionIndex.clear();
    incomeGroupIndex.clear();
    recordCount = 0;
}
