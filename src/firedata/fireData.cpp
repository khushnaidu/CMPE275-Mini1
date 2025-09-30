// Serial and parallel (OpenMP) implementation of FireData class

#include "firedata/fireData.hpp"
#include "common/csvParser.hpp"
#include <iostream>
#include <filesystem>
// Conditional compilation: only include OpenMP headers if _OPENMP is defined at compile time
#ifdef _OPENMP
#include <omp.h>
// Mutex needed for thread-safe access to shared data in parallel sections
#include <mutex>
#endif

// Namespace alias makes std::filesystem easier to use as just "fs"
namespace fs = std::filesystem;

FireData::FireData() : recordCount(0) {}


FireData::~FireData() 
{ 
    clear(); 
}

// Collect CSV files - handle both single file and directory paths
void FireData::loadFromDirectory(const std::string& dirpath) {

    std::vector<std::string> csvFiles;

    // Creates filesystem path object for easier path manipulation
    fs::path inputPath(dirpath);

    // Check if path is a regular file
    if (fs::is_regular_file(inputPath)) {
        // Single file - check if it's a CSV
        std::string filename = inputPath.string();
        // Extract file extension by finding last dot and taking substring after it
        if (filename.substr(filename.find_last_of(".") + 1) == "csv") {
            csvFiles.push_back(filename);
        }
    }
    // Check if path is a directory
    else if (fs::is_directory(inputPath)) {
        // recursive_directory_iterator walks through all subdirectories (not just top level)
        for (const auto& entry : fs::recursive_directory_iterator(dirpath)) {
            if (entry.is_regular_file()) {
                std::string filename = entry.path().string();
                if (filename.substr(filename.find_last_of(".") + 1) == "csv") {
                    csvFiles.push_back(filename);
                }
            }
        }
    }

    printf("Found %zu CSV files to load...\n", csvFiles.size());

#ifdef _OPENMP
    // PARALLEL file loading
    std::mutex recordsMutex;

    // OpenMP pragma distributes loop iterations across multiple threads
    #pragma omp parallel for
    for (size_t f = 0; f < csvFiles.size(); ++f) {
        auto data = CSVParser::readFile(csvFiles[f], false, ',');
        // Each thread has its own local vector to avoid race conditions
        std::vector<FireRecord> localRecords;

        for (const auto& row : data) {
            // Skip rows that don't have all 13 expected columns
            if (row.size() < 13) continue;

            FireRecord record;
            // Array indexing: row[0] is first column, row[1] is second, etc.
            record.setLatitude(CSVParser::toDouble(row[0]));
            record.setLongitude(CSVParser::toDouble(row[1]));
            record.setTimestamp(row[2]);
            record.setPollutantType(row[3]);
            record.setValue(CSVParser::toDouble(row[4]));
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

        // Critical section: only one thread can execute this block at a time
        #pragma omp critical
        {
            // Insert all local records into shared records vector
            records.insert(records.end(), localRecords.begin(), localRecords.end());
        }
    }
#else
    // SERIAL file loading - only when OpenMP is not available
    for (const auto& filename : csvFiles) {
        auto data = CSVParser::readFile(filename, false, ',');

        for (const auto& row : data) {
            if (row.size() < 13) continue;

            FireRecord record;
            record.setLatitude(CSVParser::toDouble(row[0]));
            record.setLongitude(CSVParser::toDouble(row[1]));
            record.setTimestamp(row[2]);
            record.setPollutantType(row[3]);
            record.setValue(CSVParser::toDouble(row[4]));
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

    recordCount = records.size();
    // Build indexes after loading all data for faster queries
    buildIndexes();
}

void FireData::buildIndexes() {
    pollutantIndex.clear();

    #ifdef _OPENMP
        #pragma omp parallel for
        for (size_t i = 0; i < records.size(); ++i) {
            #pragma omp critical
            {
                // Maps pollutant type string to record index for O(log n) lookup
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
    // equal_range returns pair of iterators: [first matching, one past last matching]
    auto range = pollutantIndex.equal_range(pollutantType);
    // Iterate through all records matching the pollutant type
    for (auto it = range.first; it != range.second; ++it) {
        // it->second is the record index stored in the multimap
        results.push_back(records[it->second]);
    }
    return results;
}

std::vector<FireRecord> FireData::queryByValueRange(double minValue, double maxValue) const {
    std::vector<FireRecord> results;

#ifdef _OPENMP
    std::mutex resultsMutex;

    // Parallelize search across all records
    #pragma omp parallel for
    for (size_t i = 0; i < records.size(); ++i) {
        double value = records[i].getValue();
        // Check if value falls within the specified range (inclusive)
        if (value >= minValue && value <= maxValue) {
            #pragma omp critical
            {
                results.push_back(records[i]);
            }
        }
    }
#else
    // Serial version scans through all records sequentially
    for (const auto& record : records) {
        double value = record.getValue();
        if (value >= minValue && value <= maxValue) {
            results.push_back(record);
        }
    }
#endif

    return results;
}

void FireData::clear() {
    // Free memory by clearing all containers
    records.clear();
    pollutantIndex.clear();
    recordCount = 0;
}