// Facade for managing fire data records
#ifndef FIRE_DATA_HPP
#define FIRE_DATA_HPP

#include <vector>
#include <string>
#include <map>
#include "firedata/fireRecord.hpp"

class FireData {
private:
    // Dynamic array storing all fire records
    std::vector<FireRecord> records;
    // Multimap allows multiple records with same key (pollutant type) -> maps to record index
    std::multimap<std::string, size_t> pollutantIndex;
    size_t recordCount;

    // Private helper to build search indexes for faster queries
    void buildIndexes();

public:
    // Constructor and destructor
    FireData();
    ~FireData();

    // Loads all CSV files from a directory (or single file) recursively
    void loadFromDirectory(const std::string& dirpath);
    // Query methods return new vectors containing matching records
    std::vector<FireRecord> queryByPollutant(const std::string& pollutantType) const;
    std::vector<FireRecord> queryByValueRange(double minValue, double maxValue) const;

    // Inline getter - function body in header for potential compiler optimization
    size_t size() const 
    { 
        return recordCount; 
    }
    void clear();
};

#endif