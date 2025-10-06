#ifndef POPULATION_DATA_HPP
#define POPULATION_DATA_HPP

#include <vector>
#include <string>
#include <map>
#include "PopulationData/populationRecord.hpp"
#include "common/parallelStrategy.hpp"

class PopulationData {
private:
    std::vector<PopulationRecord> records;
    std::multimap<std::string, size_t> countryIndex;  // maps country code to record index
    std::multimap<std::string, size_t> regionIndex;
    std::multimap<std::string, size_t> incomeGroupIndex;
    size_t recordCount;

    void buildIndexes();
    
    // different loading methods for each strategy
    void loadSerial(const std::vector<std::string>& csvFiles);
    void loadWithOpenMP(const std::vector<std::string>& csvFiles);
    void loadWithCentralizedQueue(const std::vector<std::string>& csvFiles);
    void loadWithRoundRobin(const std::vector<std::string>& csvFiles);

public:
    PopulationData();
    ~PopulationData();

    // main loading function
    void loadFromDirectory(const std::string& dirpath, 
                          ParallelStrategy strategy = ParallelStrategy::OPENMP);
    
    // query methods
    std::vector<PopulationRecord> queryByCountry(const std::string& countryCode) const;
    std::vector<PopulationRecord> queryByRegion(const std::string& region) const;
    std::vector<PopulationRecord> queryByIncomeGroup(const std::string& incomeGroup) const;
    
    std::vector<PopulationRecord> queryByPopulationRange(double minPopulation, double maxPopulation, 
                                                         int year = 2020,
                                                         ParallelStrategy strategy = ParallelStrategy::OPENMP) const;
    std::vector<PopulationRecord> queryByYearRange(int startYear, int endYear,
                                                    ParallelStrategy strategy = ParallelStrategy::OPENMP) const;

    size_t size() const { return recordCount; }
    void clear();
};

#endif
