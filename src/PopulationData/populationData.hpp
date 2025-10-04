// facade class for managing population data records
#ifndef POPULATION_DATA_HPP
#define POPULATION_DATA_HPP

#include <vector>
#include <string>
#include <map>
#include "PopulationData/populationRecord.hpp"
#include "common/parallelStrategy.hpp"

class PopulationData {
private:
    // vector storing all the population records we loaded
    std::vector<PopulationRecord> records;
    // multimap lets us have multiple records with same key, maps country code to record index for fast lookup
    std::multimap<std::string, size_t> countryIndex;
    // multimap for doing region queries
    std::multimap<std::string, size_t> regionIndex;
    // income group index map
    std::multimap<std::string, size_t> incomeGroupIndex;
    size_t recordCount;

    // helper function to build the indexes after loading, makes queries way faster
    void buildIndexes();
    
    // different implementations for each strategy
    void loadWithOpenMP(const std::vector<std::string>& csvFiles);
    void loadWithCentralizedQueue(const std::vector<std::string>& csvFiles);
    void loadWithRoundRobin(const std::vector<std::string>& csvFiles);

public:
    // constructor and destructor
    PopulationData();
    ~PopulationData();

    // main loading function, can load single file or whole directory
    // strategy parameter picks which parallelization method to use
    void loadFromDirectory(const std::string& dirpath, 
                          ParallelStrategy strategy = ParallelStrategy::OPENMP);
    
    // these query methods return vectors of matching records
    std::vector<PopulationRecord> queryByCountry(const std::string& countryCode) const;
    std::vector<PopulationRecord> queryByRegion(const std::string& region) const;
    std::vector<PopulationRecord> queryByIncomeGroup(const std::string& incomeGroup) const;
    
    // these queries can use different parallel strategies too
    std::vector<PopulationRecord> queryByPopulationRange(double minPopulation, double maxPopulation, 
                                                         int year = 2020,
                                                         ParallelStrategy strategy = ParallelStrategy::OPENMP) const;
    std::vector<PopulationRecord> queryByYearRange(int startYear, int endYear,
                                                    ParallelStrategy strategy = ParallelStrategy::OPENMP) const;

    // inline getter returns number of records
    size_t size() const { return recordCount; }
    void clear();
};

#endif
