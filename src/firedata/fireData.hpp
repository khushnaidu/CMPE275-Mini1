#ifndef FIRE_DATA_HPP
#define FIRE_DATA_HPP

#include <vector>
#include <string>
#include <map>
#include "firedata/fireRecord.hpp"
#include "common/parallelStrategy.hpp"

class FireData {
private:
    std::vector<FireRecord> records;
    std::multimap<std::string, size_t> pollutantIndex;  // maps pollutant type to record index
    size_t recordCount;
    ParallelStrategy strategy;

    void buildIndexes();
    
    // different loading strategies
    void loadSerial(const std::vector<std::string>& csvFiles);
    void loadWithOpenMP(const std::vector<std::string>& csvFiles);
    void loadWithCentralizedQueue(const std::vector<std::string>& csvFiles);
    void loadWithRoundRobin(const std::vector<std::string>& csvFiles);

public:
    FireData();
    ~FireData();

    void loadFromDirectory(const std::string& dirpath, ParallelStrategy strat = ParallelStrategy::OPENMP);
    
    // query methods
    std::vector<FireRecord> queryByPollutant(const std::string& pollutantType) const;
    std::vector<FireRecord> queryByValueRange(double minValue, double maxValue, 
                                              ParallelStrategy strat = ParallelStrategy::OPENMP) const;
    std::vector<FireRecord> queryByGeographicBounds(double minLat, double maxLat, 
                                                     double minLon, double maxLon,
                                                     ParallelStrategy strat = ParallelStrategy::OPENMP) const;
    std::vector<FireRecord> queryByAQICategory(int category, 
                                               ParallelStrategy strat = ParallelStrategy::OPENMP) const;
    std::vector<FireRecord> queryBySiteName(const std::string& siteName, 
                                            ParallelStrategy strat = ParallelStrategy::OPENMP) const;
    
    // aggregation methods
    double calculateAverageConcentrationByPollutant(const std::string& pollutantType,
                                                    ParallelStrategy strat = ParallelStrategy::OPENMP) const;
    std::map<int, size_t> countRecordsByCategory(ParallelStrategy strat = ParallelStrategy::OPENMP) const;

    size_t size() const { return recordCount; }
    void clear();
};

#endif
