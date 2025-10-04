// facade class for managing fire data records
#ifndef FIRE_DATA_HPP
#define FIRE_DATA_HPP

#include <vector>
#include <string>
#include <map>
#include "firedata/fireRecord.hpp"
#include "common/parallelStrategy.hpp"

class FireData {
private:
    // vector storing all the fire records we loaded
    std::vector<FireRecord> records;
    // multimap lets us have multiple records with same key, maps pollutant type to record index for fast lookup
    std::multimap<std::string, size_t> pollutantIndex;
    size_t recordCount;

    // helper function to build the indexes after loading, makes queries way faster
    void buildIndexes();

    // different implementations for each strategy
    void loadWithOpenMP(const std::vector<std::string>& csvFiles);
    void loadWithCentralizedQueue(const std::vector<std::string>& csvFiles);
    void loadWithRoundRobin(const std::vector<std::string>& csvFiles);

public:
    // constructor and destructor
    FireData();
    ~FireData();

    // main loading function, can load single file or whole directory
    // strategy parameter picks which parallelization method to use
    void loadFromDirectory(const std::string& dirpath,
                          ParallelStrategy strategy = ParallelStrategy::OPENMP);

    // these query methods return vectors of matching records
    std::vector<FireRecord> queryByPollutant(const std::string& pollutantType) const;

    // these queries can use different parallel strategies too
    std::vector<FireRecord> queryByValueRange(double minValue, double maxValue,
                                               ParallelStrategy strategy = ParallelStrategy::OPENMP) const;
    std::vector<FireRecord> queryByGeographicBounds(double minLat, double maxLat,
                                                     double minLon, double maxLon,
                                                     ParallelStrategy strategy = ParallelStrategy::OPENMP) const;
    std::vector<FireRecord> queryByAQICategory(int category,
                                                ParallelStrategy strategy = ParallelStrategy::OPENMP) const;
    std::vector<FireRecord> queryBySiteName(const std::string& siteName,
                                             ParallelStrategy strategy = ParallelStrategy::OPENMP) const;

    // aggregation methods with parallel strategy support
    double calculateAverageConcentrationByPollutant(const std::string& pollutantType,
                                                     ParallelStrategy strategy = ParallelStrategy::OPENMP) const;
    std::map<int, size_t> countRecordsByCategory(ParallelStrategy strategy = ParallelStrategy::OPENMP) const;

    // inline getter returns number of records
    size_t size() const { return recordCount; }
    void clear();
};

#endif
