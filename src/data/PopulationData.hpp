#pragma once

#include <vector>
#include <string>
#include <unordered_map>
#include <map>

// Defining enum classes for country metadata to help with the parsing of the data
enum class IncomeLevel {
    LOW_INCOME,
    LOWER_MIDDLE_INCOME,
    UPPER_MIDDLE_INCOME,
    HIGH_INCOME,
    UNCLASSIFIED
};

enum class RegionType{
    SUB_SAHARAN_AFRICA,
    EUROPE_CENTRAL_ASIA,
    MIDDLE_EAST_NORTH_AFRICA,
    LATIN_AMERICA_AND_THE_CARIBBEAN,
    EAST_ASIA_PACIFIC,
    NORTH_AMERICA,
    SOUTH_ASIA,
    OTHER
};

struct CountryMetadata {

    std::string country_code;
    IncomeLevel income_level;
    RegionType region_type;
    std::string special_notes;
    std::string table_name;
    bool is_it_an_aggregate;

    CountryMetadata() : region_type(RegionType::OTHER), income_level(IncomeLevel::UNCLASSIFIED), is_it_an_aggregate(false) {}
};

class PopulationData {

    private: 
        std::string country_code;
        std::string country_name;
        CountryMetadata country_metadata;
        std::map<int, long long> population_data;

        // Constructors
        PopulationData() = default;
        PopulationData(const std::string& code, const std::string& name, const CountryMetadata& metadata) :
            country_code(code),
            country_name(name), 
            country_metadata(metadata)
        {}

        // Getters
        std::string getCountryCode() const { return country_code; }
        std::string getCountryName() const { return country_name; }
        CountryMetadata getMetadata() const { return country_metadata; }
        
        // Get population for a specific year
        long long getPopulationForYear(int year) const {
            auto it = population_data.find(year);
            if (it != population_data.end()) {
                return it->second;
            }
            return -1;
        }
        // Setters
        void setCountryCode(const std::string& code) { countryCode = code; }
        void setCountryName(const std::string& name) { countryName = name; }
        void setMetadata(const CountryMetadata& meta) { metadata = meta; }
        void setPopulation(int year, long long population) {
            if (population >= 0) {  
            yearlyPopulation[year] = population;
        }
    }

}
