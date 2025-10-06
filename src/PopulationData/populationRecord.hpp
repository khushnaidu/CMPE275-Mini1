#ifndef POPULATION_RECORD_HPP
#define POPULATION_RECORD_HPP

#include <string>
#include <vector>

class PopulationRecord
{
private:
    std::string countryName;
    std::string countryCode;
    std::string indicatorName;
    std::string indicatorCode;
    std::vector<double> yearlyValues; // 1960-2023
    std::string region;
    std::string incomeGroup;
    std::string specialNotes;

public:
    PopulationRecord() {}

    PopulationRecord(const std::string& country, const std::string& code, 
                    const std::string& indicator, const std::string& indCode,
                    const std::vector<double>& values, const std::string& reg = "",
                    const std::string& income = "", const std::string& notes = "")
        : countryName(country), countryCode(code), indicatorName(indicator), 
          indicatorCode(indCode), yearlyValues(values), region(reg), 
          incomeGroup(income), specialNotes(notes) {}

    // getters
    const std::string& getCountryName() const { return countryName; }
    const std::string& getCountryCode() const { return countryCode; }
    const std::string& getIndicatorName() const { return indicatorName; }
    const std::string& getIndicatorCode() const { return indicatorCode; }
    const std::vector<double>& getYearlyValues() const { return yearlyValues; }
    const std::string& getRegion() const { return region; }
    const std::string& getIncomeGroup() const { return incomeGroup; }
    const std::string& getSpecialNotes() const { return specialNotes; }

    // 1960 is index 0, 2023 is index 63
    double getPopulationForYear(int year) const {
        int index = year - 1960;
        if (index >= 0 && index < static_cast<int>(yearlyValues.size())) {
            return yearlyValues[index];
        }
        return 0.0;
    }

    double getTotalPopulation() const {
        double total = 0.0;
        for (double value : yearlyValues) {
            total += value;
        }
        return total;
    }

    double getAveragePopulation() const {
        if (yearlyValues.empty()) return 0.0;
        return getTotalPopulation() / yearlyValues.size();
    }

    double getPopulationForYearRange(int startYear, int endYear) const {
        double total = 0.0;
        int count = 0;
        for (int year = startYear; year <= endYear; year++) {
            double value = getPopulationForYear(year);
            if (value > 0) {
                total += value;
                count++;
            }
        }
        return count > 0 ? total / count : 0.0;
    }

    // setters
    void setCountryName(const std::string& country) { countryName = country; }
    void setCountryCode(const std::string& code) { countryCode = code; }
    void setIndicatorName(const std::string& indicator) { indicatorName = indicator; }
    void setIndicatorCode(const std::string& code) { indicatorCode = code; }
    void setYearlyValues(const std::vector<double>& values) { yearlyValues = values; }
    void setRegion(const std::string& reg) { region = reg; }
    void setIncomeGroup(const std::string& income) { incomeGroup = income; }
    void setSpecialNotes(const std::string& notes) { specialNotes = notes; }

    void addYearlyValue(double value) { yearlyValues.push_back(value); }
};

#endif
