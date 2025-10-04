// Class for storing fire data records
#ifndef FIRE_RECORD_HPP
#define FIRE_RECORD_HPP

#include <string>

class FireRecord
{
private:
    double latitude;
    double longitude;
    std::string UTC;
    std::string pollutantType;
    double concentration;
    std::string unit;
    double rawConcentration;
    int aqi;
    int category;
    std::string siteName;
    std::string agencyName;
    std::string aqsId;
    std::string fullAqsId;

public:
    // Default constructor initializes numeric fields to 0 using initializer list
    FireRecord() : latitude(0.0), longitude(0.0), concentration(0.0), rawConcentration(0.0), aqi(0), category(0) {}

    // Parameterized constructor takes const references to avoid copying strings (more efficient)
    FireRecord(double lat, double lon, const std::string &utc, const std::string &pollutant,
               double conc, const std::string &u, double raw, int aqiVal, int cat,
               const std::string &site, const std::string &agency, const std::string &aqsid,
               const std::string &fullaqsid)
        // Initializer list directly assigns all member variables from parameters
        : latitude(lat), longitude(lon), UTC(utc), pollutantType(pollutant),
          concentration(conc), unit(u), rawConcentration(raw), aqi(aqiVal), category(cat),
          siteName(site), agencyName(agency), aqsId(aqsid), fullAqsId(fullaqsid) {}

    // Getter methods - all marked const since they don't modify the object
    double getLatitude() const
    {
        return latitude;
    }
    double getLongitude() const
    {
        return longitude;
    }
    // Returns const reference to avoid copying the string
    const std::string &getUTC() const
    {
        return UTC;
    }
    const std::string &getPollutantType() const
    {
        return pollutantType;
    }
    double getConcentration() const
    {
        return concentration;
    }
    const std::string &getUnit() const
    {
        return unit;
    }
    double getRawConcentration() const
    {
        return rawConcentration;
    }
    int getAqi() const
    {
        return aqi;
    }
    int getCategory() const
    {
        return category;
    }
    const std::string &getSiteName() const
    {
        return siteName;
    }

    // Setter methods - modify the object's state
    void setLatitude(double lat)
    {
        latitude = lat;
    }
    void setLongitude(double lon)
    {
        longitude = lon;
    }
    // Takes const reference to avoid unnecessary copying of string parameter
    void setUTC(const std::string &utc)
    {
        UTC = utc;
    }
    void setPollutantType(const std::string &pollutant)
    {
        pollutantType = pollutant;
    }
    void setConcentration(double conc)
    {
        concentration = conc;
    }
    void setUnit(const std::string &u)
    {
        unit = u;
    }
    void setRawConcentration(double raw)
    {
        rawConcentration = raw;
    }
    void setAqi(int aqiVal)
    {
        aqi = aqiVal;
    }
    void setCategory(int cat)
    {
        category = cat;
    }
    void setSiteName(const std::string &site)
    {
        siteName = site;
    }
    void setAgencyName(const std::string &agency)
    {
        agencyName = agency;
    }
    void setAqsId(const std::string &aqsid)
    {
        aqsId = aqsid;
    }
    void setFullAqsId(const std::string &fullaqsid)
    {
        fullAqsId = fullaqsid;
    }
};

#endif