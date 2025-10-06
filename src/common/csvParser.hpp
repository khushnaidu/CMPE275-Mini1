#ifndef CSV_PARSER_HPP
#define CSV_PARSER_HPP

#include <string>
#include <vector>
#include <fstream>
#include <sstream>

class CSVParser {
public:
    
    static std::vector<std::string> parseLine(const std::string& line, char delimiter = ',') {
        std::vector<std::string> fields;
        std::string field;
        bool inQuotes = false;

        for (size_t i = 0; i < line.length(); ++i) {
            char c = line[i];
            if (c == '"') {
                // handle escaped quotes
                if (inQuotes && i + 1 < line.length() && line[i + 1] == '"') {
                    field += '"';
                    ++i; 
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == delimiter && !inQuotes) {
                fields.push_back(field);
                field.clear();
            } else {
                field += c;
            }
        }
        fields.push_back(field);
        return fields;
    }

    // read entire CSV file
    static std::vector<std::vector<std::string>> readFile(const std::string& filename,
                                                           bool hasHeader = false,
                                                           char delimiter = ',') {
        std::ifstream file(filename);
        if (!file.is_open()) {
            throw std::runtime_error("Cannot open file: " + filename);
        }

        std::vector<std::vector<std::string>> data;
        std::string line;
        bool firstLine = true;

        while (std::getline(file, line)) {
            if (hasHeader && firstLine) {
                firstLine = false;
                continue;
            }
            if (line.empty()) continue;
            if (!line.empty() && line.back() == '\r') line.pop_back();

            data.push_back(parseLine(line, delimiter));
        }

        file.close();
        return data;
    }

    // safe string to double conversion
    static double toDouble(const std::string& str, double defaultValue = 0.0) {
        try {
            return str.empty() ? defaultValue : std::stod(str);
        } catch (...) {
            return defaultValue;
        }
    }

    // safe string to int conversion
    static int toInt(const std::string& str, int defaultValue = 0) {
        try {
            return str.empty() ? defaultValue : std::stoi(str);
        } catch (...) {
            return defaultValue;
        }
    }
};

#endif
