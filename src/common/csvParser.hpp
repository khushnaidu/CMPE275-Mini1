// CSV parser
#ifndef CSV_PARSER_HPP
#define CSV_PARSER_HPP

#include <string>
#include <vector>
#include <fstream>
#include <sstream>

class CSVParser {
public:
    // Static method - doesn't need an object instance to call (CSVParser::parseLine)
    // Default parameter: delimiter defaults to ',' if not provided
    static std::vector<std::string> parseLine(const std::string& line, char delimiter = ',') {
        std::vector<std::string> fields;
        std::string field;
        // Tracks whether we're inside quoted text (to handle commas within quotes)
        bool inQuotes = false;

        for (size_t i = 0; i < line.length(); ++i) {
            char c = line[i];
            if (c == '"') {
                // Handle escaped quotes: "" inside quotes becomes a single "
                if (inQuotes && i + 1 < line.length() && line[i + 1] == '"') {
                    field += '"';
                    ++i; // Skip next quote
                } else {
                    // Toggle quote mode with ! operator (true becomes false, false becomes true)
                    inQuotes = !inQuotes;
                }
            } else if (c == delimiter && !inQuotes) {
                // Only split on delimiter if we're not inside quotes
                fields.push_back(field);
                field.clear();
            } else {
                field += c;
            }
        }
        // Add the last field
        fields.push_back(field);
        return fields;
    }

    // Reads entire CSV file and returns 2D vector (vector of rows, each row is vector of strings)
    static std::vector<std::vector<std::string>> readFile(const std::string& filename,
                                                           bool hasHeader = false,
                                                           char delimiter = ',') {
        // Opens input file stream
        std::ifstream file(filename);
        if (!file.is_open()) {
            // Throws exception that propagates to caller for error handling
            throw std::runtime_error("Cannot open file: " + filename);
        }

        std::vector<std::vector<std::string>> data;
        std::string line;
        bool firstLine = true;

        // std::getline reads line-by-line until end of file
        while (std::getline(file, line)) {
            // Skip header row if hasHeader is true
            if (hasHeader && firstLine) {
                firstLine = false;
                continue; // Skip to next iteration
            }
            if (line.empty()) continue;
            // Remove carriage return '\r' if present (handles Windows line endings)
            if (!line.empty() && line.back() == '\r') line.pop_back();

            data.push_back(parseLine(line, delimiter));
        }

        file.close();
        return data;
    }

    // Safely converts string to double with error handling
    static double toDouble(const std::string& str, double defaultValue = 0.0) {
        try {
            // Ternary: if string is empty return default, else convert with std::stod
            return str.empty() ? defaultValue : std::stod(str);
        } catch (...) {
            // Catch-all (...) handles any exception type (e.g., invalid format)
            return defaultValue;
        }
    }

    // Safely converts string to int with error handling
    static int toInt(const std::string& str, int defaultValue = 0) {
        try {
            // std::stoi converts string to integer
            return str.empty() ? defaultValue : std::stoi(str);
        } catch (...) {
            return defaultValue;
        }
    }
};

#endif