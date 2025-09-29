// include/DataLoader.hpp
#pragma once
#include "PopulationData.hpp"
#include <string>
#include <vector>
#include <functional>
#include <fstream>
#include <iostream>

class CSVParser {
    private:
        // Helper functions for parsing
        std::vector<std::string> splitCSVLine(const std::string& line) const;
        std::string removeQuotes(const std::string& field) const;
        RegionType parseRegion(const std::string& regionStr) const;
        IncomeLevel parseIncomeLevel(const std::string& incomeStr) const;
        long long parsePopulation(const std::string& str) const;
        bool isValidPopulation(const std::string& str) const;
        
    public:
        template <typename T, typename Parser>
        bool loadCSV(const std::string& path, std::vector<T>& out_items, bool has_header, Parser parser) const {
            out_items.clear();
            std::ifstream file(path);
            if (!file.is_open()) {
                std::cerr << "Error: Cannot open file: " << path << std::endl;
                return false;
            }
            
            std::string line;
            int line_number = 0;
            
            // Skip header if needed
            if (has_header && !std::getline(file, line)) {
                std::cerr << "Error: Empty file or cannot read header" << std::endl;
                return false;
            }
            
            // Parse data lines
            while (std::getline(file, line)) {
                line_number++;
                if (line.empty()) continue;
                
                std::vector<std::string> cols = splitCSVLine(line);
                T item;
                if (parser(cols, item)) {
                    out_items.push_back(item);
                } else {
                    std::cerr << "Warning: Skipped malformed line " << line_number 
                                << " in " << path << std::endl;
                }
            }
            
            std::cout << "Loaded " << out_items.size() << " records from " << path << std::endl;
            return true;
        }