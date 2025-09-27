#include <vector>
#include <string>
#include <iostream>
#include <cstdio>
#include <cstdlib>
#include <filesystem>


struct User { int id; std::string name; int age; };
struct fire {
    std::string latitude;
    std::string longitude;
    std::string UTC;
    std::string concentration;
    std::string unit;
    std::string raw_concentration;
    std::string AQI;
    std::string category;
    std::string site_name;
    std::string site_agency;
    std::string AQS_ID;
    std::string Full_AQS_ID;
};

void parse_csv_record(const char* line, std::vector<std::string>& out) {
    out.clear();
    std::string field;
    int i = 0;
    int in_quotes = 0;

    while (line[i] != '\0' && line[i] != '\r' && line[i] != '\n') {
        char c = line[i];
        if (in_quotes) {
            if (c == '"') {
                if (line[i + 1] == '"') {      // escaped quote
                    field.push_back('"');
                    ++i;
                } else {
                    in_quotes = 0;
                }
            } else {
                field.push_back(c);
            }
        } else {
            if (c == '"') {
                in_quotes = 1;
            } else if (c == ',') {
                out.push_back(field);
                field.clear();
            } else {
                field.push_back(c);
            }
        }
        ++i;
    }
    out.push_back(field);
}

// Generic CSV loader: Parser is a callable with signature bool(const std::vector<std::string>& cols, T& out)
template<typename T, typename Parser>
void load_csv(const char* path, std::vector<T>& out_items, int has_header, Parser parser) {
    out_items.clear();
    FILE* f = std::fopen(path, "rb");
    if (!f) { std::perror("open"); return; }

    char line[4096];
    if (has_header) { if (!std::fgets(line, sizeof line, f)) { std::fclose(f); return; } }

    while (std::fgets(line, sizeof line, f)) {
        std::vector<std::string> cols;
        parse_csv_record(line, cols);
        T item;
        if (parser(cols, item)) {
            out_items.push_back(item);
        }
    }
    std::fclose(f);
}

template<typename T, typename Parser>
void load_csv_from_dir(const std::string& dir, std::vector<T>& out_items, int has_header, Parser parser, 
    const std::string& filename_pattern = ".csv") {
        out_items.clear();
        try {
            // Recursively iterate through all files in the directory
            for (const auto& entry : std::filesystem::recursive_directory_iterator(dir)) {
                if (entry.is_regular_file()) {
                    std::string file_path = entry.path().string();
                    
                    // Check if file matches the pattern (default: .csv)
                    if (file_path.find(filename_pattern) != std::string::npos) {
                        std::cout << "Loading CSV file: " << file_path << std::endl;
                        
                        // Load CSV from this file
                        std::vector<T> file_items;
                        load_csv(file_path.c_str(), file_items, has_header, parser);
                        
                        // Append to the main collection
                        out_items.insert(out_items.end(), file_items.begin(), file_items.end());
                        
                        std::cout << "Loaded " << file_items.size() << " records from " << file_path << std::endl;
                    }
                }
            }
        } catch (const std::filesystem::filesystem_error& ex) {
            std::cerr << "Filesystem error: " << ex.what() << std::endl;
        }
        
        std::cout << "Total records loaded: " << out_items.size() << std::endl;
}

// Generic printer
template<typename T, typename Printer>
void print_items(const std::vector<T>& items, Printer printer) {
    for (size_t i = 0; i < items.size(); ++i) {
        printer(items[i]);
    }
}




int main(int argc, char** argv) {
    if (argc <= 1) {
        std::fprintf(stderr, "Usage: %s <csv_file_or_directory>\n", argv[0] ? argv[0] : "mini1");
        std::fprintf(stderr, "Examples:\n");
        std::fprintf(stderr, "  %s single_file.csv\n", argv[0] ? argv[0] : "mini1");
        std::fprintf(stderr, "  %s datasets/2020-fire/data\n", argv[0] ? argv[0] : "mini1");
        return 1;
    }

    const char* path = argv[1];

    std::vector<fire> fires;
    auto fire_parser = [](const std::vector<std::string>& cols, fire& f) -> bool {
        if (cols.size() < 12) return false;
        f.latitude = cols[0];
        f.longitude = cols[1];
        f.UTC = cols[2];
        f.concentration = cols[3];
        f.unit = cols[4];
        f.raw_concentration = cols[5];
        f.AQI = cols[6];
        f.category = cols[7];
        f.site_name = cols[8];
        f.site_agency = cols[9];
        f.AQS_ID = cols[10];
        f.Full_AQS_ID = cols[11];
        return true;
    };

    auto fire_printer = [](const fire& f) {
        std::printf("latitude=%s longitude=%s UTC=%s concentration=%s\n",
                    f.latitude.c_str(), f.longitude.c_str(), f.UTC.c_str(), f.concentration.c_str());
    };

    // Check if the path is a directory or a file
    std::filesystem::path fs_path(path);
    
    if (std::filesystem::is_directory(fs_path)) {
        //std::cout << "Loading CSV files from directory: " << path << std::endl;
        // Load from all subdirectories recursively
        load_csv_from_dir(path, fires, 1, fire_parser);
    } else {
        std::cout << "Loading single CSV file: " << path << std::endl;
        // Load single file (original behavior)
        load_csv(path, fires, 1, fire_parser);
    }
    
    print_items<fire>(fires, fire_printer);
    return 0;
}
