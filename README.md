# Data Storage and Retrieval Optimization using OpenMP Parallelization

## Overview
This project implements parallel data processing for wildfire and population datasets using OpenMP to optimize storage and retrieval operations.


## Build Instructions

1. Place the datasets folder in the project root directory
2. Build the project:
```bash
mkdir build && cd build
cmake ..
make
```

## Usage
```bash
./test_fire
./test_population
```

## Project Structure
- `src/firedata/` - Wildfire data processing implementation
- `src/PopulationData/` - Population data processing implementation
- `src/common/` - Shared utilities and parallel processing strategies
- `test/` - Unit tests for data processing modules

## Implementation Details

### Parallel Processing
The project utilizes OpenMP parallel strategies for efficient data processing:
- Parallel data loading and parsing
- Concurrent data structure operations
- Thread-safe data aggregation

### Data Structures
- Custom record types for fire and population data
- Optimized storage formats for query performance
- Memory-efficient data representations




