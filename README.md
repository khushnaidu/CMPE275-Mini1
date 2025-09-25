# Optimization of Data Storage and Retrieval using Parellization with OpenMP

## Setup
1. Copy the datasets folder into the main directory
2. Build and compile using CMake
```
    mkdir build
    cd build
    cmake ..
    make
```
## Simple working CSV parser and storage

### Currently testing in one cpp file, example run:
```
./mini1 ../data/2020-fire/data/20200810/20200810-01.csv
```

## To-do
1. Make a header file implementation for reusability
2. Add the retreival logic
3. Allow for multiple csv files to be read
4. Find a way to make a template for the row fields to be made into a struct without hardcoding the specific column headers
5. Finding which part of the storage or retrieval operation would best benefit from threading