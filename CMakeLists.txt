cmake_minimum_required(VERSION 4.0)

project(mini1)

# compilers to use (9.2)
#set(CMAKE_C_COMPILER /opt/homebrew/Cellar/gcc/15.1.0/bin/gcc-15)
#set(CMAKE_CXX_COMPILER  /opt/homebrew/Cellar/gcc/15.1.0/bin/g++-15)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/Modules)

# openmp flags
# UNCOMMENT to enable OpenMP optimization
#SET(OPENMP_CXX_FLAGS "-O3 -fopenmp --target=x86_64-pc-linux-gnu")
SET(OPENMP_CXX_FLAGS "-O3 -fopenmp")
SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} ${OPENMP_CXX_FLAGS}" )


include_directories(${CMAKE_SOURCE_DIR})
include_directories(${CMAKE_SOURCE_DIR}/src)

add_executable(test_fire
    src/firedata/fireData.cpp
    test/test_FireData.cpp
)

# Required for OpenMP on macOS - links C++ standard library
target_link_libraries(test_fire c++)


