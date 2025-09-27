cmake_minimum_required(VERSION 4.0)

project(mini1)

# compilers to use (9.2)
#set(CMAKE_C_COMPILER /opt/homebrew/Cellar/gcc/15.1.0/bin/gcc-15)
#set(CMAKE_CXX_COMPILER  /opt/homebrew/Cellar/gcc/15.1.0/bin/g++-15)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/Modules)

# openmp flags
#SET(OPENMP_CXX_FLAGS "-O3 -fopenmp --target=x86_64-pc-linux-gnu")
#SET(OPENMP_CXX_FLAGS "-O3 -fopenmp")
#SET(OPENMP_CXX_FLAGS "-O3")
SET(CMAKE_CXX_FLAGS  "${CMAKE_CXX_FLAGS} ${OPENMP_CXX_FLAGS}" )


add_executable(mini1 mini1.cpp)


