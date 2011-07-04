project (BDBIDX)
cmake_minimum_required(VERSION 2.8)

include_directories( ${PROJECT_SOURCE_DIR} /usr/local/include)
link_directories(${PROJECT_SOURCE_DIR})

add_library(bdbidx BDBIDX.cpp)

add_executable (bdb_test ${PROJECT_SOURCE_DIR}/main.cpp)
target_link_libraries(bdb_test bdbidx bdb)

