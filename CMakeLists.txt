project (BDBIDX)
cmake_minimum_required(VERSION 2.8)

add_subdirectory( BehaviorDB )

include_directories( ${PROJECT_SOURCE_DIR} /usr/local/include BehaviorDB/bdb)
link_directories(${PROJECT_SOURCE_DIR})

add_library(bdbidx BDBIDX.cpp)

add_executable (idx_test ${PROJECT_SOURCE_DIR}/main.cpp)
target_link_libraries(idx_test bdbidx bdb)

