project (BDBIDX)
cmake_minimum_required(VERSION 2.8)

add_subdirectory( BehaviorDB )

include_directories( ${PROJECT_SOURCE_DIR} /usr/local/include BehaviorDB)
link_directories(${PROJECT_SOURCE_DIR} /usr/local/lib)

add_library(bdbidx BDBIDX.cpp)

add_library(rec_db rec_db.cpp BDBIDX.cpp)

add_executable (idx_test ${PROJECT_SOURCE_DIR}/main.cpp)
target_link_libraries(idx_test bdbidx bdb)

install (FILES BDBIDX.h DESTINATION include/BDBIDX)
install (TARGETS bdbidx DESTINATION lib EXPORT bdbidx-targets )
install (EXPORT bdbidx-targets DESTINATION lib)

