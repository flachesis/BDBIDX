project (BDBIDX)
cmake_minimum_required(VERSION 2.8)

add_subdirectory( BehaviorDB )

set (Boost_ADDITIONAL_VERSIONS "1.47" "1.47.0" )
find_package ( Boost 1.45.0 )

if(Boost_FOUND)
  include_directories(${Boost_INCLUDE_DIRS})
endif()

include_directories( ${PROJECT_SOURCE_DIR} BehaviorDB)
link_directories(${PROJECT_SOURCE_DIR} )

add_library(bdbidx BDBIDX.cpp)

add_library(rec_db rec_db.cpp BDBIDX.cpp)

add_executable (idx_test ${PROJECT_SOURCE_DIR}/main.cpp)
target_link_libraries(idx_test bdbidx bdb)

install (FILES BDBIDX.h DESTINATION include/BDBIDX)
install (TARGETS bdbidx DESTINATION lib EXPORT bdbidx-targets )
install (EXPORT bdbidx-targets DESTINATION lib)

