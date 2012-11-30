project (BDBIDX)
cmake_minimum_required(VERSION 2.8)

add_subdirectory( BehaviorDB )
include (BehaviorDB/use_c++11.cmake)

include_directories( ${PROJECT_SOURCE_DIR} BehaviorDB)
link_directories(${PROJECT_SOURCE_DIR} )

set (Boost_ADDITIONAL_VERSIONS "1.52" "1.52.0" )
find_package ( Boost 1.52.0 )

if(Boost_FOUND)
  include_directories(${Boost_INCLUDE_DIRS})
endif()

add_definitions ( -DBDB_STATIC )
add_library(bdbidx BDBIDX.cpp)
target_link_libraries (bdbidx bdb)

#add_library(rec_db rec_db.cpp BDBIDX.cpp)

option( BDBIDX_BUILD_TEST "Build tests" OFF )

if( BDBIDX_BUILD_TEST )
  add_executable (idx_test ${PROJECT_SOURCE_DIR}/main.cpp)
  target_link_libraries(idx_test bdbidx)
endif()

install (FILES BDBIDX.h DESTINATION include/BDBIDX)
#install (TARGETS bdbidx DESTINATION lib EXPORT bdbidx-targets )
#install (EXPORT bdbidx-targets DESTINATION lib)

