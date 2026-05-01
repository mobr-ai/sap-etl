cmake_minimum_required(VERSION 3.24)
project(sap-etl VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(SAP_ETL_BUILD_TESTS "Build tests" ON)

find_package(PkgConfig REQUIRED)
pkg_check_modules(CURL REQUIRED IMPORTED_TARGET libcurl)

add_library(sap_etl_lib
    src/app/application.cpp
    src/checkpoint/checkpoint_store.cpp
    src/clickhouse/clickhouse_client.cpp
    src/config/cli_parser.cpp
    src/http/http_client.cpp
    src/load/qlever_loader.cpp
    src/load/ttl_loader.cpp
    src/model/selection.cpp
    src/source/clickhouse_jetstreamer_source.cpp
    src/transform/ontology_mapper.cpp
    src/util/filesystem.cpp
    src/util/strings.cpp
)

target_include_directories(sap_etl_lib PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(sap_etl_lib PUBLIC PkgConfig::CURL)

add_executable(sap-etl src/main.cpp)
target_link_libraries(sap-etl PRIVATE sap_etl_lib)

if (SAP_ETL_BUILD_TESTS)
  enable_testing()
  add_executable(sap_etl_tests
      tests/test_main.cpp
      tests/test_cli_parser.cpp
      tests/test_selection.cpp
      tests/test_ontology_mapper.cpp
  )
  target_link_libraries(sap_etl_tests PRIVATE sap_etl_lib)
  add_test(NAME sap_etl_tests COMMAND sap_etl_tests)
endif()
