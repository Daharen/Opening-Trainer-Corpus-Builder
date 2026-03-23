cmake_minimum_required(VERSION 3.20)

project(opening_trainer_corpus_builder VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_executable(opening-trainer-corpus-builder
    src/main.cpp
    src/cli.cpp
    src/config.cpp
    src/manifest.cpp
    src/bundle_writer.cpp
    src/build_plan.cpp
    src/preflight.cpp
    src/source_boundaries.cpp
    src/range_plan.cpp
    src/game_envelope.cpp
    src/header_scan.cpp
)

target_include_directories(opening-trainer-corpus-builder
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

if(MSVC)
    target_compile_options(opening-trainer-corpus-builder PRIVATE /W4 /permissive-)
else()
    target_compile_options(opening-trainer-corpus-builder PRIVATE -Wall -Wextra -Wpedantic)
endif()
