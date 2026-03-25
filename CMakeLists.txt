cmake_minimum_required(VERSION 3.20)

project(opening_trainer_corpus_builder VERSION 0.1.0 LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(OTCB_USE_SYSTEM_SQLITE "Use system-provided SQLite instead of the repo-local default path." OFF)

add_executable(opening-trainer-corpus-builder
    src/main.cpp
    src/cli.cpp
    src/config.cpp
    src/progress.cpp
    src/manifest.cpp
    src/bundle_writer.cpp
    src/build_plan.cpp
    src/preflight.cpp
    src/source_boundaries.cpp
    src/range_plan.cpp
    src/game_envelope.cpp
    src/header_scan.cpp
    src/chess_types.cpp
    src/chess_board.cpp
    src/san_tokenizer.cpp
    src/san_replay.cpp
    src/opening_extraction.cpp
    src/position_key.cpp
    src/aggregation.cpp
    src/sqlite_writer.cpp
)

if(OTCB_USE_SYSTEM_SQLITE)
    find_package(SQLite3 REQUIRED)
    set(OTCB_SQLITE_TARGET SQLite::SQLite3)
else()
    add_library(otcb_sqlite_vendor STATIC
        third_party/sqlite/sqlite3.c
    )
    target_include_directories(otcb_sqlite_vendor PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/sqlite
    )

    if(MSVC)
        target_compile_options(otcb_sqlite_vendor PRIVATE /W0)
    else()
        target_compile_options(otcb_sqlite_vendor PRIVATE -w)
    endif()

    # The default repo-local path exposes SQLite headers from third_party/sqlite.
    # Link libc sqlite symbols explicitly for environments where sqlite is packaged
    # as a system library.
    target_link_libraries(otcb_sqlite_vendor PUBLIC sqlite3)
    set(OTCB_SQLITE_TARGET otcb_sqlite_vendor)
endif()

target_include_directories(opening-trainer-corpus-builder
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

if(MSVC)
    target_compile_options(opening-trainer-corpus-builder PRIVATE /W4 /permissive-)
else()
    target_compile_options(opening-trainer-corpus-builder PRIVATE -Wall -Wextra -Wpedantic)
endif()

target_link_libraries(opening-trainer-corpus-builder PRIVATE ${OTCB_SQLITE_TARGET})


add_executable(build-behavioral-training-extract
    src/build_behavioral_training_extract_main.cpp
    src/behavioral_extract.cpp
    src/chess_types.cpp
    src/chess_board.cpp
    src/san_replay.cpp
    src/position_key.cpp
)

target_include_directories(build-behavioral-training-extract
    PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
)

if(MSVC)
    target_compile_options(build-behavioral-training-extract PRIVATE /W4 /permissive-)
else()
    target_compile_options(build-behavioral-training-extract PRIVATE -Wall -Wextra -Wpedantic)
endif()

target_link_libraries(build-behavioral-training-extract PRIVATE ${OTCB_SQLITE_TARGET} zstd)

include(CTest)
if(BUILD_TESTING)
    find_package(Python3 COMPONENTS Interpreter REQUIRED)
    add_test(
        NAME sqlite_aggregate_payload_validation
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/validate_sqlite_payload.py
                $<TARGET_FILE:opening-trainer-corpus-builder> ${CMAKE_CURRENT_SOURCE_DIR}
    )
    add_test(
        NAME jsonl_aggregate_payload_validation
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/validate_jsonl_payload.py
                $<TARGET_FILE:opening-trainer-corpus-builder> ${CMAKE_CURRENT_SOURCE_DIR}
    )
    add_test(
        NAME behavioral_extract_validation
        COMMAND ${Python3_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/tests/validate_behavioral_extract.py
                $<TARGET_FILE:build-behavioral-training-extract> ${CMAKE_CURRENT_SOURCE_DIR}
    )
endif()
