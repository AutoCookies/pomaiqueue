cmake_minimum_required(VERSION 3.20)
project(pomaiqueue LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(POMAIQUEUE_BUILD_SERVER "Build pomaiqueued gRPC server" OFF)
option(POMAIQUEUE_BUILD_CLI "Build pomaiqctl" ON)
option(POMAIQUEUE_BUILD_TESTS "Build unit tests" ON)
option(POMAIQUEUE_BUILD_BENCH "Build benchmarks" ON)

add_library(pomaiqueue_core
  src/core/api/pomai_queue.cc
  src/core/engine/queue_engine.cc
  src/core/engine/shard_actor.cc
  src/core/model/message.h
  src/core/storage/checkpoint.cc
  src/core/storage/record.cc
  src/core/storage/segment_log.cc
  src/core/util/crc64.cc
  src/core/util/fsync.cc
)

target_include_directories(pomaiqueue_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

if (POMAIQUEUE_BUILD_CLI)
  add_executable(pomaiqctl src/cli/pomaiqctl.cc)
endif()

if (POMAIQUEUE_BUILD_SERVER)
  add_executable(pomaiqueued src/server/main.cc)
endif()

if (POMAIQUEUE_BUILD_TESTS)
  enable_testing()
  add_executable(test_record tests/unit/test_record.cc)
  target_link_libraries(test_record PRIVATE pomaiqueue_core)
  add_test(NAME test_record COMMAND test_record)
  add_executable(test_queue_engine tests/unit/test_queue_engine.cc)
  target_link_libraries(test_queue_engine PRIVATE pomaiqueue_core)
  add_test(NAME test_queue_engine COMMAND test_queue_engine)
endif()

if (POMAIQUEUE_BUILD_BENCH)
  add_executable(bench_pomaiqueue benchmarks/bench_pomaiqueue.cc)
endif()
