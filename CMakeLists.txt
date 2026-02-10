cmake_minimum_required(VERSION 3.20)
project(pomaiqueue LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(POMAIQUEUE_BUILD_SERVER "Build pomaiqueued gRPC server" OFF)
option(POMAIQUEUE_BUILD_CLI "Build pomaiqctl" ON)
option(POMAIQUEUE_BUILD_TESTS "Build unit tests" ON)
option(POMAIQUEUE_BUILD_BENCH "Build benchmarks" ON)
option(POMAIQUEUE_ENABLE_ASAN_UBSAN "Enable sanitizers" OFF)

if (POMAIQUEUE_ENABLE_ASAN_UBSAN)
  add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address,undefined)
endif()

add_library(pomaiqueue_core
  src/core/api/pomai_queue.cc
  src/core/engine/queue_engine.cc
  src/core/engine/shard_actor.cc
  src/core/model/message.h
  src/core/storage/checkpoint.cc
  src/core/storage/record.cc
  src/core/storage/segment_log.cc
  src/core/util/crc64.cc
  src/core/util/failpoint.cc
  src/core/util/fsync.cc
)

target_include_directories(pomaiqueue_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

if (POMAIQUEUE_BUILD_CLI)
  add_executable(pomaiqctl src/cli/pomaiqctl.cc)
  target_link_libraries(pomaiqctl PRIVATE pomaiqueue_core)
  add_executable(pomaiqueue_stress src/cli/pomaiqueue_stress.cc)
  target_link_libraries(pomaiqueue_stress PRIVATE pomaiqueue_core)
endif()

if (POMAIQUEUE_BUILD_SERVER)
  add_executable(pomaiqueued src/server/main.cc)
endif()

if (POMAIQUEUE_BUILD_TESTS)
  enable_testing()
  add_executable(test_record tests/unit/test_record.cc)
  target_link_libraries(test_record PRIVATE pomaiqueue_core)
  add_test(NAME test_record COMMAND test_record)

  add_executable(test_segment_log tests/unit/test_segment_log.cc)
  target_link_libraries(test_segment_log PRIVATE pomaiqueue_core)
  add_test(NAME test_segment_log COMMAND test_segment_log)

  add_executable(test_checkpoint tests/unit/test_checkpoint.cc)
  target_link_libraries(test_checkpoint PRIVATE pomaiqueue_core)
  add_test(NAME test_checkpoint COMMAND test_checkpoint)

  add_executable(test_queue_engine tests/unit/test_queue_engine.cc)
  target_link_libraries(test_queue_engine PRIVATE pomaiqueue_core)
  add_test(NAME test_queue_engine COMMAND test_queue_engine)

  add_executable(test_engine_integration tests/integration/test_engine_integration.cc)
  target_link_libraries(test_engine_integration PRIVATE pomaiqueue_core)
  add_test(NAME test_engine_integration COMMAND test_engine_integration)

  add_executable(crash_runner tests/crash/runner.cc)
  target_link_libraries(crash_runner PRIVATE pomaiqueue_core)
  add_test(NAME crash_runner COMMAND crash_runner 2)
endif()

if (POMAIQUEUE_BUILD_BENCH)
  add_executable(bench_enqueue benchmarks/bench_enqueue.cc)
  target_link_libraries(bench_enqueue PRIVATE pomaiqueue_core)
  add_executable(bench_lease_ack benchmarks/bench_lease_ack.cc)
  target_link_libraries(bench_lease_ack PRIVATE pomaiqueue_core)
  add_executable(bench_timeout_retry benchmarks/bench_timeout_retry.cc)
  target_link_libraries(bench_timeout_retry PRIVATE pomaiqueue_core)
  add_executable(bench_recovery benchmarks/bench_recovery.cc)
  target_link_libraries(bench_recovery PRIVATE pomaiqueue_core)
endif()
