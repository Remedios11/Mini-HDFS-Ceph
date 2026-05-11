cmake_minimum_required(VERSION 3.10)
project(mini_storage CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_compile_options(-Wall -Wextra -O2)

# ===== 依赖 =====
find_package(Protobuf REQUIRED)
find_package(ZLIB REQUIRED)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/proto
    ${Protobuf_INCLUDE_DIRS}
)

# ===== Phase 1：单机存储引擎 =====
set(PHASE1_SRCS
    coding.cpp
    db_write_batch.cpp
    db.cpp
    log_reader.cpp
    log_writer.cpp
    Lrucache.cpp
    MemTable.cpp
    SSTableBuilder.cpp
    SSTableReader.cpp
)

add_library(phase1_engine STATIC ${PHASE1_SRCS})
target_link_libraries(phase1_engine ZLIB::ZLIB pthread stdc++fs)

# ===== Phase 2：分布式层 =====
set(PROTO_SRCS
    proto/storage.pb.cc
    proto/namenode.pb.cc
    proto/datanode.pb.cc
)
set(NET_SRCS
    net_thread_pool.cpp
    net_channel.cpp
    net_event_loop.cpp
    net_tcp_connection.cpp
    net_tcp_server.cpp
)
set(NAMENODE_SRCS
    namenode_metadata_store.cpp
    namenode_block_manager.cpp
    namenode_datanode_manager.cpp
    namenode_edit_log.cpp
    namenode_namenode_server.cpp
    namenode_replication_monitor.cpp
    namenode_block_replicator.cpp
    namenode_consistency_checker.cpp
    namenode_fault_detector.cpp
)
set(DATANODE_SRCS
    datanode_block_store.cpp
    datanode_pipeline_handler.cpp
    datanode_datanode_client.cpp
    datanode_datanode_server.cpp
)

add_library(phase2_distributed STATIC
    ${PROTO_SRCS} ${NET_SRCS} ${NAMENODE_SRCS} ${DATANODE_SRCS}
    client_storage_client.cpp
)
target_link_libraries(phase2_distributed
    ${Protobuf_LIBRARIES} ZLIB::ZLIB pthread stdc++fs)

# ===== 可执行文件 =====
add_executable(namenode_server main_namenode.cpp)
target_link_libraries(namenode_server phase2_distributed)

add_executable(datanode_server main_datanode.cpp)
target_link_libraries(datanode_server phase2_distributed)

# ===== 测试 =====
enable_testing()

add_executable(test_phase1 test_phase1.cpp)
target_link_libraries(test_phase1 phase1_engine)
add_test(NAME Phase1_Engine COMMAND test_phase1)

add_executable(test_thread_pool test_thread_pool.cpp)
target_link_libraries(test_thread_pool phase2_distributed)
add_test(NAME ThreadPool COMMAND test_thread_pool)

add_executable(test_metadata_store test_metadata_store.cpp)
target_link_libraries(test_metadata_store phase2_distributed)
add_test(NAME MetadataStore COMMAND test_metadata_store)

add_executable(test_block_store test_block_store.cpp)
target_link_libraries(test_block_store phase2_distributed)
add_test(NAME BlockStore COMMAND test_block_store)

add_executable(test_namenode test_namenode.cpp)
target_link_libraries(test_namenode phase2_distributed)
add_test(NAME NameNode COMMAND test_namenode)
set_tests_properties(NameNode PROPERTIES TIMEOUT 30)

add_executable(test_end_to_end test_end_to_end.cpp)
target_link_libraries(test_end_to_end phase2_distributed)
add_test(NAME EndToEnd COMMAND test_end_to_end)
set_tests_properties(EndToEnd PROPERTIES TIMEOUT 60)

add_executable(test_week8 test_week8_fault_tolerance.cpp)
target_link_libraries(test_week8 phase2_distributed)
add_test(NAME Week8_FaultTolerance COMMAND test_week8)
set_tests_properties(Week8_FaultTolerance PROPERTIES TIMEOUT 120)
