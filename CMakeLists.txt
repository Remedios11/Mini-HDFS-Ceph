cmake_minimum_required(VERSION 3.10)
project(mini_storage CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
add_compile_options(-Wall -Wextra -O2)

find_package(Protobuf REQUIRED)
find_package(ZLIB REQUIRED)

include_directories(
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/src/engine
    ${CMAKE_CURRENT_SOURCE_DIR}/src/namenode
    ${CMAKE_CURRENT_SOURCE_DIR}/src/datanode
    ${CMAKE_CURRENT_SOURCE_DIR}/src/net
    ${CMAKE_CURRENT_SOURCE_DIR}/src/client
    ${CMAKE_CURRENT_SOURCE_DIR}/src/common
    ${CMAKE_CURRENT_SOURCE_DIR}/proto
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${Protobuf_INCLUDE_DIRS}
)

# Proto 生成文件
set(PROTO_SRCS
    proto/storage.pb.cc
    proto/namenode.pb.cc
    proto/datanode.pb.cc
    proto/raft.pb.cc
)

# Phase 1 引擎
set(ENGINE_SRCS
    src/engine/coding.cpp
    src/engine/db_write_batch.cpp
    src/engine/db.cpp
    src/engine/log_reader.cpp
    src/engine/log_writer.cpp
    src/engine/Lrucache.cpp
    src/engine/MemTable.cpp
    src/engine/SSTableBuilder.cpp
    src/engine/SSTableReader.cpp
)

# 网络层
set(NET_SRCS
    src/net/net_thread_pool.cpp
    src/net/net_channel.cpp
    src/net/net_event_loop.cpp
    src/net/net_tcp_connection.cpp
    src/net/net_tcp_server.cpp
)

# NameNode
set(NAMENODE_SRCS
    src/namenode/namenode_metadata_store.cpp
    src/namenode/namenode_block_manager.cpp
    src/namenode/namenode_datanode_manager.cpp
    src/namenode/namenode_edit_log.cpp
    src/namenode/namenode_namenode_server.cpp
    src/namenode/namenode_replication_monitor.cpp
    src/namenode/namenode_block_replicator.cpp
    src/namenode/namenode_consistency_checker.cpp
    src/namenode/namenode_fault_detector.cpp
)

# DataNode
set(DATANODE_SRCS
    src/datanode/datanode_block_store.cpp
    src/datanode/datanode_pipeline_handler.cpp
    src/datanode/datanode_datanode_client.cpp
    src/datanode/datanode_datanode_server.cpp
)

# Raft (Phase 3)
set(RAFT_SRCS
    src/raft/raft_node.cpp
    src/raft/raft_rpc.cpp
)

# HA NameNode (Phase 3)
set(HA_NAMENODE_SRCS
    src/namenode/namenode_ha_server.cpp
)

# 静态库
add_library(phase1_engine STATIC ${ENGINE_SRCS})
target_link_libraries(phase1_engine ZLIB::ZLIB pthread stdc++fs)

add_library(phase2_distributed STATIC
    ${PROTO_SRCS} ${NET_SRCS} ${NAMENODE_SRCS} ${DATANODE_SRCS}
    src/client/client_storage_client.cpp
)
target_link_libraries(phase2_distributed
    ${Protobuf_LIBRARIES} ZLIB::ZLIB pthread stdc++fs)

# Phase 3 Raft library
add_library(phase3_raft STATIC
    ${RAFT_SRCS}
)
target_link_libraries(phase3_raft
    ${Protobuf_LIBRARIES} pthread)
target_include_directories(phase3_raft PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src
    ${CMAKE_CURRENT_SOURCE_DIR}/proto)

# Phase 3 HA NameNode library (depends on raft + phase2)
add_library(phase3_ha_namenode STATIC
    ${HA_NAMENODE_SRCS}
)
target_link_libraries(phase3_ha_namenode
    phase3_raft phase2_distributed)

# 可执行文件
add_executable(namenode_server src/main_namenode.cpp)
target_link_libraries(namenode_server phase2_distributed)

add_executable(datanode_server src/main_datanode.cpp)
target_link_libraries(datanode_server phase2_distributed)

# 测试
enable_testing()

add_executable(test_phase1 tests/test_phase1.cpp)
target_link_libraries(test_phase1 phase1_engine)
add_test(NAME Phase1_Engine COMMAND test_phase1)

add_executable(test_thread_pool tests/test_thread_pool.cpp)
target_link_libraries(test_thread_pool phase2_distributed)
add_test(NAME ThreadPool COMMAND test_thread_pool)

add_executable(test_metadata_store tests/test_metadata_store.cpp)
target_link_libraries(test_metadata_store phase2_distributed)
add_test(NAME MetadataStore COMMAND test_metadata_store)

add_executable(test_block_store tests/test_block_store.cpp)
target_link_libraries(test_block_store phase2_distributed)
add_test(NAME BlockStore COMMAND test_block_store)

add_executable(test_namenode tests/test_namenode.cpp)
target_link_libraries(test_namenode phase2_distributed)
add_test(NAME NameNode COMMAND test_namenode)
set_tests_properties(NameNode PROPERTIES TIMEOUT 30)

add_executable(test_end_to_end tests/test_end_to_end.cpp)
target_link_libraries(test_end_to_end phase2_distributed)
add_test(NAME EndToEnd COMMAND test_end_to_end)
set_tests_properties(EndToEnd PROPERTIES TIMEOUT 60)

add_executable(test_week8 tests/test_week8_fault_tolerance.cpp)
target_link_libraries(test_week8 phase2_distributed)
add_test(NAME Week8_FaultTolerance COMMAND test_week8)
set_tests_properties(Week8_FaultTolerance PROPERTIES TIMEOUT 120)

# Phase 3: HA NameNode executable
add_executable(namenode_ha
    src/main_namenode_ha.cpp
)
target_link_libraries(namenode_ha phase3_ha_namenode phase3_raft phase2_distributed)

# Phase 3: Raft & HA tests
add_executable(test_phase3_raft tests/test_phase3_raft.cpp)
target_link_libraries(test_phase3_raft phase3_ha_namenode phase3_raft phase2_distributed)
add_test(NAME Phase3_RaftHA COMMAND test_phase3_raft)
set_tests_properties(Phase3_RaftHA PROPERTIES TIMEOUT 180)
