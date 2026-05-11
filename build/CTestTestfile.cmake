# CMake generated Testfile for 
# Source directory: /home/remedios/mini_storage
# Build directory: /home/remedios/mini_storage/build
# 
# This file includes the relevant testing commands required for 
# testing this directory and lists subdirectories to be tested as well.
add_test(Phase1_Engine "/home/remedios/mini_storage/build/test_phase1")
set_tests_properties(Phase1_Engine PROPERTIES  _BACKTRACE_TRIPLES "/home/remedios/mini_storage/CMakeLists.txt;84;add_test;/home/remedios/mini_storage/CMakeLists.txt;0;")
add_test(ThreadPool "/home/remedios/mini_storage/build/test_thread_pool")
set_tests_properties(ThreadPool PROPERTIES  _BACKTRACE_TRIPLES "/home/remedios/mini_storage/CMakeLists.txt;88;add_test;/home/remedios/mini_storage/CMakeLists.txt;0;")
add_test(MetadataStore "/home/remedios/mini_storage/build/test_metadata_store")
set_tests_properties(MetadataStore PROPERTIES  _BACKTRACE_TRIPLES "/home/remedios/mini_storage/CMakeLists.txt;92;add_test;/home/remedios/mini_storage/CMakeLists.txt;0;")
add_test(BlockStore "/home/remedios/mini_storage/build/test_block_store")
set_tests_properties(BlockStore PROPERTIES  _BACKTRACE_TRIPLES "/home/remedios/mini_storage/CMakeLists.txt;96;add_test;/home/remedios/mini_storage/CMakeLists.txt;0;")
add_test(NameNode "/home/remedios/mini_storage/build/test_namenode")
set_tests_properties(NameNode PROPERTIES  TIMEOUT "30" _BACKTRACE_TRIPLES "/home/remedios/mini_storage/CMakeLists.txt;100;add_test;/home/remedios/mini_storage/CMakeLists.txt;0;")
add_test(EndToEnd "/home/remedios/mini_storage/build/test_end_to_end")
set_tests_properties(EndToEnd PROPERTIES  TIMEOUT "60" _BACKTRACE_TRIPLES "/home/remedios/mini_storage/CMakeLists.txt;105;add_test;/home/remedios/mini_storage/CMakeLists.txt;0;")
add_test(Week8_FaultTolerance "/home/remedios/mini_storage/build/test_week8")
set_tests_properties(Week8_FaultTolerance PROPERTIES  TIMEOUT "120" _BACKTRACE_TRIPLES "/home/remedios/mini_storage/CMakeLists.txt;110;add_test;/home/remedios/mini_storage/CMakeLists.txt;0;")
