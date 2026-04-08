cmake_minimum_required(VERSION 3.10)
project(MiniStorage)

# C++17标准
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# 编译选项
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Wextra -O2")

# 包含目录
include_directories(${PROJECT_SOURCE_DIR}/src)

# 源文件
file(GLOB_RECURSE SOURCES "src/*.cpp")

# 可执行文件
add_executable(mini_storage ${SOURCES})

# 如果需要链接pthread
find_package(Threads REQUIRED)
target_link_libraries(mini_storage Threads::Threads)

# 测试（可选，后续添加）
# enable_testing()
# add_subdirectory(test)
