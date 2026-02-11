cmake_minimum_required(VERSION 3.10)
project(rdma_kv C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -O2 -g")

# 查找 RDMA 库
find_library(IBVERBS_LIB ibverbs)
find_library(RDMACM_LIB rdmacm)

if(NOT IBVERBS_LIB OR NOT RDMACM_LIB)
    message(WARNING "RDMA libraries not found. Building with mock implementation.")
    add_definitions(-DRDMA_MOCK)
endif()

# 头文件目录
include_directories(${CMAKE_SOURCE_DIR}/include)

# 源文件
set(COMMON_SOURCES
    src/memory_pool.c
    src/hash_table.c
    src/rdma_common.c
    src/kv_store.c
)

# 静态库
add_library(rdma_kv_static STATIC ${COMMON_SOURCES})
set_target_properties(rdma_kv_static PROPERTIES OUTPUT_NAME rdma_kv)

# 动态库
add_library(rdma_kv_shared SHARED ${COMMON_SOURCES})
set_target_properties(rdma_kv_shared PROPERTIES OUTPUT_NAME rdma_kv)

# 链接 RDMA 库
if(IBVERBS_LIB AND RDMACM_LIB)
    target_link_libraries(rdma_kv_static ${IBVERBS_LIB} ${RDMACM_LIB} pthread)
    target_link_libraries(rdma_kv_shared ${IBVERBS_LIB} ${RDMACM_LIB} pthread)
else()
    target_link_libraries(rdma_kv_static pthread)
    target_link_libraries(rdma_kv_shared pthread)
endif()

# 服务端程序
add_executable(kv_server examples/server_main.c)
target_link_libraries(kv_server rdma_kv_static)

# 客户端程序
add_executable(kv_client examples/client_main.c)
target_link_libraries(kv_client rdma_kv_static)

# 输出目录
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# 安装规则
install(TARGETS rdma_kv_static rdma_kv_shared kv_server kv_client
    RUNTIME DESTINATION bin
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
)

install(DIRECTORY include/ DESTINATION include/rdma_kv)

# 测试目标
enable_testing()

# 添加简单的单元测试
add_executable(test_memory_pool tests/test_memory_pool.c)
target_link_libraries(test_memory_pool rdma_kv_static)

add_executable(test_hash_table tests/test_hash_table.c)
target_link_libraries(test_hash_table rdma_kv_static)

add_test(NAME test_memory_pool COMMAND test_memory_pool)
add_test(NAME test_hash_table COMMAND test_hash_table)