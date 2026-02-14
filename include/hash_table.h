#ifndef RDMA_KV_HASH_TABLE_H
#define RDMA_KV_HASH_TABLE_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <pthread.h>
#include "memory_pool.h"

#ifdef __cplusplus
extern "C" {
#endif

// 哈希表配置
#define HASH_TABLE_INITIAL_SIZE    65536
#define HASH_TABLE_LOAD_FACTOR     0.75
#define MAX_KEY_LENGTH             256
#define MAX_VALUE_LENGTH           (1 << 20)  // 1MB

// 哈希表条目状态
typedef enum {
    ENTRY_EMPTY = 0,
    ENTRY_OCCUPIED,
    ENTRY_DELETED
} entry_state_t;

// 哈希表条目（用于 RDMA 远程访问）
typedef struct hash_entry {
    uint64_t version;            // 乐观锁版本号
    uint32_t key_len;
    uint32_t value_len;
    uint64_t value_offset;       // 值在内存池中的偏移
    uint8_t  state;              // entry_state_t
    uint8_t  padding[7];         // 对齐填充
    char     key[MAX_KEY_LENGTH]; // 内联存储 key
} __attribute__((packed, aligned(64))) hash_entry_t;

// 哈希桶（支持链式处理冲突）
typedef struct hash_bucket {
    pthread_rwlock_t lock;       // 读写锁
    uint32_t count;              // 当前条目数
    uint32_t capacity;           // 桶容量
    hash_entry_t *entries;       // 条目数组
} hash_bucket_t;

// 哈希表
typedef struct hash_table {
    hash_bucket_t *buckets;      // 桶数组
    size_t num_buckets;          // 桶数量
    size_t num_entries;          // 总条目数
    memory_pool_t *mem_pool;     // 内存池引用
    
    // 统计信息
    uint64_t get_count;
    uint64_t put_count;
    uint64_t delete_count;
    uint64_t cas_count;
    uint64_t collision_count;
    
    pthread_mutex_t resize_lock; // 扩容锁
} hash_table_t;

// KV 操作结果
typedef struct kv_result {
    int status;                  // 0: 成功, 负数: 错误码
    uint64_t version;            // 当前版本
    void *value;                 // 值指针
    size_t value_len;            // 值长度
} kv_result_t;

// 错误码
#define KV_OK                0
#define KV_ERR_NOT_FOUND    -1
#define KV_ERR_EXISTS       -2
#define KV_ERR_CAS_FAILED   -3
#define KV_ERR_KEY_TOO_LONG -4
#define KV_ERR_VALUE_TOO_LONG -5
#define KV_ERR_NO_MEMORY    -6
#define KV_ERR_INTERNAL     -7

// 创建哈希表
hash_table_t* hash_table_create(size_t initial_size, memory_pool_t *mem_pool);

// 销毁哈希表
void hash_table_destroy(hash_table_t *table);

// GET 操作
// @param key: 键
// @param key_len: 键长度
// @param result: 输出结果
kv_result_t hash_table_get(hash_table_t *table, const char *key, size_t key_len);

// PUT 操作
// @param key: 键
// @param key_len: 键长度  
// @param value: 值
// @param value_len: 值长度
// @param version: 输出新版本号
kv_result_t hash_table_put(hash_table_t *table, const char *key, size_t key_len,
                           const void *value, size_t value_len);

// DELETE 操作
kv_result_t hash_table_delete(hash_table_t *table, const char *key, size_t key_len);

// CAS 操作（Compare And Swap）
// @param expected_version: 期望的版本号
// @param new_value: 新值
// @param new_value_len: 新值长度
kv_result_t hash_table_cas(hash_table_t *table, const char *key, size_t key_len,
                           uint64_t expected_version,
                           const void *new_value, size_t new_value_len);

// 获取条目的远程地址信息（用于 RDMA Read）
int hash_table_get_entry_addr(hash_table_t *table, const char *key, size_t key_len,
                              uint64_t *addr, size_t *size);

// PUT 操作（使用预分配的内存，用于大数据 RDMA Write 场景）
// @param value_ptr: 已分配的值存储地址（客户端已通过 RDMA Write 写入数据）
// @param value_offset: 值在内存池中的偏移
kv_result_t hash_table_put_with_allocated(hash_table_t *table, 
                                          const char *key, size_t key_len,
                                          void *value_ptr, size_t value_len,
                                          uint64_t value_offset);

// 哈希函数
uint64_t hash_key(const char *key, size_t len);

// 打印统计信息
void hash_table_print_stats(hash_table_t *table);

#ifdef __cplusplus
}
#endif

#endif // RDMA_KV_HASH_TABLE_H