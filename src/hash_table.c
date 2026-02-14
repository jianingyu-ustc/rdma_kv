#include "hash_table.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// MurmurHash3 - 高性能哈希函数
uint64_t hash_key(const char *key, size_t len) {
    const uint64_t seed = 0xc70f6907UL;
    const uint64_t m = 0xc6a4a7935bd1e995UL;
    const int r = 47;
    
    uint64_t h = seed ^ (len * m);
    
    const uint64_t *data = (const uint64_t *)key;
    const uint64_t *end = data + (len / 8);
    
    while (data != end) {
        uint64_t k = *data++;
        
        k *= m;
        k ^= k >> r;
        k *= m;
        
        h ^= k;
        h *= m;
    }
    
    const unsigned char *data2 = (const unsigned char *)data;
    
    switch (len & 7) {
        case 7: h ^= (uint64_t)(data2[6]) << 48; // fallthrough
        case 6: h ^= (uint64_t)(data2[5]) << 40; // fallthrough
        case 5: h ^= (uint64_t)(data2[4]) << 32; // fallthrough
        case 4: h ^= (uint64_t)(data2[3]) << 24; // fallthrough
        case 3: h ^= (uint64_t)(data2[2]) << 16; // fallthrough
        case 2: h ^= (uint64_t)(data2[1]) << 8;  // fallthrough
        case 1: h ^= (uint64_t)(data2[0]);
                h *= m;
    }
    
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    
    return h;
}

// 创建哈希表
hash_table_t* hash_table_create(size_t initial_size, memory_pool_t *mem_pool) {
    hash_table_t *table = (hash_table_t *)calloc(1, sizeof(hash_table_t));
    if (!table) {
        return NULL;
    }
    
    table->num_buckets = initial_size > 0 ? initial_size : HASH_TABLE_INITIAL_SIZE;
    table->mem_pool = mem_pool;
    
    // 分配桶数组
    table->buckets = (hash_bucket_t *)calloc(table->num_buckets, sizeof(hash_bucket_t));
    if (!table->buckets) {
        free(table);
        return NULL;
    }
    
    // 初始化每个桶
    for (size_t i = 0; i < table->num_buckets; i++) {
        pthread_rwlock_init(&table->buckets[i].lock, NULL);
        table->buckets[i].capacity = 4;  // 初始每桶4个条目
        table->buckets[i].count = 0;
        
        // 从内存池分配条目数组
        if (mem_pool) {
            table->buckets[i].entries = (hash_entry_t *)memory_pool_alloc(
                mem_pool, table->buckets[i].capacity * sizeof(hash_entry_t));
        } else {
            table->buckets[i].entries = (hash_entry_t *)calloc(
                table->buckets[i].capacity, sizeof(hash_entry_t));
        }
        
        if (!table->buckets[i].entries) {
            // 清理已分配的资源
            for (size_t j = 0; j < i; j++) {
                pthread_rwlock_destroy(&table->buckets[j].lock);
                if (!mem_pool) {
                    free(table->buckets[j].entries);
                }
            }
            free(table->buckets);
            free(table);
            return NULL;
        }
        
        // 初始化条目状态
        for (uint32_t j = 0; j < table->buckets[i].capacity; j++) {
            table->buckets[i].entries[j].state = ENTRY_EMPTY;
        }
    }
    
    pthread_mutex_init(&table->resize_lock, NULL);
    
    return table;
}

// 销毁哈希表
void hash_table_destroy(hash_table_t *table) {
    if (!table) return;
    
    for (size_t i = 0; i < table->num_buckets; i++) {
        pthread_rwlock_destroy(&table->buckets[i].lock);
        if (!table->mem_pool) {
            free(table->buckets[i].entries);
        }
    }
    
    pthread_mutex_destroy(&table->resize_lock);
    free(table->buckets);
    free(table);
}

// 在桶中查找条目
static hash_entry_t* find_entry_in_bucket(hash_bucket_t *bucket, 
                                          const char *key, size_t key_len) {
    for (uint32_t i = 0; i < bucket->count; i++) {
        hash_entry_t *entry = &bucket->entries[i];
        if (entry->state == ENTRY_OCCUPIED &&
            entry->key_len == key_len &&
            memcmp(entry->key, key, key_len) == 0) {
            return entry;
        }
    }
    return NULL;
}

// 扩展桶容量
static int expand_bucket(hash_bucket_t *bucket, memory_pool_t *mem_pool) {
    uint32_t new_capacity = bucket->capacity * 2;
    hash_entry_t *new_entries;
    
    if (mem_pool) {
        new_entries = (hash_entry_t *)memory_pool_alloc(
            mem_pool, new_capacity * sizeof(hash_entry_t));
    } else {
        new_entries = (hash_entry_t *)calloc(new_capacity, sizeof(hash_entry_t));
    }
    
    if (!new_entries) {
        return -1;
    }
    
    // 复制旧条目
    memcpy(new_entries, bucket->entries, bucket->count * sizeof(hash_entry_t));
    
    // 初始化新条目
    for (uint32_t i = bucket->count; i < new_capacity; i++) {
        new_entries[i].state = ENTRY_EMPTY;
    }
    
    // 释放旧数组（如果不使用内存池）
    if (!mem_pool) {
        free(bucket->entries);
    }
    
    bucket->entries = new_entries;
    bucket->capacity = new_capacity;
    
    return 0;
}

// GET 操作
kv_result_t hash_table_get(hash_table_t *table, const char *key, size_t key_len) {
    kv_result_t result = {0};
    
    if (!table || !key || key_len == 0 || key_len > MAX_KEY_LENGTH) {
        result.status = KV_ERR_KEY_TOO_LONG;
        return result;
    }
    
    uint64_t hash = hash_key(key, key_len);
    size_t bucket_idx = hash % table->num_buckets;
    hash_bucket_t *bucket = &table->buckets[bucket_idx];
    
    pthread_rwlock_rdlock(&bucket->lock);
    
    hash_entry_t *entry = find_entry_in_bucket(bucket, key, key_len);
    if (entry) {
        result.status = KV_OK;
        result.version = entry->version;
        result.value_len = entry->value_len;
        
        // 计算值的地址
        if (table->mem_pool) {
            result.value = (char *)memory_pool_get_base(table->mem_pool) + entry->value_offset;
        }
        
        __sync_fetch_and_add(&table->get_count, 1);
    } else {
        result.status = KV_ERR_NOT_FOUND;
    }
    
    pthread_rwlock_unlock(&bucket->lock);
    
    return result;
}

// PUT 操作
kv_result_t hash_table_put(hash_table_t *table, const char *key, size_t key_len,
                           const void *value, size_t value_len) {
    kv_result_t result = {0};
    
    if (!table || !key || key_len == 0) {
        result.status = KV_ERR_INTERNAL;
        return result;
    }
    if (key_len > MAX_KEY_LENGTH) {
        result.status = KV_ERR_KEY_TOO_LONG;
        return result;
    }
    if (value_len > MAX_VALUE_LENGTH) {
        result.status = KV_ERR_VALUE_TOO_LONG;
        return result;
    }
    
    uint64_t hash = hash_key(key, key_len);
    size_t bucket_idx = hash % table->num_buckets;
    hash_bucket_t *bucket = &table->buckets[bucket_idx];
    
    pthread_rwlock_wrlock(&bucket->lock);
    
    // 查找现有条目
    hash_entry_t *entry = find_entry_in_bucket(bucket, key, key_len);
    
    if (entry) {
        // 更新现有条目
        void *new_value_buf = memory_pool_alloc(table->mem_pool, value_len);
        if (!new_value_buf) {
            result.status = KV_ERR_NO_MEMORY;
            pthread_rwlock_unlock(&bucket->lock);
            return result;
        }
        
        memcpy(new_value_buf, value, value_len);
        entry->value_offset = (uint64_t)new_value_buf - (uint64_t)memory_pool_get_base(table->mem_pool);
        entry->value_len = value_len;
        entry->version++;
        
        result.version = entry->version;
    } else {
        // 插入新条目
        if (bucket->count >= bucket->capacity) {
            if (expand_bucket(bucket, table->mem_pool) != 0) {
                result.status = KV_ERR_NO_MEMORY;
                pthread_rwlock_unlock(&bucket->lock);
                return result;
            }
            __sync_fetch_and_add(&table->collision_count, 1);
        }
        
        // 分配值存储空间
        void *value_buf = memory_pool_alloc(table->mem_pool, value_len);
        if (!value_buf) {
            result.status = KV_ERR_NO_MEMORY;
            pthread_rwlock_unlock(&bucket->lock);
            return result;
        }
        
        memcpy(value_buf, value, value_len);
        
        // 填充新条目
        entry = &bucket->entries[bucket->count];
        entry->key_len = key_len;
        memcpy(entry->key, key, key_len);
        entry->value_offset = (uint64_t)value_buf - (uint64_t)memory_pool_get_base(table->mem_pool);
        entry->value_len = value_len;
        entry->version = 1;
        entry->state = ENTRY_OCCUPIED;
        
        bucket->count++;
        __sync_fetch_and_add(&table->num_entries, 1);
        
        result.version = 1;
    }
    
    result.status = KV_OK;
    __sync_fetch_and_add(&table->put_count, 1);
    
    pthread_rwlock_unlock(&bucket->lock);
    
    return result;
}

// PUT 操作（使用预分配的内存，用于大数据 RDMA Write 场景）
kv_result_t hash_table_put_with_allocated(hash_table_t *table, 
                                          const char *key, size_t key_len,
                                          void *value_ptr, size_t value_len,
                                          uint64_t value_offset) {
    kv_result_t result = {0};
    
    if (!table || !key || key_len == 0) {
        result.status = KV_ERR_INTERNAL;
        return result;
    }
    if (key_len > MAX_KEY_LENGTH) {
        result.status = KV_ERR_KEY_TOO_LONG;
        return result;
    }
    if (value_len > MAX_VALUE_LENGTH) {
        result.status = KV_ERR_VALUE_TOO_LONG;
        return result;
    }
    
    uint64_t hash = hash_key(key, key_len);
    size_t bucket_idx = hash % table->num_buckets;
    hash_bucket_t *bucket = &table->buckets[bucket_idx];
    
    pthread_rwlock_wrlock(&bucket->lock);
    
    // 查找现有条目
    hash_entry_t *entry = find_entry_in_bucket(bucket, key, key_len);
    
    if (entry) {
        // 更新现有条目（旧值内存由调用者处理或泄露，简化实现）
        entry->value_offset = value_offset;
        entry->value_len = value_len;
        entry->version++;
        
        result.version = entry->version;
    } else {
        // 插入新条目
        if (bucket->count >= bucket->capacity) {
            if (expand_bucket(bucket, table->mem_pool) != 0) {
                // 分配的内存需要释放
                memory_pool_free(table->mem_pool, value_ptr);
                result.status = KV_ERR_NO_MEMORY;
                pthread_rwlock_unlock(&bucket->lock);
                return result;
            }
            __sync_fetch_and_add(&table->collision_count, 1);
        }
        
        // 填充新条目
        entry = &bucket->entries[bucket->count];
        entry->key_len = key_len;
        memcpy(entry->key, key, key_len);
        entry->value_offset = value_offset;
        entry->value_len = value_len;
        entry->version = 1;
        entry->state = ENTRY_OCCUPIED;
        
        bucket->count++;
        __sync_fetch_and_add(&table->num_entries, 1);
        
        result.version = 1;
    }
    
    result.status = KV_OK;
    __sync_fetch_and_add(&table->put_count, 1);
    
    pthread_rwlock_unlock(&bucket->lock);
    
    return result;
}

// DELETE 操作
kv_result_t hash_table_delete(hash_table_t *table, const char *key, size_t key_len) {
    kv_result_t result = {0};
    
    if (!table || !key || key_len == 0 || key_len > MAX_KEY_LENGTH) {
        result.status = KV_ERR_KEY_TOO_LONG;
        return result;
    }
    
    uint64_t hash = hash_key(key, key_len);
    size_t bucket_idx = hash % table->num_buckets;
    hash_bucket_t *bucket = &table->buckets[bucket_idx];
    
    pthread_rwlock_wrlock(&bucket->lock);
    
    hash_entry_t *entry = find_entry_in_bucket(bucket, key, key_len);
    if (entry) {
        entry->state = ENTRY_DELETED;
        result.status = KV_OK;
        result.version = entry->version;
        __sync_fetch_and_add(&table->delete_count, 1);
        __sync_fetch_and_sub(&table->num_entries, 1);
    } else {
        result.status = KV_ERR_NOT_FOUND;
    }
    
    pthread_rwlock_unlock(&bucket->lock);
    
    return result;
}

// CAS 操作
kv_result_t hash_table_cas(hash_table_t *table, const char *key, size_t key_len,
                           uint64_t expected_version,
                           const void *new_value, size_t new_value_len) {
    kv_result_t result = {0};
    
    if (!table || !key || key_len == 0 || key_len > MAX_KEY_LENGTH) {
        result.status = KV_ERR_KEY_TOO_LONG;
        return result;
    }
    if (new_value_len > MAX_VALUE_LENGTH) {
        result.status = KV_ERR_VALUE_TOO_LONG;
        return result;
    }
    
    uint64_t hash = hash_key(key, key_len);
    size_t bucket_idx = hash % table->num_buckets;
    hash_bucket_t *bucket = &table->buckets[bucket_idx];
    
    pthread_rwlock_wrlock(&bucket->lock);
    
    hash_entry_t *entry = find_entry_in_bucket(bucket, key, key_len);
    if (!entry) {
        result.status = KV_ERR_NOT_FOUND;
        pthread_rwlock_unlock(&bucket->lock);
        return result;
    }
    
    // 比较版本号
    if (entry->version != expected_version) {
        result.status = KV_ERR_CAS_FAILED;
        result.version = entry->version;  // 返回当前版本
        pthread_rwlock_unlock(&bucket->lock);
        return result;
    }
    
    // 版本匹配，执行更新
    void *new_value_buf = memory_pool_alloc(table->mem_pool, new_value_len);
    if (!new_value_buf) {
        result.status = KV_ERR_NO_MEMORY;
        pthread_rwlock_unlock(&bucket->lock);
        return result;
    }
    
    memcpy(new_value_buf, new_value, new_value_len);
    entry->value_offset = (uint64_t)new_value_buf - (uint64_t)memory_pool_get_base(table->mem_pool);
    entry->value_len = new_value_len;
    entry->version++;
    
    result.status = KV_OK;
    result.version = entry->version;
    __sync_fetch_and_add(&table->cas_count, 1);
    
    pthread_rwlock_unlock(&bucket->lock);
    
    return result;
}

// 获取条目的远程地址信息
int hash_table_get_entry_addr(hash_table_t *table, const char *key, size_t key_len,
                              uint64_t *addr, size_t *size) {
    if (!table || !key || key_len == 0) {
        return -1;
    }
    
    uint64_t hash = hash_key(key, key_len);
    size_t bucket_idx = hash % table->num_buckets;
    hash_bucket_t *bucket = &table->buckets[bucket_idx];
    
    pthread_rwlock_rdlock(&bucket->lock);
    
    hash_entry_t *entry = find_entry_in_bucket(bucket, key, key_len);
    if (entry) {
        *addr = (uint64_t)memory_pool_get_base(table->mem_pool) + entry->value_offset;
        *size = entry->value_len;
        pthread_rwlock_unlock(&bucket->lock);
        return 0;
    }
    
    pthread_rwlock_unlock(&bucket->lock);
    return -1;
}

// 打印统计信息
void hash_table_print_stats(hash_table_t *table) {
    if (!table) return;
    
    printf("\n=== Hash Table Statistics ===\n");
    printf("Number of buckets: %zu\n", table->num_buckets);
    printf("Number of entries: %zu\n", table->num_entries);
    printf("Load factor: %.2f%%\n", 100.0 * table->num_entries / table->num_buckets);
    printf("GET operations: %lu\n", table->get_count);
    printf("PUT operations: %lu\n", table->put_count);
    printf("DELETE operations: %lu\n", table->delete_count);
    printf("CAS operations: %lu\n", table->cas_count);
    printf("Bucket expansions (collisions): %lu\n", table->collision_count);
    printf("=============================\n\n");
}