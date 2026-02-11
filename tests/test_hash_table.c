/**
 * Hash Table Unit Tests
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "hash_table.h"
#include "memory_pool.h"

#define TEST_POOL_SIZE (64 * 1024 * 1024)  // 64MB

static memory_pool_t *g_pool = NULL;

static void setup(void) {
    g_pool = memory_pool_create(TEST_POOL_SIZE, false);
    assert(g_pool != NULL);
}

static void teardown(void) {
    if (g_pool) {
        memory_pool_destroy(g_pool);
        g_pool = NULL;
    }
}

static void test_create_destroy(void) {
    printf("Test: create and destroy hash table... ");
    
    hash_table_t *table = hash_table_create(1024, g_pool);
    assert(table != NULL);
    
    hash_table_destroy(table);
    printf("PASSED\n");
}

static void test_put_get(void) {
    printf("Test: basic put and get... ");
    
    hash_table_t *table = hash_table_create(1024, g_pool);
    assert(table != NULL);
    
    // 插入键值对
    const char *key = "test_key";
    const char *value = "test_value_12345";
    
    kv_result_t result = hash_table_put(table, key, strlen(key), value, strlen(value));
    assert(result.status == KV_OK);
    assert(result.version == 1);
    
    // 读取键值对
    result = hash_table_get(table, key, strlen(key));
    assert(result.status == KV_OK);
    assert(result.value_len == strlen(value));
    assert(memcmp(result.value, value, result.value_len) == 0);
    
    hash_table_destroy(table);
    printf("PASSED\n");
}

static void test_update(void) {
    printf("Test: update existing key... ");
    
    hash_table_t *table = hash_table_create(1024, g_pool);
    assert(table != NULL);
    
    const char *key = "update_key";
    const char *value1 = "first_value";
    const char *value2 = "second_value_updated";
    
    // 首次插入
    kv_result_t result = hash_table_put(table, key, strlen(key), value1, strlen(value1));
    assert(result.status == KV_OK);
    assert(result.version == 1);
    
    // 更新
    result = hash_table_put(table, key, strlen(key), value2, strlen(value2));
    assert(result.status == KV_OK);
    assert(result.version == 2);
    
    // 验证更新后的值
    result = hash_table_get(table, key, strlen(key));
    assert(result.status == KV_OK);
    assert(result.value_len == strlen(value2));
    assert(memcmp(result.value, value2, result.value_len) == 0);
    
    hash_table_destroy(table);
    printf("PASSED\n");
}

static void test_delete(void) {
    printf("Test: delete key... ");
    
    hash_table_t *table = hash_table_create(1024, g_pool);
    assert(table != NULL);
    
    const char *key = "delete_key";
    const char *value = "to_be_deleted";
    
    // 插入
    kv_result_t result = hash_table_put(table, key, strlen(key), value, strlen(value));
    assert(result.status == KV_OK);
    
    // 删除
    result = hash_table_delete(table, key, strlen(key));
    assert(result.status == KV_OK);
    
    // 验证已删除
    result = hash_table_get(table, key, strlen(key));
    assert(result.status == KV_ERR_NOT_FOUND);
    
    // 重复删除应该返回 NOT_FOUND
    result = hash_table_delete(table, key, strlen(key));
    assert(result.status == KV_ERR_NOT_FOUND);
    
    hash_table_destroy(table);
    printf("PASSED\n");
}

static void test_cas(void) {
    printf("Test: compare and swap... ");
    
    hash_table_t *table = hash_table_create(1024, g_pool);
    assert(table != NULL);
    
    const char *key = "cas_key";
    const char *value1 = "initial_value";
    const char *value2 = "updated_by_cas";
    const char *value3 = "should_fail";
    
    // 插入初始值
    kv_result_t result = hash_table_put(table, key, strlen(key), value1, strlen(value1));
    assert(result.status == KV_OK);
    uint64_t version = result.version;
    
    // CAS 成功（版本匹配）
    result = hash_table_cas(table, key, strlen(key), version, value2, strlen(value2));
    assert(result.status == KV_OK);
    assert(result.version == version + 1);
    
    // CAS 失败（版本不匹配）
    result = hash_table_cas(table, key, strlen(key), version, value3, strlen(value3));
    assert(result.status == KV_ERR_CAS_FAILED);
    
    // 验证值没有被错误更新
    result = hash_table_get(table, key, strlen(key));
    assert(result.status == KV_OK);
    assert(memcmp(result.value, value2, strlen(value2)) == 0);
    
    hash_table_destroy(table);
    printf("PASSED\n");
}

static void test_collision(void) {
    printf("Test: hash collision handling... ");
    
    // 使用小桶数来增加碰撞概率
    hash_table_t *table = hash_table_create(16, g_pool);
    assert(table != NULL);
    
    char key[64];
    char value[128];
    
    // 插入大量键值对
    for (int i = 0; i < 1000; i++) {
        snprintf(key, sizeof(key), "collision_key_%d", i);
        snprintf(value, sizeof(value), "collision_value_%d", i);
        
        kv_result_t result = hash_table_put(table, key, strlen(key), value, strlen(value));
        assert(result.status == KV_OK);
    }
    
    // 验证所有值都能正确读取
    for (int i = 0; i < 1000; i++) {
        snprintf(key, sizeof(key), "collision_key_%d", i);
        snprintf(value, sizeof(value), "collision_value_%d", i);
        
        kv_result_t result = hash_table_get(table, key, strlen(key));
        assert(result.status == KV_OK);
        assert(result.value_len == strlen(value));
        assert(memcmp(result.value, value, result.value_len) == 0);
    }
    
    hash_table_destroy(table);
    printf("PASSED\n");
}

static void test_not_found(void) {
    printf("Test: key not found... ");
    
    hash_table_t *table = hash_table_create(1024, g_pool);
    assert(table != NULL);
    
    const char *key = "nonexistent_key";
    
    kv_result_t result = hash_table_get(table, key, strlen(key));
    assert(result.status == KV_ERR_NOT_FOUND);
    
    hash_table_destroy(table);
    printf("PASSED\n");
}

static void test_large_values(void) {
    printf("Test: large values... ");
    
    hash_table_t *table = hash_table_create(1024, g_pool);
    assert(table != NULL);
    
    const char *key = "large_value_key";
    
    // 创建 100KB 的值
    size_t large_size = 100 * 1024;
    char *large_value = (char *)malloc(large_size);
    assert(large_value != NULL);
    
    for (size_t i = 0; i < large_size; i++) {
        large_value[i] = 'A' + (i % 26);
    }
    
    // 插入大值
    kv_result_t result = hash_table_put(table, key, strlen(key), large_value, large_size);
    assert(result.status == KV_OK);
    
    // 读取并验证
    result = hash_table_get(table, key, strlen(key));
    assert(result.status == KV_OK);
    assert(result.value_len == large_size);
    assert(memcmp(result.value, large_value, large_size) == 0);
    
    free(large_value);
    hash_table_destroy(table);
    printf("PASSED\n");
}

static void test_stats(void) {
    printf("Test: statistics... ");
    
    hash_table_t *table = hash_table_create(1024, g_pool);
    assert(table != NULL);
    
    char key[64];
    char value[128];
    
    // 执行各种操作
    for (int i = 0; i < 100; i++) {
        snprintf(key, sizeof(key), "stats_key_%d", i);
        snprintf(value, sizeof(value), "stats_value_%d", i);
        hash_table_put(table, key, strlen(key), value, strlen(value));
    }
    
    for (int i = 0; i < 50; i++) {
        snprintf(key, sizeof(key), "stats_key_%d", i);
        hash_table_get(table, key, strlen(key));
    }
    
    for (int i = 0; i < 20; i++) {
        snprintf(key, sizeof(key), "stats_key_%d", i);
        hash_table_delete(table, key, strlen(key));
    }
    
    // 打印统计信息
    printf("\n");
    hash_table_print_stats(table);
    
    hash_table_destroy(table);
    printf("PASSED\n");
}

int main(void) {
    printf("\n=== Hash Table Tests ===\n\n");
    
    setup();
    
    test_create_destroy();
    
    teardown();
    setup();
    test_put_get();
    
    teardown();
    setup();
    test_update();
    
    teardown();
    setup();
    test_delete();
    
    teardown();
    setup();
    test_cas();
    
    teardown();
    setup();
    test_collision();
    
    teardown();
    setup();
    test_not_found();
    
    teardown();
    setup();
    test_large_values();
    
    teardown();
    setup();
    test_stats();
    
    teardown();
    
    printf("\nAll tests passed!\n\n");
    return 0;
}