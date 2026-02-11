/**
 * Memory Pool Unit Tests
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include "memory_pool.h"

#define TEST_POOL_SIZE (64 * 1024 * 1024)  // 64MB

static void test_create_destroy(void) {
    printf("Test: create and destroy memory pool... ");
    
    memory_pool_t *pool = memory_pool_create(TEST_POOL_SIZE, false);
    assert(pool != NULL);
    assert(memory_pool_get_base(pool) != NULL);
    assert(memory_pool_get_size(pool) == TEST_POOL_SIZE);
    
    memory_pool_destroy(pool);
    printf("PASSED\n");
}

static void test_small_allocations(void) {
    printf("Test: small allocations from slab... ");
    
    memory_pool_t *pool = memory_pool_create(TEST_POOL_SIZE, false);
    assert(pool != NULL);
    
    // 测试不同大小的分配
    void *ptrs[100];
    size_t sizes[] = {32, 64, 100, 128, 200, 256, 400, 512, 1000, 2048};
    
    for (int i = 0; i < 100; i++) {
        size_t size = sizes[i % 10];
        ptrs[i] = memory_pool_alloc(pool, size);
        assert(ptrs[i] != NULL);
        
        // 写入数据验证可用性
        memset(ptrs[i], 0xAB, size);
    }
    
    // 释放部分内存
    for (int i = 0; i < 50; i++) {
        memory_pool_free(pool, ptrs[i]);
    }
    
    // 再次分配
    for (int i = 0; i < 50; i++) {
        size_t size = sizes[i % 10];
        ptrs[i] = memory_pool_alloc(pool, size);
        assert(ptrs[i] != NULL);
    }
    
    memory_pool_destroy(pool);
    printf("PASSED\n");
}

static void test_large_allocations(void) {
    printf("Test: large allocations... ");
    
    memory_pool_t *pool = memory_pool_create(TEST_POOL_SIZE, false);
    assert(pool != NULL);
    
    // 大块分配
    void *ptr1 = memory_pool_alloc(pool, 16 * 1024);  // 16KB
    assert(ptr1 != NULL);
    memset(ptr1, 0xCD, 16 * 1024);
    
    void *ptr2 = memory_pool_alloc(pool, 64 * 1024);  // 64KB
    assert(ptr2 != NULL);
    memset(ptr2, 0xEF, 64 * 1024);
    
    void *ptr3 = memory_pool_alloc(pool, 256 * 1024);  // 256KB
    assert(ptr3 != NULL);
    memset(ptr3, 0x12, 256 * 1024);
    
    memory_pool_destroy(pool);
    printf("PASSED\n");
}

static void test_concurrent_allocations(void) {
    printf("Test: concurrent allocations... ");
    
    memory_pool_t *pool = memory_pool_create(TEST_POOL_SIZE, false);
    assert(pool != NULL);
    
    // 模拟多线程分配（这里用串行模拟，真正的多线程测试需要 pthread）
    const int num_allocs = 1000;
    void *ptrs[num_allocs];
    
    for (int i = 0; i < num_allocs; i++) {
        size_t size = 64 + (i % 256);
        ptrs[i] = memory_pool_alloc(pool, size);
        assert(ptrs[i] != NULL);
    }
    
    for (int i = 0; i < num_allocs; i++) {
        memory_pool_free(pool, ptrs[i]);
    }
    
    memory_pool_destroy(pool);
    printf("PASSED\n");
}

static void test_stats(void) {
    printf("Test: statistics... ");
    
    memory_pool_t *pool = memory_pool_create(TEST_POOL_SIZE, false);
    assert(pool != NULL);
    
    // 进行一些分配和释放
    for (int i = 0; i < 100; i++) {
        void *ptr = memory_pool_alloc(pool, 128);
        assert(ptr != NULL);
        if (i % 2 == 0) {
            memory_pool_free(pool, ptr);
        }
    }
    
    // 打印统计信息
    printf("\n");
    memory_pool_print_stats(pool);
    
    memory_pool_destroy(pool);
    printf("PASSED\n");
}

int main(void) {
    printf("\n=== Memory Pool Tests ===\n\n");
    
    test_create_destroy();
    test_small_allocations();
    test_large_allocations();
    test_concurrent_allocations();
    test_stats();
    
    printf("\nAll tests passed!\n\n");
    return 0;
}