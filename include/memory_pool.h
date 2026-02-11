#ifndef RDMA_KV_MEMORY_POOL_H
#define RDMA_KV_MEMORY_POOL_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

// 内存块头部
typedef struct mem_block_header {
    struct mem_block_header *next;
    size_t size;
    bool in_use;
} mem_block_header_t;

// 空闲链表节点
typedef struct free_list_node {
    struct free_list_node *next;
} free_list_node_t;

// 内存池 Slab
typedef struct mem_slab {
    size_t block_size;           // 该 slab 管理的块大小
    size_t total_blocks;         // 总块数
    size_t free_blocks;          // 空闲块数
    free_list_node_t *free_list; // 空闲链表
    void *base;                  // 基地址
    pthread_spinlock_t lock;     // 自旋锁保护
} mem_slab_t;

// 预定义的 slab 大小类别
#define SLAB_SIZE_CLASSES  8
static const size_t SLAB_SIZES[SLAB_SIZE_CLASSES] = {
    64, 128, 256, 512, 1024, 2048, 4096, 8192
};

// 内存池
typedef struct memory_pool {
    void *base_addr;             // 大块内存基地址
    size_t total_size;           // 总大小
    bool use_huge_pages;         // 是否使用大页
    
    mem_slab_t slabs[SLAB_SIZE_CLASSES];  // 各大小类别的 slab
    
    // 大块分配（超过最大 slab 大小）
    void *large_block_base;
    size_t large_block_offset;
    pthread_mutex_t large_block_lock;
    
    // 统计信息
    uint64_t alloc_count;
    uint64_t free_count;
    uint64_t alloc_bytes;
} memory_pool_t;

// 创建内存池
// @param size: 内存池总大小
// @param use_huge_pages: 是否使用大页内存
memory_pool_t* memory_pool_create(size_t size, bool use_huge_pages);

// 销毁内存池
void memory_pool_destroy(memory_pool_t *pool);

// 分配内存
void* memory_pool_alloc(memory_pool_t *pool, size_t size);

// 释放内存
void memory_pool_free(memory_pool_t *pool, void *ptr);

// 获取内存池基地址（用于 RDMA 内存注册）
void* memory_pool_get_base(memory_pool_t *pool);

// 获取内存池大小
size_t memory_pool_get_size(memory_pool_t *pool);

// 打印统计信息
void memory_pool_print_stats(memory_pool_t *pool);

// 辅助函数：分配大页内存
void* alloc_huge_pages(size_t size);
void free_huge_pages(void *addr, size_t size);

#ifdef __cplusplus
}
#endif

#endif // RDMA_KV_MEMORY_POOL_H