#include "memory_pool.h"
#include "rdma_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <errno.h>

// 分配大页内存
void* alloc_huge_pages(size_t size) {
    // 对齐到大页边界
    size_t aligned_size = (size + HUGE_PAGE_SIZE - 1) & ~(HUGE_PAGE_SIZE - 1);
    
    void *addr = mmap(NULL, aligned_size,
                      PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
                      -1, 0);
    
    if (addr == MAP_FAILED) {
        // 如果大页分配失败，回退到普通内存
        fprintf(stderr, "Warning: Huge page allocation failed, falling back to normal pages\n");
        addr = mmap(NULL, aligned_size,
                    PROT_READ | PROT_WRITE,
                    MAP_PRIVATE | MAP_ANONYMOUS,
                    -1, 0);
        if (addr == MAP_FAILED) {
            fprintf(stderr, "Failed to allocate memory: %s\n", strerror(errno));
            return NULL;
        }
    }
    
    // 锁定内存，防止换出
    if (mlock(addr, aligned_size) != 0) {
        fprintf(stderr, "Warning: Failed to lock memory: %s\n", strerror(errno));
    }
    
    return addr;
}

// 释放大页内存
void free_huge_pages(void *addr, size_t size) {
    size_t aligned_size = (size + HUGE_PAGE_SIZE - 1) & ~(HUGE_PAGE_SIZE - 1);
    munlock(addr, aligned_size);
    munmap(addr, aligned_size);
}

// 初始化 slab
static int init_slab(mem_slab_t *slab, void *base, size_t block_size, size_t total_size) {
    slab->block_size = block_size;
    slab->base = base;
    slab->total_blocks = total_size / block_size;
    slab->free_blocks = slab->total_blocks;
    
    if (pthread_spin_init(&slab->lock, PTHREAD_PROCESS_PRIVATE) != 0) {
        return -1;
    }
    
    // 初始化空闲链表
    slab->free_list = NULL;
    char *ptr = (char *)base;
    for (size_t i = 0; i < slab->total_blocks; i++) {
        free_list_node_t *node = (free_list_node_t *)ptr;
        node->next = slab->free_list;
        slab->free_list = node;
        ptr += block_size;
    }
    
    return 0;
}

// 创建内存池
memory_pool_t* memory_pool_create(size_t size, bool use_huge_pages) {
    memory_pool_t *pool = (memory_pool_t *)calloc(1, sizeof(memory_pool_t));
    if (!pool) {
        return NULL;
    }
    
    pool->total_size = size;
    pool->use_huge_pages = use_huge_pages;
    
    // 分配大块内存
    if (use_huge_pages) {
        pool->base_addr = alloc_huge_pages(size);
    } else {
        pool->base_addr = aligned_alloc(4096, size);
        if (pool->base_addr) {
            memset(pool->base_addr, 0, size);
        }
    }
    
    if (!pool->base_addr) {
        free(pool);
        return NULL;
    }
    
    // 计算每个 slab 的大小分配
    // 总内存的 60% 给 slabs，40% 给大块分配
    size_t slab_total_size = size * 6 / 10;
    size_t slab_each_size = slab_total_size / SLAB_SIZE_CLASSES;
    
    char *slab_base = (char *)pool->base_addr;
    for (int i = 0; i < SLAB_SIZE_CLASSES; i++) {
        if (init_slab(&pool->slabs[i], slab_base, SLAB_SIZES[i], slab_each_size) != 0) {
            memory_pool_destroy(pool);
            return NULL;
        }
        slab_base += slab_each_size;
    }
    
    // 初始化大块分配区域
    pool->large_block_base = slab_base;
    pool->large_block_offset = 0;
    pthread_mutex_init(&pool->large_block_lock, NULL);
    
    return pool;
}

// 销毁内存池
void memory_pool_destroy(memory_pool_t *pool) {
    if (!pool) return;
    
    for (int i = 0; i < SLAB_SIZE_CLASSES; i++) {
        pthread_spin_destroy(&pool->slabs[i].lock);
    }
    pthread_mutex_destroy(&pool->large_block_lock);
    
    if (pool->base_addr) {
        if (pool->use_huge_pages) {
            free_huge_pages(pool->base_addr, pool->total_size);
        } else {
            free(pool->base_addr);
        }
    }
    
    free(pool);
}

// 找到合适的 slab 索引
static int find_slab_index(size_t size) {
    for (int i = 0; i < SLAB_SIZE_CLASSES; i++) {
        if (SLAB_SIZES[i] >= size) {
            return i;
        }
    }
    return -1;  // 需要大块分配
}

// 从 slab 分配
static void* slab_alloc(mem_slab_t *slab) {
    void *ptr = NULL;
    
    pthread_spin_lock(&slab->lock);
    if (slab->free_list) {
        ptr = slab->free_list;
        slab->free_list = slab->free_list->next;
        slab->free_blocks--;
    }
    pthread_spin_unlock(&slab->lock);
    
    return ptr;
}

// 归还到 slab
static void slab_free(mem_slab_t *slab, void *ptr) {
    pthread_spin_lock(&slab->lock);
    free_list_node_t *node = (free_list_node_t *)ptr;
    node->next = slab->free_list;
    slab->free_list = node;
    slab->free_blocks++;
    pthread_spin_unlock(&slab->lock);
}

// 分配内存
void* memory_pool_alloc(memory_pool_t *pool, size_t size) {
    if (!pool || size == 0) return NULL;
    
    // 加上头部大小用于追踪
    size_t actual_size = size + sizeof(mem_block_header_t);
    
    int slab_idx = find_slab_index(actual_size);
    void *ptr = NULL;
    
    if (slab_idx >= 0) {
        // 从 slab 分配
        ptr = slab_alloc(&pool->slabs[slab_idx]);
        if (ptr) {
            mem_block_header_t *header = (mem_block_header_t *)ptr;
            header->size = SLAB_SIZES[slab_idx];
            header->in_use = true;
            header->next = NULL;
            
            __sync_fetch_and_add(&pool->alloc_count, 1);
            __sync_fetch_and_add(&pool->alloc_bytes, size);
            
            return (char *)ptr + sizeof(mem_block_header_t);
        }
    }
    
    // 大块分配
    pthread_mutex_lock(&pool->large_block_lock);
    
    size_t large_block_total = pool->total_size * 4 / 10;
    if (pool->large_block_offset + actual_size <= large_block_total) {
        ptr = (char *)pool->large_block_base + pool->large_block_offset;
        pool->large_block_offset += actual_size;
        
        // 对齐到 64 字节
        pool->large_block_offset = (pool->large_block_offset + 63) & ~63;
        
        mem_block_header_t *header = (mem_block_header_t *)ptr;
        header->size = actual_size;
        header->in_use = true;
        header->next = NULL;
        
        __sync_fetch_and_add(&pool->alloc_count, 1);
        __sync_fetch_and_add(&pool->alloc_bytes, size);
        
        ptr = (char *)ptr + sizeof(mem_block_header_t);
    }
    
    pthread_mutex_unlock(&pool->large_block_lock);
    
    return ptr;
}

// 释放内存
void memory_pool_free(memory_pool_t *pool, void *ptr) {
    if (!pool || !ptr) return;
    
    mem_block_header_t *header = (mem_block_header_t *)((char *)ptr - sizeof(mem_block_header_t));
    
    if (!header->in_use) {
        fprintf(stderr, "Warning: Double free detected\n");
        return;
    }
    
    header->in_use = false;
    __sync_fetch_and_add(&pool->free_count, 1);
    
    // 检查是否在 slab 范围内
    for (int i = 0; i < SLAB_SIZE_CLASSES; i++) {
        if (header->size == SLAB_SIZES[i]) {
            char *slab_start = (char *)pool->slabs[i].base;
            char *slab_end = slab_start + pool->slabs[i].total_blocks * SLAB_SIZES[i];
            
            if ((char *)header >= slab_start && (char *)header < slab_end) {
                slab_free(&pool->slabs[i], header);
                return;
            }
        }
    }
    
    // 大块分配的内存目前不回收（简化实现）
    // 生产环境应该实现更复杂的大块内存管理
}

// 获取内存池基地址
void* memory_pool_get_base(memory_pool_t *pool) {
    return pool ? pool->base_addr : NULL;
}

// 获取内存池大小
size_t memory_pool_get_size(memory_pool_t *pool) {
    return pool ? pool->total_size : 0;
}

// 打印统计信息
void memory_pool_print_stats(memory_pool_t *pool) {
    if (!pool) return;
    
    printf("\n=== Memory Pool Statistics ===\n");
    printf("Total size: %zu bytes (%.2f MB)\n", 
           pool->total_size, (double)pool->total_size / (1024 * 1024));
    printf("Use huge pages: %s\n", pool->use_huge_pages ? "yes" : "no");
    printf("Allocations: %lu\n", pool->alloc_count);
    printf("Frees: %lu\n", pool->free_count);
    printf("Allocated bytes: %lu\n", pool->alloc_bytes);
    
    printf("\nSlab Statistics:\n");
    for (int i = 0; i < SLAB_SIZE_CLASSES; i++) {
        mem_slab_t *slab = &pool->slabs[i];
        printf("  Size %5zu: %zu/%zu blocks free (%.1f%% used)\n",
               slab->block_size,
               slab->free_blocks,
               slab->total_blocks,
               100.0 * (slab->total_blocks - slab->free_blocks) / slab->total_blocks);
    }
    printf("==============================\n\n");
}