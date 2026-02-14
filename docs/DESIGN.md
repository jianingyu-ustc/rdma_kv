# RDMA KV Store 技术设计文档

## 目录

1. [项目概述](#1-项目概述)
2. [RDMA 技术原理](#2-rdma-技术原理)
3. [系统架构](#3-系统架构)
4. [内存池设计](#4-内存池设计)
5. [哈希表设计](#5-哈希表设计)
6. [RDMA 通信层](#6-rdma-通信层)
7. [KV 存储层](#7-kv-存储层)
8. [一致性与并发控制](#8-一致性与并发控制)
9. [性能优化策略](#9-性能优化策略)
10. [数据流程详解](#10-数据流程详解)

---

## 1. 项目概述

### 1.1 背景

传统的 Key-Value 存储系统（如 Redis、Memcached）基于 TCP/IP 协议栈进行网络通信，存在以下性能瓶颈：

- **多次数据拷贝**：用户态 → 内核态 → 网卡缓冲区
- **CPU 中断开销**：每个网络包都需要 CPU 处理
- **协议栈开销**：TCP/IP 协议处理消耗 CPU 资源
- **延迟较高**：通常在几十到几百微秒级别

### 1.2 RDMA 解决方案

RDMA (Remote Direct Memory Access) 允许网卡直接访问应用程序的用户态内存，绕过 CPU 和内核，实现：

- **零拷贝**：数据直接在用户态内存和网卡之间传输
- **内核旁路**：无需操作系统内核参与
- **CPU 卸载**：网卡硬件完成协议处理
- **超低延迟**：可达 1-2 微秒

### 1.3 项目目标

设计一个基于 RDMA 的高性能分布式 KV 存储，实现：

- 微秒级读写延迟
- 百万级 IOPS
- 支持原子操作（CAS）
- 内存高效利用

---

## 2. RDMA 技术原理

### 2.1 RDMA 核心概念

```
┌────────────────────────────────────────────────────────────────────┐
│                        RDMA 通信模型                                │
├────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   应用程序                                    应用程序              │
│      │                                           ▲                  │
│      ▼                                           │                  │
│   ┌──────┐    ┌──────┐          ┌──────┐    ┌──────┐              │
│   │ QP   │───►│ CQ   │          │ CQ   │◄───│ QP   │              │
│   └──────┘    └──────┘          └──────┘    └──────┘              │
│      │                                           ▲                  │
│      ▼                                           │                  │
│   ┌──────────────┐              ┌──────────────┐                   │
│   │ Memory Region│   RDMA网络   │ Memory Region│                   │
│   │    (MR)      │◄────────────►│    (MR)      │                   │
│   └──────────────┘              └──────────────┘                   │
│                                                                     │
│   客户端 (Requester)              服务端 (Responder)               │
└────────────────────────────────────────────────────────────────────┘
```

#### 关键组件说明

| 组件 | 全称 | 作用 |
|-----|------|------|
| **QP** | Queue Pair | 发送/接收队列对，每个连接一对 |
| **CQ** | Completion Queue | 完成队列，通知操作完成状态 |
| **MR** | Memory Region | 注册的内存区域，允许网卡直接访问 |
| **PD** | Protection Domain | 保护域，隔离不同应用的资源 |
| **WR** | Work Request | 工作请求，描述一次 RDMA 操作 |
| **WC** | Work Completion | 工作完成，描述操作结果 |

### 2.2 RDMA 操作类型

#### 2.2.1 Send/Recv（双边操作）

```
客户端                              服务端
   │                                   │
   │  1. Post Recv (准备接收缓冲区)    │
   │                                   │◄─┐
   │                                   │  │
   │  2. Post Send ───────────────────►│  │ 需要双方配合
   │                                   │  │
   │  3. Poll CQ (等待完成)            │──┘
   │                                   │
```

- **特点**：需要双方同时参与
- **适用场景**：小数据传输、控制消息
- **本项目用途**：< 4KB 的 KV 数据传输

#### 2.2.2 RDMA Read/Write（单边操作）

```
客户端                              服务端
   │                                   │
   │  RDMA Write ─────────────────────►│ 直接写入远程内存
   │  (服务端CPU无感知)                │
   │                                   │
   │  RDMA Read ◄─────────────────────│ 直接读取远程内存
   │  (服务端CPU无感知)                │
   │                                   │
```

- **特点**：单方发起，远端 CPU 不参与
- **适用场景**：大数据传输、低延迟读取
- **本项目用途**：>= 4KB 的数据，索引查找

#### 2.2.3 RDMA Atomic（原子操作）

```c
// Compare and Swap (CAS)
if (*remote_addr == expected) {
    *remote_addr = desired;
    return expected;    // 成功
} else {
    return *remote_addr; // 失败，返回当前值
}
```

- **特点**：网卡硬件保证原子性
- **适用场景**：分布式锁、版本控制
- **本项目用途**：乐观锁实现

### 2.3 Memory Registration

```c
// 注册内存区域
struct ibv_mr *mr = ibv_reg_mr(
    pd,                          // 保护域
    buffer,                      // 内存地址
    size,                        // 大小
    IBV_ACCESS_LOCAL_WRITE |     // 本地写
    IBV_ACCESS_REMOTE_READ |     // 远程读
    IBV_ACCESS_REMOTE_WRITE |    // 远程写
    IBV_ACCESS_REMOTE_ATOMIC     // 远程原子操作
);
```

**为什么需要注册？**

1. **Pin 内存**：防止页面被换出到磁盘
2. **建立映射**：创建虚拟地址到物理地址的映射表
3. **安全检查**：验证访问权限
4. **生成密钥**：lkey (本地) 和 rkey (远程)

**开销**：注册是昂贵操作（~100-500μs），应预先注册大块内存。

---

## 3. 系统架构

### 3.1 整体架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              KV Client                                   │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                        Client API Layer                          │   │
│  │  kv_client_get() | kv_client_put() | kv_client_cas() | ...      │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                     │
│                                    ▼                                     │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Transfer Mode Selector                        │   │
│  │           < 4KB: Send/Recv  |  >= 4KB: RDMA Read/Write          │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                     │
│                                    ▼                                     │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      RDMA Communication                          │   │
│  │              rdma_send | rdma_recv | rdma_read | rdma_write      │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                     │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                       Local Memory Pool                          │   │
│  │                      (RDMA Registered)                           │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
                                     │
                              RDMA Network
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                              KV Server                                   │
├─────────────────────────────────────────────────────────────────────────┤
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    Request Handler (Workers)                     │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                     │
│                                    ▼                                     │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                         Hash Table                               │   │
│  │    ┌─────────┬─────────┬─────────┬─────────┬─────────┐         │   │
│  │    │Bucket 0 │Bucket 1 │Bucket 2 │  ...    │Bucket N │         │   │
│  │    └────┬────┴────┬────┴────┬────┴─────────┴────┬────┘         │   │
│  │         │         │         │                   │               │   │
│  │         ▼         ▼         ▼                   ▼               │   │
│  │    [Entry]   [Entry]   [Entry]   ...       [Entry]              │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                     │
│                                    ▼                                     │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                       Memory Pool                                │   │
│  │  ┌──────────────────────────────────────────────────────────┐   │   │
│  │  │  Slab Allocator (64B | 128B | 256B | ... | 8KB)          │   │   │
│  │  ├──────────────────────────────────────────────────────────┤   │   │
│  │  │  Large Block Allocator (> 8KB)                           │   │   │
│  │  └──────────────────────────────────────────────────────────┘   │   │
│  │                    (Huge Pages + RDMA Registered)               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

### 3.2 模块依赖关系

```
                    ┌─────────────┐
                    │  kv_store   │
                    │ (kv_store.h)│
                    └──────┬──────┘
                           │
           ┌───────────────┼───────────────┐
           │               │               │
           ▼               ▼               ▼
    ┌────────────┐  ┌────────────┐  ┌────────────┐
    │ hash_table │  │rdma_common │  │memory_pool │
    │(.h/.c)     │  │(.h/.c)     │  │(.h/.c)     │
    └─────┬──────┘  └────────────┘  └────────────┘
          │                                ▲
          └────────────────────────────────┘
              (hash_table 使用 memory_pool)
```

---

## 4. 内存池设计

### 4.1 设计目标

1. **减少 Memory Registration 开销**：预先注册大块内存
2. **快速分配/释放**：O(1) 时间复杂度
3. **减少内存碎片**：Slab 分配器
4. **支持大页内存**：减少 TLB miss

### 4.2 内存布局

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Memory Pool (1GB 示例)                           │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                    Slab 区域 (60% = 614MB)                         │ │
│  │  ┌─────────────────────────────────────────────────────────────┐  │ │
│  │  │  Slab 64B   (~77MB)   [blk][blk][blk]...[blk] (~1.2M blocks)│  │ │
│  │  ├─────────────────────────────────────────────────────────────┤  │ │
│  │  │  Slab 128B  (~77MB)   [blk][blk][blk]...[blk] (~600K blocks)│  │ │
│  │  ├─────────────────────────────────────────────────────────────┤  │ │
│  │  │  Slab 256B  (~77MB)   [blk][blk][blk]...[blk] (~300K blocks)│  │ │
│  │  ├─────────────────────────────────────────────────────────────┤  │ │
│  │  │  Slab 512B  (~77MB)   [blk][blk][blk]...[blk] (~150K blocks)│  │ │
│  │  ├─────────────────────────────────────────────────────────────┤  │ │
│  │  │  Slab 1KB   (~77MB)   [blk][blk][blk]...[blk] (~75K blocks) │  │ │
│  │  ├─────────────────────────────────────────────────────────────┤  │ │
│  │  │  Slab 2KB   (~77MB)   [blk][blk][blk]...[blk] (~37K blocks) │  │ │
│  │  ├─────────────────────────────────────────────────────────────┤  │ │
│  │  │  Slab 4KB   (~77MB)   [blk][blk][blk]...[blk] (~19K blocks) │  │ │
│  │  ├─────────────────────────────────────────────────────────────┤  │ │
│  │  │  Slab 8KB   (~77MB)   [blk][blk][blk]...[blk] (~9K blocks)  │  │ │
│  │  └─────────────────────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐ │
│  │                  大块分配区域 (40% = 410MB)                         │ │
│  │  [allocated][allocated][free space...........................]     │ │
│  │       ▲                    ▲                                       │ │
│  │       │                    └── large_block_offset                  │ │
│  │       └── large_block_base                                         │ │
│  └────────────────────────────────────────────────────────────────────┘ │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.3 Slab 分配器实现

#### 数据结构

```c
typedef struct mem_slab {
    size_t block_size;           // 块大小：64/128/256/.../8192
    size_t total_blocks;         // 总块数
    size_t free_blocks;          // 空闲块数
    free_list_node_t *free_list; // 空闲链表头
    void *base;                  // 基地址
    pthread_spinlock_t lock;     // 自旋锁
} mem_slab_t;
```

#### 空闲链表

```
初始状态（所有块都在空闲链表中）：
free_list ──► [block 0] ──► [block 1] ──► [block 2] ──► ... ──► [block N] ──► NULL

分配一个块后：
free_list ──► [block 1] ──► [block 2] ──► ... ──► [block N] ──► NULL
              (block 0 被分配出去)

释放 block 0 后：
free_list ──► [block 0] ──► [block 1] ──► [block 2] ──► ... ──► NULL
              (block 0 重新加入链表头部)
```

#### 分配流程

```c
void* slab_alloc(mem_slab_t *slab) {
    pthread_spin_lock(&slab->lock);
    
    if (slab->free_list == NULL) {
        pthread_spin_unlock(&slab->lock);
        return NULL;  // 该 slab 已满
    }
    
    // O(1) 分配：直接取链表头
    void *ptr = slab->free_list;
    slab->free_list = slab->free_list->next;
    slab->free_blocks--;
    
    pthread_spin_unlock(&slab->lock);
    return ptr;
}
```

### 4.4 大页内存

```c
void* alloc_huge_pages(size_t size) {
    // 对齐到 2MB 边界
    size_t aligned = (size + (2MB - 1)) & ~(2MB - 1);
    
    void *addr = mmap(NULL, aligned,
                      PROT_READ | PROT_WRITE,
                      MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB,
                      -1, 0);
    
    // 锁定内存，防止换出
    mlock(addr, aligned);
    return addr;
}
```

**大页内存的优势**：

| 对比项 | 4KB 普通页 | 2MB 大页 |
|-------|-----------|----------|
| 1GB 内存需要的页数 | 262,144 页 | 512 页 |
| TLB 条目数 | 很容易超出 | 通常够用 |
| TLB miss 概率 | 高 | 低 |

---

## 5. 哈希表设计

### 5.1 设计目标

1. **O(1) 平均查找复杂度**
2. **支持高并发读写**
3. **内存布局适合 RDMA 远程访问**
4. **支持乐观锁（CAS）**

### 5.2 数据结构

#### 哈希条目 (64 字节对齐)

```c
typedef struct hash_entry {
    uint64_t version;            // 8B: 乐观锁版本号
    uint32_t key_len;            // 4B: 键长度
    uint32_t value_len;          // 4B: 值长度
    uint64_t value_offset;       // 8B: 值在内存池中的偏移
    uint8_t  state;              // 1B: EMPTY/OCCUPIED/DELETED
    uint8_t  padding[7];         // 7B: 对齐填充
    char     key[MAX_KEY_LEN];   // 256B: 内联存储键
} __attribute__((packed, aligned(64))) hash_entry_t;
// 总大小：288B，按 64B 对齐 → 实际占用 320B
```

**为什么 64 字节对齐？**

- CPU 缓存行大小通常是 64B
- 避免伪共享 (False Sharing)
- 一个条目可能跨 5 个缓存行，但不会和其他条目共享缓存行

#### 哈希桶

```c
typedef struct hash_bucket {
    pthread_rwlock_t lock;       // 读写锁
    uint32_t count;              // 当前条目数
    uint32_t capacity;           // 桶容量
    hash_entry_t *entries;       // 条目数组（动态扩展）
} hash_bucket_t;
```

#### 哈希表

```c
typedef struct hash_table {
    hash_bucket_t *buckets;      // 桶数组
    size_t num_buckets;          // 桶数量（默认 65536）
    size_t num_entries;          // 总条目数
    memory_pool_t *mem_pool;     // 内存池引用
    // ... 统计信息
} hash_table_t;
```

### 5.3 哈希函数

使用 **MurmurHash3** 算法，具有以下特点：

- 高性能（~3GB/s）
- 良好的分布性
- 低冲突率

```c
uint64_t hash_key(const char *key, size_t len) {
    const uint64_t seed = 0xc70f6907UL;
    const uint64_t m = 0xc6a4a7935bd1e995UL;
    const int r = 47;
    
    uint64_t h = seed ^ (len * m);
    
    // 每次处理 8 字节
    const uint64_t *data = (const uint64_t *)key;
    while (data != end) {
        uint64_t k = *data++;
        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
    }
    
    // 处理剩余字节
    // ...
    
    // 最终混合
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    
    return h;
}
```

### 5.4 冲突处理

使用**桶内数组**方式处理冲突（而非链表）：

```
bucket[i]:
┌──────────────────────────────────────────────────────────┐
│  lock | count=3 | capacity=4                             │
│  entries ──► [entry0][entry1][entry2][empty]             │
└──────────────────────────────────────────────────────────┘

当 count == capacity 时，扩展数组（2x）：
┌──────────────────────────────────────────────────────────┐
│  lock | count=3 | capacity=8                             │
│  entries ──► [e0][e1][e2][empty][empty][empty][empty][empty]
└──────────────────────────────────────────────────────────┘
```

**为什么用数组而不是链表？**

1. **缓存友好**：数组元素连续存储
2. **RDMA 友好**：可以一次 RDMA Read 读取整个桶
3. **预取有效**：CPU 可以预取后续元素

### 5.5 查找流程

```c
kv_result_t hash_table_get(hash_table_t *table, const char *key, size_t key_len) {
    // 1. 计算哈希值，定位桶
    uint64_t hash = hash_key(key, key_len);
    size_t bucket_idx = hash % table->num_buckets;
    hash_bucket_t *bucket = &table->buckets[bucket_idx];
    
    // 2. 加读锁（允许并发读）
    pthread_rwlock_rdlock(&bucket->lock);
    
    // 3. 遍历桶内条目
    for (uint32_t i = 0; i < bucket->count; i++) {
        hash_entry_t *entry = &bucket->entries[i];
        if (entry->state == ENTRY_OCCUPIED &&
            entry->key_len == key_len &&
            memcmp(entry->key, key, key_len) == 0) {
            // 找到！
            result.status = KV_OK;
            result.version = entry->version;
            result.value = base_addr + entry->value_offset;
            result.value_len = entry->value_len;
            break;
        }
    }
    
    // 4. 释放读锁
    pthread_rwlock_unlock(&bucket->lock);
    return result;
}
```

---

## 6. RDMA 通信层

### 6.1 建链方式

本项目支持两种 RDMA 连接建立方式，用户可根据需求自由选择：

| 模式 | 说明 | 优点 | 缺点 |
|-----|------|------|------|
| **RDMA CM** | 使用 RDMA CM 库自动管理 | 简单易用，自动处理路径发现 | 依赖 rdma_cm 服务 |
| **Socket** | TCP 交换 QP 信息 + 手动管理 | 更灵活，便于调试和定制 | 需要手动管理 QP 状态 |

### 6.2 RDMA CM 建链流程（默认）

```
客户端                              服务端
   │                                   │
   │  1. rdma_create_event_channel     │  rdma_create_event_channel
   │  2. rdma_create_id                │  rdma_create_id
   │                                   │  rdma_bind_addr
   │                                   │  rdma_listen
   │                                   │
   │  3. rdma_resolve_addr ──────────► │
   │     ADDR_RESOLVED ◄─────────────  │
   │                                   │
   │  4. rdma_resolve_route            │
   │     ROUTE_RESOLVED ◄─────────────│
   │                                   │
   │  5. 创建 PD, CQ, QP               │
   │                                   │
   │  6. rdma_connect ─────────────────┼─► CONNECT_REQUEST
   │                                   │
   │                                   │  7. 创建 PD, CQ, QP
   │                                   │     rdma_accept
   │                                   │
   │     ESTABLISHED ◄─────────────────┼── ESTABLISHED
   │                                   │
   │  8. 交换内存区域信息 (rkey, addr)  │
   │                                   │
```

### 6.3 Socket 建链流程

Socket 模式通过 TCP 连接交换 QP 信息，然后手动管理 QP 状态转换。

```
客户端                              服务端
   │                                   │
   │  1. TCP connect ──────────────────► TCP accept
   │                                   │
   │  2. ibv_open_device               │  ibv_open_device
   │     ibv_alloc_pd                  │  ibv_alloc_pd
   │     ibv_create_cq                 │  ibv_create_cq
   │     ibv_create_qp                 │  ibv_create_qp
   │                                   │
   │  3. 填充本地 QP 信息               │  填充本地 QP 信息
   │     (qp_num, lid, gid, psn)       │  (qp_num, lid, gid, psn)
   │                                   │
   │  4. TCP send/recv ◄──────────────► TCP send/recv
   │     (交换 QP 信息)                 │  (交换 QP 信息)
   │                                   │
   │  5. QP 状态转换：                  │  QP 状态转换：
   │     RESET → INIT                  │  RESET → INIT
   │     INIT  → RTR                   │  INIT  → RTR
   │     RTR   → RTS                   │  RTR   → RTS
   │                                   │
   │     === RDMA 连接建立完成 ===      │
   │                                   │
   │  6. 交换内存区域信息 (rkey, addr)  │
   │                                   │
```

#### QP 状态转换代码

```c
// RESET -> INIT
int rdma_modify_qp_to_init(rdma_context_t *ctx) {
    struct ibv_qp_attr attr = {
        .qp_state = IBV_QPS_INIT,
        .pkey_index = 0,
        .port_num = ctx->port_num,
        .qp_access_flags = IBV_ACCESS_LOCAL_WRITE |
                           IBV_ACCESS_REMOTE_READ |
                           IBV_ACCESS_REMOTE_WRITE |
                           IBV_ACCESS_REMOTE_ATOMIC,
    };
    return ibv_modify_qp(ctx->qp, &attr, 
        IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS);
}

// INIT -> RTR (Ready to Receive)
int rdma_modify_qp_to_rtr(rdma_context_t *ctx) {
    struct ibv_qp_attr attr = {
        .qp_state = IBV_QPS_RTR,
        .path_mtu = IBV_MTU_1024,
        .dest_qp_num = ctx->remote_info.qp_num,
        .rq_psn = ctx->remote_info.psn,
        .max_dest_rd_atomic = 4,
        .min_rnr_timer = 12,
        .ah_attr = {
            .is_global = 1,
            .dlid = ctx->remote_info.lid,
            .port_num = ctx->port_num,
            // ... GRH 设置
        },
    };
    return ibv_modify_qp(ctx->qp, &attr, ...);
}

// RTR -> RTS (Ready to Send)
int rdma_modify_qp_to_rts(rdma_context_t *ctx) {
    struct ibv_qp_attr attr = {
        .qp_state = IBV_QPS_RTS,
        .timeout = 14,
        .retry_cnt = 7,
        .rnr_retry = 7,
        .sq_psn = ctx->local_info.psn,
        .max_rd_atomic = 4,
    };
    return ibv_modify_qp(ctx->qp, &attr, ...);
}
```

#### QP 信息交换结构

```c
typedef struct {
    uint32_t qp_num;       // QP 号
    uint16_t lid;          // Local ID
    uint8_t  gid[16];      // Global ID
    uint32_t psn;          // Packet Sequence Number
} qp_conn_info_t;
```

### 6.4 使用方式

#### 服务端

```bash
# RDMA CM 模式（默认）
./kv_server -a 0.0.0.0 -p 12345

# Socket 模式
./kv_server -a 0.0.0.0 -p 12345 -S [-d mlx5_0]
```

#### 客户端

```bash
# RDMA CM 模式（默认）
./kv_client -a <server_ip> -p 12345

# Socket 模式
./kv_client -a <server_ip> -p 12345 -S [-d mlx5_0]
```

### 6.5 RDMA 操作实现

#### Send 操作

```c
int rdma_send(rdma_context_t *ctx, void *buf, size_t size) {
    struct ibv_sge sge = {
        .addr = (uint64_t)buf,
        .length = size,
        .lkey = ctx->mr->lkey,  // 本地密钥
    };
    
    struct ibv_send_wr wr = {
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_SEND,
        .send_flags = IBV_SEND_SIGNALED,  // 完成时通知
    };
    
    struct ibv_send_wr *bad_wr;
    ibv_post_send(ctx->qp, &wr, &bad_wr);
    
    return rdma_poll_completion(ctx, timeout);
}
```

#### RDMA Read 操作

```c
int rdma_read(rdma_context_t *ctx, void *local_buf, size_t size,
              uint64_t remote_addr, uint32_t rkey) {
    struct ibv_sge sge = {
        .addr = (uint64_t)local_buf,
        .length = size,
        .lkey = ctx->mr->lkey,
    };
    
    struct ibv_send_wr wr = {
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_READ,
        .send_flags = IBV_SEND_SIGNALED,
        .wr.rdma = {
            .remote_addr = remote_addr,  // 远程地址
            .rkey = rkey,                // 远程密钥
        },
    };
    
    ibv_post_send(ctx->qp, &wr, &bad_wr);
    return rdma_poll_completion(ctx, timeout);
}
```

#### RDMA CAS 操作

```c
int rdma_cas(rdma_context_t *ctx, uint64_t remote_addr, uint32_t rkey,
             uint64_t expected, uint64_t desired, uint64_t *result) {
    struct ibv_send_wr wr = {
        .opcode = IBV_WR_ATOMIC_CMP_AND_SWP,
        .send_flags = IBV_SEND_SIGNALED,
        .wr.atomic = {
            .remote_addr = remote_addr,
            .rkey = rkey,
            .compare_add = expected,  // 期望值
            .swap = desired,          // 新值
        },
    };
    
    ibv_post_send(ctx->qp, &wr, &bad_wr);
    // CAS 结果存储在本地缓冲区
    // *result 为远程内存的原始值
}
```

### 6.6 完成队列轮询

```c
int rdma_poll_completion(rdma_context_t *ctx, int timeout_ms) {
    struct ibv_wc wc;
    int polls = 0;
    
    while (polls < timeout_ms * 1000) {
        int ret = ibv_poll_cq(ctx->cq, 1, &wc);
        
        if (ret > 0) {
            if (wc.status == IBV_WC_SUCCESS) {
                return 0;  // 成功
            } else {
                return -1; // 失败
            }
        }
        
        usleep(1);  // 避免 busy-wait 过度消耗 CPU
        polls++;
    }
    
    return -1;  // 超时
}
```

---

## 7. KV 存储层

### 7.1 消息协议

```c
// 消息头（所有请求/响应共用）
typedef struct {
    uint32_t msg_type;     // 消息类型
    uint32_t key_len;      // 键长度
    uint32_t value_len;    // 值长度
    uint64_t version;      // 版本号（CAS 用）
    int32_t  status;       // 响应状态码
} rdma_msg_header_t;

// 消息类型
MSG_TYPE_GET_REQUEST     = 0
MSG_TYPE_GET_RESPONSE    = 1
MSG_TYPE_PUT_REQUEST     = 2
MSG_TYPE_PUT_RESPONSE    = 3
MSG_TYPE_DELETE_REQUEST  = 4
MSG_TYPE_DELETE_RESPONSE = 5
MSG_TYPE_CAS_REQUEST     = 6
MSG_TYPE_CAS_RESPONSE    = 7
MSG_TYPE_RDMA_INFO       = 8  // 交换内存信息
```

### 7.2 GET 操作流程

```
┌──────────────────────────────────────────────────────────────────────────┐
│                            GET 操作流程                                   │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  客户端                               服务端                              │
│     │                                    │                                │
│     │  1. 构建 GET_REQUEST               │                                │
│     │     [header][key]                  │                                │
│     │                                    │                                │
│     │  2. rdma_send ────────────────────►│                                │
│     │                                    │  3. 解析请求                   │
│     │                                    │     hash_table_get()           │
│     │                                    │                                │
│     │              小数据 (< 4KB)        │                                │
│     │  4a. rdma_recv ◄──────────────────│  返回 [header][value]          │
│     │      直接从响应中获取 value         │                                │
│     │                                    │                                │
│     │              大数据 (>= 4KB)       │                                │
│     │  4b. rdma_recv ◄──────────────────│  返回 [header][remote_addr]    │
│     │                                    │                                │
│     │  5b. rdma_read ───────────────────►│  直接读取远程内存              │
│     │      [value] ◄─────────────────────│  (服务端 CPU 不参与)          │
│     │                                    │                                │
└──────────────────────────────────────────────────────────────────────────┘
```

### 7.3 PUT 操作流程

#### 小数据 PUT (< 4KB) - 1 RTT

```
┌──────────────────────────────────────────────────────────────────────────┐
│                       小数据 PUT 操作流程 (1 RTT)                         │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  客户端                               服务端                              │
│     │                                    │                                │
│     │  1. rdma_send ────────────────────►│  [header][key][value]         │
│     │                                    │                                │
│     │                                    │  2. hash_table_put()           │
│     │                                    │                                │
│     │  3. rdma_recv ◄───────────────────│  [header] (status, version)    │
│     │                                    │                                │
└──────────────────────────────────────────────────────────────────────────┘
```

#### 大数据 PUT (>= 4KB) - 1 RTT 优化版

使用预分配写缓冲区 + RDMA Write with Immediate 实现 1 RTT：

```
┌──────────────────────────────────────────────────────────────────────────┐
│                   大数据 PUT 操作流程 (1 RTT 优化)                        │
├──────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  连接阶段：服务端为每个客户端预分配写缓冲区 (~1MB)                         │
│                                                                           │
│  客户端                               服务端                              │
│     │                                    │                                │
│     │  1. rdma_write_imm ───────────────►│  直接写入预分配缓冲区          │
│     │     [header][key][value]           │  (立即数携带消息类型)          │
│     │                                    │                                │
│     │                                    │  2. rdma_recv_imm() 收到通知   │
│     │                                    │     解析数据，hash_table_put() │
│     │                                    │                                │
│     │  3. rdma_recv ◄───────────────────│  [header] (status, version)    │
│     │                                    │                                │
└──────────────────────────────────────────────────────────────────────────┘
```

**1 RTT 优化原理**：

| 对比项 | 旧方案 (2+ RTT) | 新方案 (1 RTT) |
|-------|----------------|----------------|
| 流程 | send→recv→write→send→recv | write_imm→recv |
| 往返次数 | 2-3 | **1** |
| 服务端 CPU | 参与地址分配 | 只处理最终存储 |
| 限制 | 无 | 单次 ≤ 预分配缓冲区大小 |

### 7.4 传输模式选择

```c
transfer_mode_t select_transfer_mode(size_t data_size, bool is_write) {
    // 阈值：4KB
    // 小于 4KB 用 Send/Recv，大于等于 4KB 用 RDMA Read/Write
    
    if (data_size < 4096) {
        return TRANSFER_SEND_RECV;
    }
    return is_write ? TRANSFER_RDMA_WRITE : TRANSFER_RDMA_READ;
}
```

**为什么是 4KB？**

| 数据大小 | Send/Recv 优势 | RDMA Read/Write 优势 |
|---------|---------------|---------------------|
| < 4KB | 一次往返完成，简单 | 需要额外信令，反而更慢 |
| >= 4KB | 需要分片，多次往返 | 一次完成，绕过 CPU |

---

## 8. 一致性与并发控制

### 8.1 乐观锁机制

每个哈希条目都有一个 `version` 字段，用于实现乐观并发控制：

```c
typedef struct hash_entry {
    uint64_t version;  // 每次修改递增
    // ...
} hash_entry_t;
```

#### CAS 操作流程

```
客户端 A                                        服务端
    │                                              │
    │  1. GET key1 ────────────────────────────────►
    │     返回 version=5, value="old"  ◄───────────│
    │                                              │
    │  2. 本地计算新值 "new"                        │
    │                                              │
    │  3. CAS(key1, expected_ver=5, new="new") ────►
    │                                              │
    │     服务端检查：                              │
    │     if (entry->version == 5) {              │
    │         entry->value = "new";               │
    │         entry->version = 6;                 │
    │         return OK;                          │
    │     } else {                                │
    │         return CAS_FAILED;                  │
    │     }                                       │
    │                                              │
    │     成功：返回 version=6  ◄──────────────────│
    │                                              │
```

#### 并发冲突处理

```
客户端 A                客户端 B                服务端
    │                      │                       │
    │  GET key1 ──────────────────────────────────►│
    │  ver=5 ◄────────────────────────────────────│
    │                      │                       │
    │                      │  GET key1 ───────────►│
    │                      │  ver=5 ◄─────────────│
    │                      │                       │
    │  CAS(ver=5) ────────────────────────────────►│
    │  OK, ver=6 ◄────────────────────────────────│
    │                      │                       │
    │                      │  CAS(ver=5) ─────────►│
    │                      │  FAILED, cur=6 ◄─────│  版本不匹配！
    │                      │                       │
    │                      │  重试：GET + CAS      │
    │                      │                       │
```

### 8.2 锁粒度设计

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              锁粒度设计                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  级别 1: 表级锁 (resize_lock)                                           │
│  ┌─────────────────────────────────────────────────────────────────┐    │
│  │  仅在扩容时使用，不影响正常读写                                    │    │
│  └─────────────────────────────────────────────────────────────────┘    │
│                                                                          │
│  级别 2: 桶级锁 (bucket->lock) - 读写锁                                 │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐                    │
│  │ rwlock  │  │ rwlock  │  │ rwlock  │  │ rwlock  │  ...               │
│  │bucket 0 │  │bucket 1 │  │bucket 2 │  │bucket 3 │                    │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘                    │
│                                                                          │
│  优势：                                                                  │
│  - 不同桶的操作完全并行                                                  │
│  - 同一桶的读操作可以并行                                                │
│  - 写操作只阻塞同一桶的其他操作                                          │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 8.3 并发性能

| 场景 | 锁行为 | 并发度 |
|-----|-------|-------|
| 不同 key 读 | 无冲突 | 完全并行 |
| 不同 key 写 | 大概率无冲突 | 高并行 |
| 同 key 读 | 共享读锁 | 并行 |
| 同 key 写 | 互斥 | 串行 |
| 同 key 读写 | 写阻塞读 | 部分串行 |

---

## 9. 性能优化策略

### 9.1 内存优化

| 优化策略 | 实现方式 | 效果 |
|---------|---------|------|
| 大页内存 | `MAP_HUGETLB` | 减少 TLB miss |
| 内存锁定 | `mlock()` | 防止页面换出 |
| 预注册 | 启动时一次性注册 | 避免运行时开销 |
| Slab 分配 | 固定大小块 | O(1) 分配，无碎片 |
| 64B 对齐 | `aligned(64)` | 避免伪共享 |

### 9.2 网络优化

| 优化策略 | 实现方式 | 效果 |
|---------|---------|------|
| 传输模式选择 | < 4KB 用 Send | 减少往返次数 |
| 批量操作 | batch_get/put | 减少网络 RTT |
| 流水线 | 多 WR 同时发送 | 隐藏延迟 |
| Inline 发送 | 小数据内联 | 减少 DMA |

### 9.3 CPU 优化

| 优化策略 | 实现方式 | 效果 |
|---------|---------|------|
| 自旋锁 | `pthread_spinlock` | 短临界区更快 |
| 读写锁 | `pthread_rwlock` | 读多写少优化 |
| 原子操作 | `__sync_*` | 无锁计数器 |
| Key 内联 | 256B 固定空间 | 减少指针跳转 |

### 9.4 性能指标

典型配置下的预期性能：

| 操作 | 延迟 (μs) | 吞吐 (ops/s) |
|-----|----------|-------------|
| GET (小数据) | 2-5 | 500K+ |
| GET (大数据) | 5-10 | 200K+ |
| PUT (小数据) | 3-6 | 400K+ |
| PUT (大数据) | 6-15 | 150K+ |
| CAS | 4-8 | 300K+ |

---

## 10. 数据流程详解

### 10.1 启动流程

```
服务端启动：
┌──────────────────────────────────────────────────────────────┐
│ 1. 创建内存池                                                 │
│    memory_pool_create(1GB, huge_pages=true)                  │
│    └─► mmap(MAP_HUGETLB) + mlock()                          │
│                                                              │
│ 2. 创建哈希表                                                 │
│    hash_table_create(65536, mem_pool)                        │
│    └─► 分配 65536 个桶，每桶初始 4 条目                       │
│                                                              │
│ 3. 初始化 RDMA                                               │
│    rdma_init_context()                                       │
│    └─► 创建 event_channel                                    │
│                                                              │
│ 4. 注册内存                                                   │
│    rdma_register_memory(mem_pool_base, mem_pool_size)        │
│    └─► ibv_reg_mr() 返回 lkey, rkey                         │
│                                                              │
│ 5. 开始监听                                                   │
│    rdma_listen(port)                                         │
│    └─► 等待客户端连接                                        │
│                                                              │
│ 6. 启动工作线程                                               │
│    for i in range(num_workers):                              │
│        pthread_create(worker_thread)                         │
└──────────────────────────────────────────────────────────────┘
```

### 10.2 完整 PUT 操作示例

```
时间轴：

T0   客户端: kv_client_put("user_123", user_data, 2048)
     │
T1   客户端: select_transfer_mode(2048) → SEND_RECV (< 4KB)
     │
T2   客户端: 构建请求
     │       ┌────────────────────────────────────┐
     │       │ msg_type: PUT_REQUEST              │
     │       │ key_len: 8                         │
     │       │ value_len: 2048                    │
     │       ├────────────────────────────────────┤
     │       │ key: "user_123"                    │
     │       ├────────────────────────────────────┤
     │       │ value: [2048 bytes of user_data]  │
     │       └────────────────────────────────────┘
     │
T3   客户端: rdma_send() ─────────────────────────────────────►
     │                                                          │
T4   │                                            服务端: rdma_recv()
     │                                                          │
T5   │                                            服务端: 解析请求
     │                                                          │
T6   │                                            服务端: hash_table_put()
     │                                            │ 1. hash("user_123") = 0xABCD1234
     │                                            │ 2. bucket_idx = 0xABCD1234 % 65536 = 4660
     │                                            │ 3. wrlock(bucket[4660])
     │                                            │ 4. 检查是否存在 → 不存在
     │                                            │ 5. memory_pool_alloc(2048) → 分配值空间
     │                                            │ 6. 创建新 entry:
     │                                            │    - version = 1
     │                                            │    - key = "user_123"
     │                                            │    - value_offset = allocated_addr
     │                                            │ 7. bucket[4660].count++
     │                                            │ 8. unlock(bucket[4660])
     │                                                          │
T7   │                                            服务端: 构建响应
     │                                            ┌────────────────────────┐
     │                                            │ msg_type: PUT_RESPONSE │
     │                                            │ status: KV_OK          │
     │                                            │ version: 1             │
     │                                            └────────────────────────┘
     │                                                          │
T8   客户端: rdma_recv() ◄─────────────────────────────────────
     │
T9   客户端: 返回 KV_OK

总耗时: ~5-10 μs (取决于网络和数据大小)
```

---

## 附录

### A. 错误码定义

| 错误码 | 值 | 含义 |
|-------|---|------|
| KV_OK | 0 | 成功 |
| KV_ERR_NOT_FOUND | -1 | 键不存在 |
| KV_ERR_EXISTS | -2 | 键已存在 |
| KV_ERR_CAS_FAILED | -3 | CAS 失败（版本不匹配） |
| KV_ERR_KEY_TOO_LONG | -4 | 键超长 (> 256B) |
| KV_ERR_VALUE_TOO_LONG | -5 | 值超长 (> 1MB) |
| KV_ERR_NO_MEMORY | -6 | 内存不足 |
| KV_ERR_INTERNAL | -7 | 内部错误 |

### B. 配置参数

| 参数 | 默认值 | 说明 |
|-----|-------|------|
| RDMA_SMALL_DATA_THRESHOLD | 4096 | Send/Recv 阈值 |
| MEMORY_POOL_SIZE | 1GB | 内存池大小 |
| HASH_TABLE_INITIAL_SIZE | 65536 | 哈希桶数量 |
| MAX_KEY_LENGTH | 256 | 最大键长度 |
| MAX_VALUE_LENGTH | 1MB | 最大值长度 |
| RDMA_TIMEOUT_MS | 5000 | RDMA 操作超时 |

### C. 参考资料

1. RDMA Aware Networks Programming User Manual (Mellanox)
2. InfiniBand Architecture Specification
3. Linux Kernel RDMA Subsystem Documentation
4. "Design Guidelines for High Performance RDMA Systems" (USENIX ATC)
5. "FaRM: Fast Remote Memory" (NSDI)