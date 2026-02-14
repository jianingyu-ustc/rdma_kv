# RDMA-based High-Performance Distributed Key-Value Store

基于 RDMA (Remote Direct Memory Access) 实现的高性能分布式 Key-Value 存储系统。

## 设计概述

### 架构图

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        RDMA KV Store Architecture                        │
├─────────────────────────────────────────────────────────────────────────┤
│   ┌──────────────┐                           ┌──────────────────────┐   │
│   │   Client     │                           │       Server         │   │
│   │              │     RDMA Send/Recv       │                      │   │
│   │  ┌────────┐  │ ◄─────────────────────► │  ┌────────────────┐  │   │
│   │  │ Local  │  │     (Small Data)         │  │   Hash Table   │  │   │
│   │  │ Memory │  │                          │  └────────────────┘  │   │
│   │  │  Pool  │  │     RDMA Read/Write      │                      │   │
│   │  └────────┘  │ ◄─────────────────────► │  ┌────────────────┐  │   │
│   │              │     (Large Data)         │  │  Memory Pool   │  │   │
│   │              │                          │  │  (Huge Pages)  │  │   │
│   │              │     RDMA CAS             │  └────────────────┘  │   │
│   │              │ ◄─────────────────────► │                      │   │
│   └──────────────┘     (Atomic Ops)         └──────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

### 核心设计

#### 1. 索引查找 - RDMA Read
使用 RDMA Read 获取远程哈希表节点，实现零拷贝远程内存访问，延迟低至 1-2 微秒。

#### 2. 数据读写策略

| 数据大小 | 传输方式 | RTT | 说明 |
|---------|---------|-----|------|
| < 4KB   | Send/Recv | 1 | 小数据开销低 |
| >= 4KB  | RDMA Write with Immediate | **1** | 写入时零拷贝，1 RTT 优化 |
| >= 4KB  | RDMA Read  | 2 | 读取时零拷贝 |

**1 RTT PUT 优化**：连接时服务端为客户端预分配写缓冲区，大数据 PUT 时客户端直接用 `write_imm` 写入预分配区域，省去请求分配地址的往返。

#### 3. 一致性保证 - RDMA CAS
- 使用版本号实现乐观并发控制
- CAS 失败时返回当前版本号，客户端可重试
- 支持原子的读-修改-写操作

#### 4. 性能优化
- **大页内存 (Huge Pages)**: 减少 TLB miss
- **预分配 Slab 内存池**: O(1) 分配/释放，减少碎片
- **预注册 RDMA 内存**: 避免运行时 Memory Registration 开销

#### 5. 建链方式
支持两种 RDMA 连接建立方式，用户可自由选择：

| 模式 | 说明 | 适用场景 |
|-----|------|---------|
| **RDMA CM** | 使用 RDMA CM 库自动管理连接 | 大多数场景（默认） |
| **Socket** | TCP 交换 QP 信息 + 手动管理 QP 状态 | 需要更多控制、调试场景 |

## 目录结构

```
rdma_kv/
├── include/              # 头文件
│   ├── rdma_common.h     # RDMA 基础设施
│   ├── memory_pool.h     # 内存池
│   ├── hash_table.h      # 哈希表
│   └── kv_store.h        # KV 存储接口
├── src/                  # 源文件
├── examples/             # 示例代码
├── tests/                # 单元测试
├── CMakeLists.txt        # 构建配置
└── README.md
```

## 编译

### 依赖
- libibverbs (RDMA Verbs 库)
- librdmacm (RDMA Connection Manager)
- pthread

### 构建步骤

```bash
mkdir build && cd build
cmake ..
make
make test
```

## 运行

### 服务端
```bash
# RDMA CM 模式（默认）
./bin/kv_server -a 0.0.0.0 -p 12345 -m 1024 -H

# Socket 模式
./bin/kv_server -a 0.0.0.0 -p 12345 -m 1024 -S [-d mlx5_0]
```

### 客户端
```bash
# RDMA CM 模式（默认）
./bin/kv_client -a <server_ip> -p 12345

# Socket 模式
./bin/kv_client -a <server_ip> -p 12345 -S [-d mlx5_0]

# 交互命令
kv> put mykey myvalue
kv> get mykey
kv> cas mykey 1 newvalue
kv> del mykey
kv> bench 1000
```

### 命令行参数

| 参数 | 说明 |
|-----|------|
| `-a, --address` | IP 地址 |
| `-p, --port` | 端口号 |
| `-m, --memory` | 内存池大小 (MB)，仅服务端 |
| `-H, --hugepages` | 启用大页内存，仅服务端 |
| `-S, --socket` | 使用 Socket 建链模式 |
| `-d, --device` | 指定 IB 设备名（如 mlx5_0） |

## API 示例

### RDMA CM 模式（默认）
```c
#include "kv_store.h"

// 创建并连接（RDMA CM 模式）
kv_client_t *client = kv_client_create(CONN_MODE_CM);
kv_client_connect(client, "192.168.1.100", 12345);

// 操作
kv_client_put(client, "key", 3, "value", 5);
kv_client_get(client, "key", 3, buf, &len);
kv_client_cas(client, "key", 3, 1, "new", 3, &ver);

// 清理
kv_client_disconnect(client);
kv_client_destroy(client);
```

### Socket 模式
```c
// 创建并连接（Socket 模式）
kv_client_t *client = kv_client_create(CONN_MODE_SOCKET);
kv_client_connect_socket(client, "192.168.1.100", 12345, NULL); // NULL 自动选择设备

// 操作方式相同
kv_client_put(client, "key", 3, "value", 5);
// ...
```

## License

MIT License