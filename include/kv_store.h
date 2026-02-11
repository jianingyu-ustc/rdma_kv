#ifndef RDMA_KV_KV_STORE_H
#define RDMA_KV_KV_STORE_H

#include "rdma_common.h"
#include "hash_table.h"
#include "memory_pool.h"

#ifdef __cplusplus
extern "C" {
#endif

// KV 存储配置
typedef struct kv_config {
    char bind_ip[64];            // 绑定 IP
    int port;                    // 端口
    size_t memory_size;          // 内存池大小
    bool use_huge_pages;         // 是否使用大页
    size_t hash_table_size;      // 哈希表大小
    int num_workers;             // 工作线程数
    conn_mode_t conn_mode;       // 建链方式：CONN_MODE_CM 或 CONN_MODE_SOCKET
    char ib_dev[64];             // IB 设备名（Socket 模式使用，空则自动选择）
} kv_config_t;

// KV 存储服务端
typedef struct kv_server {
    kv_config_t config;
    rdma_context_t rdma_ctx;
    hash_table_t *hash_table;
    memory_pool_t *mem_pool;
    
    // 工作线程
    pthread_t *workers;
    bool running;
    
    // 客户端连接管理
    struct {
        rdma_context_t *contexts;
        int count;
        int capacity;
        pthread_mutex_t lock;
    } clients;
} kv_server_t;

// KV 存储客户端
typedef struct kv_client {
    rdma_context_t rdma_ctx;
    memory_pool_t *mem_pool;     // 客户端本地内存池
    char server_ip[64];
    int server_port;
    bool connected;
} kv_client_t;

// ===================== 服务端 API =====================

// 创建服务端
kv_server_t* kv_server_create(const kv_config_t *config);

// 销毁服务端
void kv_server_destroy(kv_server_t *server);

// 启动服务端
int kv_server_start(kv_server_t *server);

// 停止服务端
void kv_server_stop(kv_server_t *server);

// ===================== 客户端 API =====================

// 创建客户端
// @param mode: 建链方式
kv_client_t* kv_client_create(conn_mode_t mode);

// 销毁客户端
void kv_client_destroy(kv_client_t *client);

// 连接服务端 (RDMA CM 模式)
int kv_client_connect(kv_client_t *client, const char *server_ip, int port);

// 连接服务端 (Socket 模式)
// @param ib_dev: IB 设备名，NULL 表示自动选择
int kv_client_connect_socket(kv_client_t *client, const char *server_ip, int port, const char *ib_dev);

// 断开连接
void kv_client_disconnect(kv_client_t *client);

// GET 操作
// 小数据：使用 Send/Recv
// 大数据：获取远程地址后使用 RDMA Read
int kv_client_get(kv_client_t *client, const char *key, size_t key_len,
                  void *value_buf, size_t *value_len);

// PUT 操作
// 小数据：使用 Send/Recv
// 大数据：使用 RDMA Write 直接写入服务端内存
int kv_client_put(kv_client_t *client, const char *key, size_t key_len,
                  const void *value, size_t value_len);

// DELETE 操作
int kv_client_delete(kv_client_t *client, const char *key, size_t key_len);

// CAS 操作（原子比较并交换）
int kv_client_cas(kv_client_t *client, const char *key, size_t key_len,
                  uint64_t expected_version,
                  const void *new_value, size_t new_value_len,
                  uint64_t *new_version);

// 批量 GET
int kv_client_batch_get(kv_client_t *client,
                        const char **keys, const size_t *key_lens,
                        void **values, size_t *value_lens,
                        int count);

// 批量 PUT
int kv_client_batch_put(kv_client_t *client,
                        const char **keys, const size_t *key_lens,
                        const void **values, const size_t *value_lens,
                        int count);

// ===================== 内部函数 =====================

// 选择传输方式
typedef enum {
    TRANSFER_SEND_RECV,    // 小数据用 Send/Recv
    TRANSFER_RDMA_WRITE,   // 大数据写用 RDMA Write
    TRANSFER_RDMA_READ     // 大数据读用 RDMA Read
} transfer_mode_t;

transfer_mode_t select_transfer_mode(size_t data_size, bool is_write);

#ifdef __cplusplus
}
#endif

#endif // RDMA_KV_KV_STORE_H