#ifndef RDMA_KV_RDMA_COMMON_H
#define RDMA_KV_RDMA_COMMON_H

#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// 建链方式
typedef enum {
    CONN_MODE_CM = 0,      // RDMA CM 建链（默认）
    CONN_MODE_SOCKET       // Socket 建链 + 手动 QP 管理
} conn_mode_t;

// QP 连接信息（用于 Socket 建链交换）
typedef struct {
    uint32_t qp_num;       // QP 号
    uint16_t lid;          // Local ID
    uint8_t  gid[16];      // Global ID
    uint32_t psn;          // Packet Sequence Number
} qp_conn_info_t;

// RDMA 配置常量
#define RDMA_MAX_WR          256
#define RDMA_MAX_SGE         4
#define RDMA_CQ_SIZE         512
#define RDMA_TIMEOUT_MS      5000
#define RDMA_RETRY_COUNT     7

// 数据大小阈值：小于此值用 Send，大于等于用 Write
#define RDMA_SMALL_DATA_THRESHOLD  4096

// 内存池配置
#define MEMORY_POOL_SIZE     (1UL << 30)  // 1GB
#define HUGE_PAGE_SIZE       (2UL << 20)  // 2MB 大页

// RDMA 消息类型
typedef enum {
    MSG_TYPE_GET_REQUEST = 0,
    MSG_TYPE_GET_RESPONSE,
    MSG_TYPE_PUT_REQUEST,
    MSG_TYPE_PUT_RESPONSE,
    MSG_TYPE_PUT_ALLOC_RESPONSE,  // 大数据 PUT: 返回分配的远程地址
    MSG_TYPE_PUT_COMPLETE,        // 大数据 PUT: RDMA Write 完成通知
    MSG_TYPE_DELETE_REQUEST,
    MSG_TYPE_DELETE_RESPONSE,
    MSG_TYPE_CAS_REQUEST,
    MSG_TYPE_CAS_RESPONSE,
    MSG_TYPE_RDMA_INFO,      // 交换 RDMA 内存信息
} rdma_msg_type_t;

// 大数据 PUT 分配响应信息
typedef struct {
    uint64_t remote_addr;    // 远程地址（用于 RDMA Write）
    uint32_t rkey;           // 远程密钥
    uint64_t value_offset;   // 在内存池中的偏移（Server 内部使用）
} __attribute__((packed)) put_alloc_info_t;

// RDMA 内存区域信息
typedef struct {
    uint64_t addr;
    uint32_t rkey;
    uint32_t size;
} rdma_mem_info_t;

// 客户端专用写缓冲区信息（用于 1 RTT 优化）
// 连接时服务端分配，客户端可直接用 write_imm 写入
typedef struct {
    uint64_t write_buf_addr;     // 客户端写入缓冲区地址
    uint32_t write_buf_rkey;     // 远程密钥
    uint32_t write_buf_size;     // 缓冲区大小
} client_write_buf_t;

// RDMA 消息头
typedef struct {
    uint32_t msg_type;
    uint32_t key_len;
    uint32_t value_len;
    uint64_t version;        // 用于乐观锁
    int32_t  status;         // 响应状态码
} __attribute__((packed)) rdma_msg_header_t;

// RDMA 连接上下文
typedef struct rdma_context {
    struct ibv_context      *ctx;
    struct ibv_pd           *pd;
    struct ibv_cq           *cq;
    struct ibv_qp           *qp;
    struct ibv_mr           *mr;
    
    // RDMA CM 模式使用
    struct rdma_cm_id       *cm_id;
    struct rdma_event_channel *event_channel;
    
    // Socket 模式使用
    int                     sock_fd;         // TCP socket
    int                     listen_fd;       // 监听 socket
    uint8_t                 port_num;        // IB 端口号
    union ibv_gid           gid;             // 本地 GID
    qp_conn_info_t          local_info;      // 本地 QP 信息
    qp_conn_info_t          remote_info;     // 远程 QP 信息
    
    // 建链模式
    conn_mode_t             conn_mode;
    
    // 注册的内存区域
    void                    *buf;
    size_t                  buf_size;
    
    // 远端内存信息（用于 RDMA Read/Write）
    rdma_mem_info_t         remote_mem;
    
    bool                    connected;
} rdma_context_t;

// RDMA 初始化和销毁
int rdma_init_context(rdma_context_t *ctx, conn_mode_t mode);
void rdma_destroy_context(rdma_context_t *ctx);

// RDMA CM 建链（默认方式）
int rdma_cm_connect(rdma_context_t *ctx, const char *server_ip, int port);
int rdma_cm_listen(rdma_context_t *ctx, const char *bind_ip, int port);
int rdma_cm_accept(rdma_context_t *ctx, rdma_context_t *client_ctx);
void rdma_cm_disconnect(rdma_context_t *ctx);

// Socket 建链方式
int rdma_socket_connect(rdma_context_t *ctx, const char *server_ip, int port, const char *ib_dev);
int rdma_socket_listen(rdma_context_t *ctx, const char *bind_ip, int port, const char *ib_dev);
int rdma_socket_accept(rdma_context_t *ctx, rdma_context_t *client_ctx);
void rdma_socket_disconnect(rdma_context_t *ctx);

// QP 状态转换（Socket 建链内部使用）
int rdma_modify_qp_to_init(rdma_context_t *ctx);
int rdma_modify_qp_to_rtr(rdma_context_t *ctx);
int rdma_modify_qp_to_rts(rdma_context_t *ctx);

// RDMA 内存注册
int rdma_register_memory(rdma_context_t *ctx, void *buf, size_t size);
void rdma_deregister_memory(rdma_context_t *ctx);

// RDMA 操作
int rdma_send(rdma_context_t *ctx, void *buf, size_t size);
int rdma_recv(rdma_context_t *ctx, void *buf, size_t size);
int rdma_read(rdma_context_t *ctx, void *local_buf, size_t size, 
              uint64_t remote_addr, uint32_t rkey);
int rdma_write(rdma_context_t *ctx, void *local_buf, size_t size,
               uint64_t remote_addr, uint32_t rkey);
int rdma_write_imm(rdma_context_t *ctx, void *local_buf, size_t size,
                   uint64_t remote_addr, uint32_t rkey, uint32_t imm_data);
int rdma_recv_imm(rdma_context_t *ctx, void *buf, size_t size, uint32_t *imm_data);
int rdma_cas(rdma_context_t *ctx, uint64_t remote_addr, uint32_t rkey,
             uint64_t expected, uint64_t desired, uint64_t *result);

// 等待完成
int rdma_poll_completion(rdma_context_t *ctx, int timeout_ms);

#ifdef __cplusplus
}
#endif

#endif // RDMA_KV_RDMA_COMMON_H