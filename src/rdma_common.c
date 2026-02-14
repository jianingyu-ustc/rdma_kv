#include "rdma_common.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/tcp.h>

// 初始化 RDMA 上下文
int rdma_init_context(rdma_context_t *ctx, conn_mode_t mode) {
    memset(ctx, 0, sizeof(rdma_context_t));
    ctx->conn_mode = mode;
    ctx->sock_fd = -1;
    ctx->listen_fd = -1;
    
    if (mode == CONN_MODE_CM) {
        // RDMA CM 模式：创建事件通道
        ctx->event_channel = rdma_create_event_channel();
        if (!ctx->event_channel) {
            fprintf(stderr, "Failed to create event channel: %s\n", strerror(errno));
            return -1;
        }
    }
    // Socket 模式不需要在这里初始化，会在 connect/listen 时处理
    
    return 0;
}

// 销毁 RDMA 上下文
void rdma_destroy_context(rdma_context_t *ctx) {
    if (ctx->qp) {
        ibv_destroy_qp(ctx->qp);
        ctx->qp = NULL;
    }
    if (ctx->cq) {
        ibv_destroy_cq(ctx->cq);
        ctx->cq = NULL;
    }
    if (ctx->mr) {
        ibv_dereg_mr(ctx->mr);
        ctx->mr = NULL;
    }
    if (ctx->pd) {
        ibv_dealloc_pd(ctx->pd);
        ctx->pd = NULL;
    }
    
    // RDMA CM 模式清理
    if (ctx->cm_id) {
        rdma_destroy_id(ctx->cm_id);
        ctx->cm_id = NULL;
    }
    if (ctx->event_channel) {
        rdma_destroy_event_channel(ctx->event_channel);
        ctx->event_channel = NULL;
    }
    
    // Socket 模式清理
    if (ctx->sock_fd >= 0) {
        close(ctx->sock_fd);
        ctx->sock_fd = -1;
    }
    if (ctx->listen_fd >= 0) {
        close(ctx->listen_fd);
        ctx->listen_fd = -1;
    }
    if (ctx->ctx && ctx->conn_mode == CONN_MODE_SOCKET) {
        ibv_close_device(ctx->ctx);
        ctx->ctx = NULL;
    }
}

// 创建 QP
static int create_qp(rdma_context_t *ctx) {
    struct ibv_qp_init_attr qp_attr = {
        .send_cq = ctx->cq,
        .recv_cq = ctx->cq,
        .cap = {
            .max_send_wr = RDMA_MAX_WR,
            .max_recv_wr = RDMA_MAX_WR,
            .max_send_sge = RDMA_MAX_SGE,
            .max_recv_sge = RDMA_MAX_SGE,
        },
        .qp_type = IBV_QPT_RC,  // Reliable Connection
    };
    
    if (rdma_create_qp(ctx->cm_id, ctx->pd, &qp_attr)) {
        fprintf(stderr, "Failed to create QP: %s\n", strerror(errno));
        return -1;
    }
    
    ctx->qp = ctx->cm_id->qp;
    return 0;
}

// 设置 RDMA 资源
static int setup_rdma_resources(rdma_context_t *ctx) {
    // 分配保护域
    ctx->pd = ibv_alloc_pd(ctx->cm_id->verbs);
    if (!ctx->pd) {
        fprintf(stderr, "Failed to allocate PD: %s\n", strerror(errno));
        return -1;
    }
    
    // 创建完成队列
    ctx->cq = ibv_create_cq(ctx->cm_id->verbs, RDMA_CQ_SIZE, NULL, NULL, 0);
    if (!ctx->cq) {
        fprintf(stderr, "Failed to create CQ: %s\n", strerror(errno));
        return -1;
    }
    
    // 创建 QP
    if (create_qp(ctx) != 0) {
        return -1;
    }
    
    ctx->ctx = ctx->cm_id->verbs;
    return 0;
}

// 客户端连接到服务端
int rdma_cm_connect(rdma_context_t *ctx, const char *server_ip, int port) {
    struct rdma_cm_event *event;
    struct sockaddr_in addr;
    
    // 创建 CM ID
    if (rdma_create_id(ctx->event_channel, &ctx->cm_id, NULL, RDMA_PS_TCP)) {
        fprintf(stderr, "Failed to create CM ID: %s\n", strerror(errno));
        return -1;
    }
    
    // 解析地址
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, server_ip, &addr.sin_addr);
    
    // 解析地址
    if (rdma_resolve_addr(ctx->cm_id, NULL, (struct sockaddr *)&addr, RDMA_TIMEOUT_MS)) {
        fprintf(stderr, "Failed to resolve address: %s\n", strerror(errno));
        return -1;
    }
    
    // 等待地址解析完成
    if (rdma_get_cm_event(ctx->event_channel, &event)) {
        fprintf(stderr, "Failed to get CM event: %s\n", strerror(errno));
        return -1;
    }
    if (event->event != RDMA_CM_EVENT_ADDR_RESOLVED) {
        fprintf(stderr, "Unexpected event: %d\n", event->event);
        rdma_ack_cm_event(event);
        return -1;
    }
    rdma_ack_cm_event(event);
    
    // 解析路由
    if (rdma_resolve_route(ctx->cm_id, RDMA_TIMEOUT_MS)) {
        fprintf(stderr, "Failed to resolve route: %s\n", strerror(errno));
        return -1;
    }
    
    // 等待路由解析完成
    if (rdma_get_cm_event(ctx->event_channel, &event)) {
        fprintf(stderr, "Failed to get CM event: %s\n", strerror(errno));
        return -1;
    }
    if (event->event != RDMA_CM_EVENT_ROUTE_RESOLVED) {
        fprintf(stderr, "Unexpected event: %d\n", event->event);
        rdma_ack_cm_event(event);
        return -1;
    }
    rdma_ack_cm_event(event);
    
    // 设置 RDMA 资源
    if (setup_rdma_resources(ctx) != 0) {
        return -1;
    }
    
    // 连接
    struct rdma_conn_param conn_param = {
        .initiator_depth = 4,
        .responder_resources = 4,
        .retry_count = RDMA_RETRY_COUNT,
        .rnr_retry_count = RDMA_RETRY_COUNT,
    };
    
    if (rdma_connect(ctx->cm_id, &conn_param)) {
        fprintf(stderr, "Failed to connect: %s\n", strerror(errno));
        return -1;
    }
    
    // 等待连接建立
    if (rdma_get_cm_event(ctx->event_channel, &event)) {
        fprintf(stderr, "Failed to get CM event: %s\n", strerror(errno));
        return -1;
    }
    if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
        fprintf(stderr, "Failed to establish connection: %d\n", event->event);
        rdma_ack_cm_event(event);
        return -1;
    }
    rdma_ack_cm_event(event);
    
    ctx->connected = true;
    return 0;
}

// 服务端监听
int rdma_cm_listen(rdma_context_t *ctx, const char *bind_ip, int port) {
    struct sockaddr_in addr;
    
    // 创建 CM ID
    if (rdma_create_id(ctx->event_channel, &ctx->cm_id, NULL, RDMA_PS_TCP)) {
        fprintf(stderr, "Failed to create CM ID: %s\n", strerror(errno));
        return -1;
    }
    
    // 绑定地址
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (bind_ip && strlen(bind_ip) > 0) {
        inet_pton(AF_INET, bind_ip, &addr.sin_addr);
    } else {
        addr.sin_addr.s_addr = INADDR_ANY;
    }
    
    if (rdma_bind_addr(ctx->cm_id, (struct sockaddr *)&addr)) {
        fprintf(stderr, "Failed to bind address: %s\n", strerror(errno));
        return -1;
    }
    
    // 开始监听
    if (rdma_listen(ctx->cm_id, 10)) {
        fprintf(stderr, "Failed to listen: %s\n", strerror(errno));
        return -1;
    }
    
    printf("RDMA server listening on port %d\n", port);
    return 0;
}

// 接受客户端连接
int rdma_cm_accept(rdma_context_t *listen_ctx, rdma_context_t *client_ctx) {
    struct rdma_cm_event *event;
    
    // 等待连接请求
    if (rdma_get_cm_event(listen_ctx->event_channel, &event)) {
        fprintf(stderr, "Failed to get CM event: %s\n", strerror(errno));
        return -1;
    }
    
    if (event->event != RDMA_CM_EVENT_CONNECT_REQUEST) {
        fprintf(stderr, "Unexpected event: %d\n", event->event);
        rdma_ack_cm_event(event);
        return -1;
    }
    
    // 初始化客户端上下文
    memset(client_ctx, 0, sizeof(rdma_context_t));
    client_ctx->cm_id = event->id;
    client_ctx->event_channel = listen_ctx->event_channel;
    
    rdma_ack_cm_event(event);
    
    // 设置 RDMA 资源
    if (setup_rdma_resources(client_ctx) != 0) {
        return -1;
    }
    
    // 接受连接
    struct rdma_conn_param conn_param = {
        .initiator_depth = 4,
        .responder_resources = 4,
    };
    
    if (rdma_accept(client_ctx->cm_id, &conn_param)) {
        fprintf(stderr, "Failed to accept: %s\n", strerror(errno));
        return -1;
    }
    
    // 等待连接建立
    if (rdma_get_cm_event(listen_ctx->event_channel, &event)) {
        fprintf(stderr, "Failed to get CM event: %s\n", strerror(errno));
        return -1;
    }
    if (event->event != RDMA_CM_EVENT_ESTABLISHED) {
        fprintf(stderr, "Failed to establish connection: %d\n", event->event);
        rdma_ack_cm_event(event);
        return -1;
    }
    rdma_ack_cm_event(event);
    
    client_ctx->connected = true;
    return 0;
}

// 断开连接 (RDMA CM 模式)
void rdma_cm_disconnect(rdma_context_t *ctx) {
    if (ctx->connected && ctx->cm_id) {
        rdma_disconnect(ctx->cm_id);
        ctx->connected = false;
    }
}

// ===================== Socket 建链方式实现 =====================
//
// Socket 建链流程对照：
// ┌─────────────────────────────────────────────────────────────────────────┐
// │  步骤  │       SERVER 端                │       CLIENT 端               │
// ├────────┼────────────────────────────────┼───────────────────────────────┤
// │   1    │ rdma_socket_listen():          │ rdma_socket_connect():        │
// │        │   - open_ib_device()           │   - setup_rdma_resources():   │
// │        │   - ibv_query_port/gid         │     - open_ib_device()        │
// │        │   - socket() + bind() + listen │     - ibv_query_port/gid      │
// │        │                                │     - ibv_alloc_pd()          │
// │        │                                │     - ibv_create_cq()         │
// │        │                                │     - create_qp_socket()      │
// │        │                                │   - socket() + connect()      │
// ├────────┼────────────────────────────────┼───────────────────────────────┤
// │   2    │ rdma_socket_accept():          │                               │
// │        │   - accept()                   │   ← 对应 connect()            │
// │        │   - ibv_alloc_pd()             │                               │
// │        │   - ibv_create_cq()            │                               │
// │        │   - create_qp_socket()         │                               │
// ├────────┼────────────────────────────────┼───────────────────────────────┤
// │   3    │ exchange_qp_info() ◄──────────►│ exchange_qp_info()            │
// │        │ (TCP 交换 qp_num,lid,gid,psn)  │ (TCP 交换 qp_num,lid,gid,psn) │
// ├────────┼────────────────────────────────┼───────────────────────────────┤
// │   4    │ rdma_modify_qp_to_init()       │ rdma_modify_qp_to_init()      │
// │        │ rdma_modify_qp_to_rtr()        │ rdma_modify_qp_to_rtr()       │
// │        │ rdma_modify_qp_to_rts()        │ rdma_modify_qp_to_rts()       │
// ├────────┼────────────────────────────────┼───────────────────────────────┤
// │   5    │ ═══════════ RDMA 连接建立完成 ═══════════                      │
// └─────────────────────────────────────────────────────────────────────────┘
//

// 查找并打开 IB 设备
static struct ibv_context* open_ib_device(const char *ib_dev_name) {
    struct ibv_device **dev_list;
    struct ibv_context *ctx = NULL;
    int num_devices;
    
    dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list) {
        fprintf(stderr, "Failed to get IB device list\n");
        return NULL;
    }
    
    if (num_devices == 0) {
        fprintf(stderr, "No IB devices found\n");
        ibv_free_device_list(dev_list);
        return NULL;
    }
    
    // 如果指定了设备名，查找该设备；否则使用第一个设备
    for (int i = 0; i < num_devices; i++) {
        if (ib_dev_name == NULL || 
            strcmp(ibv_get_device_name(dev_list[i]), ib_dev_name) == 0) {
            ctx = ibv_open_device(dev_list[i]);
            if (ctx) {
                printf("Opened IB device: %s\n", ibv_get_device_name(dev_list[i]));
                break;
            }
        }
    }
    
    ibv_free_device_list(dev_list);
    
    if (!ctx) {
        fprintf(stderr, "Failed to open IB device\n");
    }
    
    return ctx;
}

// 创建 QP (Socket 模式)
static int create_qp_socket(rdma_context_t *ctx) {
    struct ibv_qp_init_attr qp_attr = {
        .send_cq = ctx->cq,
        .recv_cq = ctx->cq,
        .cap = {
            .max_send_wr = RDMA_MAX_WR,
            .max_recv_wr = RDMA_MAX_WR,
            .max_send_sge = RDMA_MAX_SGE,
            .max_recv_sge = RDMA_MAX_SGE,
        },
        .qp_type = IBV_QPT_RC,
    };
    
    ctx->qp = ibv_create_qp(ctx->pd, &qp_attr);
    if (!ctx->qp) {
        fprintf(stderr, "Failed to create QP: %s\n", strerror(errno));
        return -1;
    }
    
    return 0;
}

// 设置 RDMA 资源 (Socket 模式)
static int setup_rdma_resources_socket(rdma_context_t *ctx, const char *ib_dev) {
    // 打开 IB 设备
    ctx->ctx = open_ib_device(ib_dev);
    if (!ctx->ctx) {
        return -1;
    }
    
    // 查询端口属性获取 LID
    struct ibv_port_attr port_attr;
    ctx->port_num = 1;  // 默认使用端口 1
    if (ibv_query_port(ctx->ctx, ctx->port_num, &port_attr)) {
        fprintf(stderr, "Failed to query port: %s\n", strerror(errno));
        return -1;
    }
    
    // 步骤 2b: 获取 GID (用于跨子网通信)
    if (ibv_query_gid(ctx->ctx, ctx->port_num, 0, &ctx->gid)) {
        fprintf(stderr, "Failed to query GID: %s\n", strerror(errno));
        return -1;
    }
    
    // 分配保护域
    ctx->pd = ibv_alloc_pd(ctx->ctx);
    if (!ctx->pd) {
        fprintf(stderr, "Failed to allocate PD: %s\n", strerror(errno));
        return -1;
    }
    
    // 创建完成队列
    ctx->cq = ibv_create_cq(ctx->ctx, RDMA_CQ_SIZE, NULL, NULL, 0);
    if (!ctx->cq) {
        fprintf(stderr, "Failed to create CQ: %s\n", strerror(errno));
        return -1;
    }
    
    // 创建 QP
    if (create_qp_socket(ctx) != 0) {
        return -1;
    }
    
    // 填充本地 QP 信息
    ctx->local_info.qp_num = ctx->qp->qp_num;
    ctx->local_info.lid = port_attr.lid;
    ctx->local_info.psn = rand() & 0xFFFFFF;  // 随机初始 PSN
    memcpy(ctx->local_info.gid, &ctx->gid, 16);
    
    return 0;
}

// 通过 TCP socket 交换 QP 信息
static int exchange_qp_info(int sock_fd, qp_conn_info_t *local, qp_conn_info_t *remote) {
    // 发送本地信息
    if (write(sock_fd, local, sizeof(qp_conn_info_t)) != sizeof(qp_conn_info_t)) {
        fprintf(stderr, "Failed to send QP info: %s\n", strerror(errno));
        return -1;
    }
    
    // 接收远程信息
    if (read(sock_fd, remote, sizeof(qp_conn_info_t)) != sizeof(qp_conn_info_t)) {
        fprintf(stderr, "Failed to receive QP info: %s\n", strerror(errno));
        return -1;
    }
    
    printf("QP info exchanged - Local QP: 0x%x, Remote QP: 0x%x\n",
           local->qp_num, remote->qp_num);
    
    return 0;
}

// QP 状态转换：RESET -> INIT
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
    
    int flags = IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS;
    
    if (ibv_modify_qp(ctx->qp, &attr, flags)) {
        fprintf(stderr, "Failed to modify QP to INIT: %s\n", strerror(errno));
        return -1;
    }
    
    return 0;
}

// QP 状态转换：INIT -> RTR
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
            .sl = 0,
            .src_path_bits = 0,
            .port_num = ctx->port_num,
            .grh = {
                .flow_label = 0,
                .sgid_index = 0,
                .hop_limit = 1,
                .traffic_class = 0,
            },
        },
    };
    memset(&attr.ah_attr.grh.dgid, 0, sizeof(attr.ah_attr.grh.dgid));

    // 设置远程 GID
    memcpy(&attr.ah_attr.grh.dgid, ctx->remote_info.gid, 16);
    
    int flags = IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU | IBV_QP_DEST_QPN |
                IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC | IBV_QP_MIN_RNR_TIMER;
    
    if (ibv_modify_qp(ctx->qp, &attr, flags)) {
        fprintf(stderr, "Failed to modify QP to RTR: %s\n", strerror(errno));
        return -1;
    }
    
    return 0;
}

// QP 状态转换：RTR -> RTS
int rdma_modify_qp_to_rts(rdma_context_t *ctx) {
    struct ibv_qp_attr attr = {
        .qp_state = IBV_QPS_RTS,
        .timeout = 14,
        .retry_cnt = RDMA_RETRY_COUNT,
        .rnr_retry = RDMA_RETRY_COUNT,
        .sq_psn = ctx->local_info.psn,
        .max_rd_atomic = 4,
    };
    
    int flags = IBV_QP_STATE | IBV_QP_TIMEOUT | IBV_QP_RETRY_CNT |
                IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN | IBV_QP_MAX_QP_RD_ATOMIC;
    
    if (ibv_modify_qp(ctx->qp, &attr, flags)) {
        fprintf(stderr, "Failed to modify QP to RTS: %s\n", strerror(errno));
        return -1;
    }
    
    return 0;
}

/**
 * Socket 建链：客户端连接
 * 
 * 执行流程：
 *   1. setup_rdma_resources_socket() - 创建 RDMA 资源 (PD, CQ, QP)
 *   2. socket() + connect()          - 建立 TCP 连接
 *   3. exchange_qp_info()            - 与 Server 交换 QP 信息
 *   4. modify_qp: RESET→INIT→RTR→RTS - QP 状态转换
 * 
 * 对应 Server 端：rdma_socket_listen() + rdma_socket_accept()
 */
int rdma_socket_connect(rdma_context_t *ctx, const char *server_ip, int port, const char *ib_dev) {
    ctx->conn_mode = CONN_MODE_SOCKET;
    
    // 步骤 1: 设置 RDMA 资源 (打开设备, 创建 PD/CQ/QP, 填充本地 QP 信息)
    // 对应 Server: rdma_socket_accept() 中的 ibv_alloc_pd/create_cq/create_qp_socket
    if (setup_rdma_resources_socket(ctx, ib_dev) != 0) {
        return -1;
    }
    
    // 步骤 2a: 创建 TCP socket
    ctx->sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (ctx->sock_fd < 0) {
        fprintf(stderr, "Failed to create socket: %s\n", strerror(errno));
        return -1;
    }
    
    // 步骤 2b: 连接服务端 (对应 Server: accept())
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, server_ip, &addr.sin_addr);
    
    if (connect(ctx->sock_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Failed to connect: %s\n", strerror(errno));
        return -1;
    }
    
    printf("TCP connection established\n");
    
    // 步骤 3: 交换 QP 信息 (双方同时发送本地信息，接收对端信息)
    // 对应 Server: rdma_socket_accept() 中的 exchange_qp_info()
    if (exchange_qp_info(ctx->sock_fd, &ctx->local_info, &ctx->remote_info) != 0) {
        return -1;
    }
    
    // 步骤 4: QP 状态转换 (两端必须同步进行，RTR/RTS 需要对端 QP 信息)
    // 对应 Server: rdma_socket_accept() 中的 modify_qp_to_init/rtr/rts
    if (rdma_modify_qp_to_init(ctx) != 0) return -1;  // RESET → INIT
    if (rdma_modify_qp_to_rtr(ctx) != 0) return -1;   // INIT → RTR (Ready to Receive)
    if (rdma_modify_qp_to_rts(ctx) != 0) return -1;   // RTR → RTS (Ready to Send)
    
    ctx->connected = true;
    printf("RDMA connection established (Socket mode)\n");
    
    return 0;
}

/**
 * Socket 建链：服务端监听
 * 
 * 执行流程：
 *   1. open_ib_device()              - 打开 IB 设备
 *   2. ibv_query_port/gid            - 获取端口属性和 GID
 *   3. socket() + bind() + listen()  - 创建监听 socket
 * 
 * 注意：此函数只做准备工作，不创建 PD/CQ/QP
 *       每个客户端连接的 PD/CQ/QP 在 rdma_socket_accept() 中创建
 * 
 * 对应 Client 端：rdma_socket_connect() 的前半部分
 */
int rdma_socket_listen(rdma_context_t *ctx, const char *bind_ip, int port, const char *ib_dev) {
    ctx->conn_mode = CONN_MODE_SOCKET;
    
    // 步骤 1: 打开 IB 设备（此时只准备设备信息，不创建 PD/CQ/QP）
    // PD/CQ/QP 将在 rdma_socket_accept() 中为每个客户端单独创建
    ctx->ctx = open_ib_device(ib_dev);
    if (!ctx->ctx) {
        return -1;
    }
    
    // 步骤 2a: 查询端口属性获取 LID
    struct ibv_port_attr port_attr;
    ctx->port_num = 1;
    if (ibv_query_port(ctx->ctx, ctx->port_num, &port_attr)) {
        fprintf(stderr, "Failed to query port: %s\n", strerror(errno));
        return -1;
    }
    
    // 获取 GID
    if (ibv_query_gid(ctx->ctx, ctx->port_num, 0, &ctx->gid)) {
        fprintf(stderr, "Failed to query GID: %s\n", strerror(errno));
        return -1;
    }
    
    // 步骤 3a: 创建监听 socket
    ctx->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (ctx->listen_fd < 0) {
        fprintf(stderr, "Failed to create socket: %s\n", strerror(errno));
        return -1;
    }
    
    // 设置地址重用
    int opt = 1;
    setsockopt(ctx->listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    
    // 绑定地址
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    if (bind_ip && strlen(bind_ip) > 0) {
        inet_pton(AF_INET, bind_ip, &addr.sin_addr);
    } else {
        addr.sin_addr.s_addr = INADDR_ANY;
    }
    
    if (bind(ctx->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Failed to bind: %s\n", strerror(errno));
        return -1;
    }
    
    // 步骤 3c: 开始监听，等待 Client 的 connect()
    if (listen(ctx->listen_fd, 10) < 0) {
        fprintf(stderr, "Failed to listen: %s\n", strerror(errno));
        return -1;
    }
    
    printf("RDMA server listening on port %d (Socket mode)\n", port);
    return 0;
}

/**
 * Socket 建链：接受连接
 * 
 * 执行流程：
 *   1. accept()                      - 接受 TCP 连接 (对应 Client: connect())
 *   2. ibv_alloc_pd()                - 分配保护域 (对应 Client: setup_rdma_resources)
 *   3. ibv_create_cq()               - 创建完成队列
 *   4. create_qp_socket()            - 创建 QP
 *   5. exchange_qp_info()            - 与 Client 交换 QP 信息
 *   6. modify_qp: RESET→INIT→RTR→RTS - QP 状态转换
 * 
 * 对应 Client 端：rdma_socket_connect()
 */
int rdma_socket_accept(rdma_context_t *listen_ctx, rdma_context_t *client_ctx) {
    // 步骤 1: 接受 TCP 连接 (对应 Client: connect())
    struct sockaddr_in client_addr;
    socklen_t addr_len = sizeof(client_addr);
    
    int client_fd = accept(listen_ctx->listen_fd, (struct sockaddr *)&client_addr, &addr_len);
    if (client_fd < 0) {
        fprintf(stderr, "Failed to accept: %s\n", strerror(errno));
        return -1;
    }
    
    printf("TCP connection accepted from %s:%d\n",
           inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port));
    
    // 初始化客户端上下文
    memset(client_ctx, 0, sizeof(rdma_context_t));
    client_ctx->conn_mode = CONN_MODE_SOCKET;
    client_ctx->sock_fd = client_fd;
    client_ctx->listen_fd = -1;
    client_ctx->ctx = listen_ctx->ctx;
    client_ctx->port_num = listen_ctx->port_num;
    memcpy(&client_ctx->gid, &listen_ctx->gid, sizeof(union ibv_gid));
    
    // 步骤 2: 分配 PD (对应 Client: setup_rdma_resources_socket 中的 ibv_alloc_pd)
    client_ctx->pd = ibv_alloc_pd(client_ctx->ctx);
    if (!client_ctx->pd) {
        fprintf(stderr, "Failed to allocate PD: %s\n", strerror(errno));
        close(client_fd);
        return -1;
    }
    
    // 步骤 3: 创建 CQ (对应 Client: setup_rdma_resources_socket 中的 ibv_create_cq)
    client_ctx->cq = ibv_create_cq(client_ctx->ctx, RDMA_CQ_SIZE, NULL, NULL, 0);
    if (!client_ctx->cq) {
        fprintf(stderr, "Failed to create CQ: %s\n", strerror(errno));
        close(client_fd);
        return -1;
    }
    
    // 步骤 4: 创建 QP (对应 Client: setup_rdma_resources_socket 中的 create_qp_socket)
    if (create_qp_socket(client_ctx) != 0) {
        close(client_fd);
        return -1;
    }
    
    // 步骤 5a: 填充本地 QP 信息 (对应 Client: setup_rdma_resources_socket 末尾)
    struct ibv_port_attr port_attr;
    ibv_query_port(client_ctx->ctx, client_ctx->port_num, &port_attr);
    
    client_ctx->local_info.qp_num = client_ctx->qp->qp_num;
    client_ctx->local_info.lid = port_attr.lid;
    client_ctx->local_info.psn = rand() & 0xFFFFFF;
    memcpy(client_ctx->local_info.gid, &client_ctx->gid, 16);
    
    // 步骤 5b: 交换 QP 信息 (对应 Client: exchange_qp_info)
    // Server 先发送本地信息，再接收 Client 信息
    // Client 同时发送本地信息，再接收 Server 信息
    if (exchange_qp_info(client_fd, &client_ctx->local_info, &client_ctx->remote_info) != 0) {
        close(client_fd);
        return -1;
    }
    
    // 步骤 6: QP 状态转换 (与 Client 同步进行)
    // 对应 Client: rdma_socket_connect 末尾的 modify_qp_to_init/rtr/rts
    if (rdma_modify_qp_to_init(client_ctx) != 0) return -1;  // RESET → INIT
    if (rdma_modify_qp_to_rtr(client_ctx) != 0) return -1;   // INIT → RTR
    if (rdma_modify_qp_to_rts(client_ctx) != 0) return -1;   // RTR → RTS
    
    client_ctx->connected = true;
    printf("RDMA connection established (Socket mode)\n");
    
    return 0;
}

// Socket 建链：断开连接
void rdma_socket_disconnect(rdma_context_t *ctx) {
    if (ctx->sock_fd >= 0) {
        close(ctx->sock_fd);
        ctx->sock_fd = -1;
    }
    ctx->connected = false;
}

// 注册内存
int rdma_register_memory(rdma_context_t *ctx, void *buf, size_t size) {
    ctx->mr = ibv_reg_mr(ctx->pd, buf, size,
                         IBV_ACCESS_LOCAL_WRITE |
                         IBV_ACCESS_REMOTE_READ |
                         IBV_ACCESS_REMOTE_WRITE |
                         IBV_ACCESS_REMOTE_ATOMIC);
    if (!ctx->mr) {
        fprintf(stderr, "Failed to register memory: %s\n", strerror(errno));
        return -1;
    }
    
    ctx->buf = buf;
    ctx->buf_size = size;
    return 0;
}

// 注销内存
void rdma_deregister_memory(rdma_context_t *ctx) {
    if (ctx->mr) {
        ibv_dereg_mr(ctx->mr);
        ctx->mr = NULL;
    }
}

// 发送数据（Send 操作）
int rdma_send(rdma_context_t *ctx, void *buf, size_t size) {
    struct ibv_sge sge = {
        .addr = (uint64_t)buf,
        .length = size,
        .lkey = ctx->mr->lkey,
    };
    
    struct ibv_send_wr wr = {
        .wr_id = (uint64_t)buf,
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_SEND,
        .send_flags = IBV_SEND_SIGNALED,
    };
    
    struct ibv_send_wr *bad_wr;
    if (ibv_post_send(ctx->qp, &wr, &bad_wr)) {
        fprintf(stderr, "Failed to post send: %s\n", strerror(errno));
        return -1;
    }
    
    return rdma_poll_completion(ctx, RDMA_TIMEOUT_MS);
}

// 接收数据（Recv 操作）
int rdma_recv(rdma_context_t *ctx, void *buf, size_t size) {
    struct ibv_sge sge = {
        .addr = (uint64_t)buf,
        .length = size,
        .lkey = ctx->mr->lkey,
    };
    
    struct ibv_recv_wr wr = {
        .wr_id = (uint64_t)buf,
        .sg_list = &sge,
        .num_sge = 1,
    };
    
    struct ibv_recv_wr *bad_wr;
    if (ibv_post_recv(ctx->qp, &wr, &bad_wr)) {
        fprintf(stderr, "Failed to post recv: %s\n", strerror(errno));
        return -1;
    }
    
    return rdma_poll_completion(ctx, RDMA_TIMEOUT_MS);
}

// RDMA Read 操作
int rdma_read(rdma_context_t *ctx, void *local_buf, size_t size,
              uint64_t remote_addr, uint32_t rkey) {
    struct ibv_sge sge = {
        .addr = (uint64_t)local_buf,
        .length = size,
        .lkey = ctx->mr->lkey,
    };
    
    struct ibv_send_wr wr = {
        .wr_id = (uint64_t)local_buf,
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_READ,
        .send_flags = IBV_SEND_SIGNALED,
        .wr.rdma = {
            .remote_addr = remote_addr,
            .rkey = rkey,
        },
    };
    
    struct ibv_send_wr *bad_wr;
    if (ibv_post_send(ctx->qp, &wr, &bad_wr)) {
        fprintf(stderr, "Failed to post RDMA read: %s\n", strerror(errno));
        return -1;
    }
    
    return rdma_poll_completion(ctx, RDMA_TIMEOUT_MS);
}

// RDMA Write 操作
int rdma_write(rdma_context_t *ctx, void *local_buf, size_t size,
               uint64_t remote_addr, uint32_t rkey) {
    struct ibv_sge sge = {
        .addr = (uint64_t)local_buf,
        .length = size,
        .lkey = ctx->mr->lkey,
    };
    
    struct ibv_send_wr wr = {
        .wr_id = (uint64_t)local_buf,
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_WRITE,
        .send_flags = IBV_SEND_SIGNALED,
        .wr.rdma = {
            .remote_addr = remote_addr,
            .rkey = rkey,
        },
    };
    
    struct ibv_send_wr *bad_wr;
    if (ibv_post_send(ctx->qp, &wr, &bad_wr)) {
        fprintf(stderr, "Failed to post RDMA write: %s\n", strerror(errno));
        return -1;
    }
    
    return rdma_poll_completion(ctx, RDMA_TIMEOUT_MS);
}

// RDMA Write with Immediate（带立即数的写操作）
// 完成后会在远端的 Recv Queue 产生一个完成事件
int rdma_write_imm(rdma_context_t *ctx, void *local_buf, size_t size,
                   uint64_t remote_addr, uint32_t rkey, uint32_t imm_data) {
    struct ibv_sge sge = {
        .addr = (uint64_t)local_buf,
        .length = size,
        .lkey = ctx->mr->lkey,
    };
    
    struct ibv_send_wr wr = {
        .wr_id = (uint64_t)local_buf,
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_RDMA_WRITE_WITH_IMM,
        .send_flags = IBV_SEND_SIGNALED,
        .imm_data = htonl(imm_data),  // 立即数需要网络字节序
        .wr.rdma = {
            .remote_addr = remote_addr,
            .rkey = rkey,
        },
    };
    
    struct ibv_send_wr *bad_wr;
    if (ibv_post_send(ctx->qp, &wr, &bad_wr)) {
        fprintf(stderr, "Failed to post RDMA write with imm: %s\n", strerror(errno));
        return -1;
    }
    
    return rdma_poll_completion(ctx, RDMA_TIMEOUT_MS);
}

// 接收带立即数的 RDMA Write 完成通知
// 当远端执行 write_imm 时，本端的 recv queue 会收到一个完成事件
int rdma_recv_imm(rdma_context_t *ctx, void *buf, size_t size, uint32_t *imm_data) {
    struct ibv_sge sge = {
        .addr = (uint64_t)buf,
        .length = size,
        .lkey = ctx->mr->lkey,
    };
    
    struct ibv_recv_wr wr = {
        .wr_id = (uint64_t)buf,
        .sg_list = &sge,
        .num_sge = 1,
    };
    
    struct ibv_recv_wr *bad_wr;
    if (ibv_post_recv(ctx->qp, &wr, &bad_wr)) {
        fprintf(stderr, "Failed to post recv for imm: %s\n", strerror(errno));
        return -1;
    }
    
    // 轮询完成队列，获取立即数
    struct ibv_wc wc;
    int polls = 0;
    int max_polls = RDMA_TIMEOUT_MS * 1000;
    
    while (polls < max_polls) {
        int ret = ibv_poll_cq(ctx->cq, 1, &wc);
        if (ret < 0) {
            fprintf(stderr, "Failed to poll CQ: %s\n", strerror(errno));
            return -1;
        }
        if (ret > 0) {
            if (wc.status != IBV_WC_SUCCESS) {
                fprintf(stderr, "Work completion error: %s\n",
                        ibv_wc_status_str(wc.status));
                return -1;
            }
            // 检查是否是 RDMA Write with Immediate
            if (wc.opcode == IBV_WC_RECV_RDMA_WITH_IMM && imm_data) {
                *imm_data = ntohl(wc.imm_data);
            }
            return 0;
        }
        usleep(1);
        polls++;
    }
    
    fprintf(stderr, "Polling timeout for recv_imm\n");
    return -1;
}

// RDMA CAS（Compare and Swap）原子操作
int rdma_cas(rdma_context_t *ctx, uint64_t remote_addr, uint32_t rkey,
             uint64_t expected, uint64_t desired, uint64_t *result) {
    // CAS 需要一个本地缓冲区存储结果
    uint64_t *local_buf = (uint64_t *)ctx->buf;
    
    struct ibv_sge sge = {
        .addr = (uint64_t)local_buf,
        .length = sizeof(uint64_t),
        .lkey = ctx->mr->lkey,
    };
    
    struct ibv_send_wr wr = {
        .wr_id = (uint64_t)local_buf,
        .sg_list = &sge,
        .num_sge = 1,
        .opcode = IBV_WR_ATOMIC_CMP_AND_SWP,
        .send_flags = IBV_SEND_SIGNALED,
        .wr.atomic = {
            .remote_addr = remote_addr,
            .rkey = rkey,
            .compare_add = expected,  // 期望值
            .swap = desired,          // 新值
        },
    };
    
    struct ibv_send_wr *bad_wr;
    if (ibv_post_send(ctx->qp, &wr, &bad_wr)) {
        fprintf(stderr, "Failed to post CAS: %s\n", strerror(errno));
        return -1;
    }
    
    int ret = rdma_poll_completion(ctx, RDMA_TIMEOUT_MS);
    if (ret == 0 && result) {
        *result = *local_buf;  // 返回原始值
    }
    
    return ret;
}

// 轮询完成队列
int rdma_poll_completion(rdma_context_t *ctx, int timeout_ms) {
    struct ibv_wc wc;
    int polls = 0;
    int max_polls = timeout_ms * 1000;  // 微秒级轮询
    
    while (polls < max_polls) {
        int ret = ibv_poll_cq(ctx->cq, 1, &wc);
        if (ret < 0) {
            fprintf(stderr, "Failed to poll CQ: %s\n", strerror(errno));
            return -1;
        }
        if (ret > 0) {
            if (wc.status != IBV_WC_SUCCESS) {
                fprintf(stderr, "Work completion error: %s\n",
                        ibv_wc_status_str(wc.status));
                return -1;
            }
            return 0;
        }
        usleep(1);
        polls++;
    }
    
    fprintf(stderr, "Polling timeout\n");
    return -1;
}