#include "kv_store.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>

// 选择传输方式
transfer_mode_t select_transfer_mode(size_t data_size, bool is_write) {
    if (data_size < RDMA_SMALL_DATA_THRESHOLD) {
        return TRANSFER_SEND_RECV;
    }
    return is_write ? TRANSFER_RDMA_WRITE : TRANSFER_RDMA_READ;
}

// ===================== 服务端实现 =====================

// 处理客户端请求的工作线程
static void* server_worker_thread(void *arg) {
    kv_server_t *server = (kv_server_t *)arg;
    
    while (server->running) {
        // 接受新连接（根据建链模式选择不同的 accept 函数）
        rdma_context_t client_ctx;
        int ret;
        
        if (server->config.conn_mode == CONN_MODE_SOCKET) {
            ret = rdma_socket_accept(&server->rdma_ctx, &client_ctx);
        } else {
            ret = rdma_cm_accept(&server->rdma_ctx, &client_ctx);
        }
        
        if (ret != 0) {
            if (server->running) {
                usleep(1000);
            }
            continue;
        }
        
        printf("New client connected\n");
        
        // 注册共享内存用于 RDMA 操作
        rdma_register_memory(&client_ctx, 
                           memory_pool_get_base(server->mem_pool),
                           memory_pool_get_size(server->mem_pool));
        
        // 为该客户端分配专用写缓冲区（用于 1 RTT PUT）
        // 缓冲区大小：最大支持 1MB 的 PUT 请求
        #define CLIENT_WRITE_BUF_SIZE (1024 * 1024 + sizeof(rdma_msg_header_t) + 256)
        void *client_write_buf = memory_pool_alloc(server->mem_pool, CLIENT_WRITE_BUF_SIZE);
        
        // 发送远程内存信息和写缓冲区地址给客户端
        rdma_mem_info_t mem_info = {
            .addr = (uint64_t)memory_pool_get_base(server->mem_pool),
            .rkey = client_ctx.mr->rkey,
            .size = memory_pool_get_size(server->mem_pool),
        };
        
        client_write_buf_t write_buf_info = {
            .write_buf_addr = (uint64_t)client_write_buf,
            .write_buf_rkey = client_ctx.mr->rkey,
            .write_buf_size = CLIENT_WRITE_BUF_SIZE,
        };
        
        rdma_msg_header_t header = {
            .msg_type = MSG_TYPE_RDMA_INFO,
        };
        
        // 发送内存信息 + 写缓冲区信息
        char init_buf[512];
        memcpy(init_buf, &header, sizeof(header));
        memcpy(init_buf + sizeof(header), &mem_info, sizeof(mem_info));
        memcpy(init_buf + sizeof(header) + sizeof(mem_info), &write_buf_info, sizeof(write_buf_info));
        rdma_send(&client_ctx, init_buf, sizeof(header) + sizeof(mem_info) + sizeof(write_buf_info));
        
        // 处理客户端请求
        while (server->running && client_ctx.connected) {
            // 尝试接收请求：可能是 Send 或 Write with Immediate
            char recv_buf[8192];
            uint32_t imm_data = 0;
            
            // 使用 recv_imm 可以同时接收 Send 和 Write_imm
            if (rdma_recv_imm(&client_ctx, recv_buf, sizeof(recv_buf), &imm_data) != 0) {
                break;
            }
            
            rdma_msg_header_t *req_header;
            char *key;
            char *value;
            
            // 检查是否是 1 RTT 优化的请求（通过 write_imm 发送）
            if (imm_data == MSG_TYPE_PUT_REQUEST) {
                // 数据在预分配的写缓冲区中
                req_header = (rdma_msg_header_t *)client_write_buf;
                key = (char *)client_write_buf + sizeof(rdma_msg_header_t);
                value = key + req_header->key_len;
            } else {
                // 普通 Send/Recv 请求
                req_header = (rdma_msg_header_t *)recv_buf;
                key = recv_buf + sizeof(rdma_msg_header_t);
                value = key + req_header->key_len;
            }
            
            rdma_msg_header_t resp_header = {0};
            char resp_buf[8192];
            size_t resp_len = sizeof(rdma_msg_header_t);
            
            switch (req_header->msg_type) {
                case MSG_TYPE_GET_REQUEST: {
                    kv_result_t result = hash_table_get(server->hash_table, 
                                                       key, req_header->key_len);
                    resp_header.msg_type = MSG_TYPE_GET_RESPONSE;
                    resp_header.status = result.status;
                    resp_header.version = result.version;
                    
                    if (result.status == KV_OK) {
                        resp_header.value_len = result.value_len;
                        
                        // 小数据直接在响应中返回
                        if (result.value_len < RDMA_SMALL_DATA_THRESHOLD) {
                            memcpy(resp_buf + sizeof(rdma_msg_header_t), 
                                   result.value, result.value_len);
                            resp_len += result.value_len;
                        }
                        // 大数据：客户端将使用 RDMA Read
                    }
                    break;
                }
                
                case MSG_TYPE_PUT_REQUEST: {
                    // 检查是否是 1 RTT 优化模式（通过 write_imm 发送）
                    if (imm_data == MSG_TYPE_PUT_REQUEST) {
                        // 1 RTT 模式：数据已经在预分配缓冲区中，直接存储
                        kv_result_t result = hash_table_put(server->hash_table,
                                                           key, req_header->key_len,
                                                           value, req_header->value_len);
                        resp_header.msg_type = MSG_TYPE_PUT_RESPONSE;
                        resp_header.status = result.status;
                        resp_header.version = result.version;
                    } else if (req_header->value_len < RDMA_SMALL_DATA_THRESHOLD) {
                        // 小数据：通过 Send/Recv 发送，value 在 recv_buf 中
                        kv_result_t result = hash_table_put(server->hash_table,
                                                           key, req_header->key_len,
                                                           value, req_header->value_len);
                        resp_header.msg_type = MSG_TYPE_PUT_RESPONSE;
                        resp_header.status = result.status;
                        resp_header.version = result.version;
                    } else {
                        // 旧模式大数据（兼容保留）：2 RTT 流程
                        void *value_buf = memory_pool_alloc(server->mem_pool, req_header->value_len);
                        if (!value_buf) {
                            resp_header.msg_type = MSG_TYPE_PUT_RESPONSE;
                            resp_header.status = KV_ERR_NO_MEMORY;
                        } else {
                            put_alloc_info_t alloc_info = {
                                .remote_addr = (uint64_t)value_buf,
                                .rkey = client_ctx.mr->rkey,
                                .value_offset = (uint64_t)value_buf - (uint64_t)memory_pool_get_base(server->mem_pool),
                            };
                            
                            rdma_msg_header_t alloc_resp = {
                                .msg_type = MSG_TYPE_PUT_ALLOC_RESPONSE,
                                .key_len = req_header->key_len,
                                .value_len = req_header->value_len,
                                .status = KV_OK,
                            };
                            
                            char alloc_resp_buf[512];
                            memcpy(alloc_resp_buf, &alloc_resp, sizeof(alloc_resp));
                            memcpy(alloc_resp_buf + sizeof(alloc_resp), &alloc_info, sizeof(alloc_info));
                            rdma_send(&client_ctx, alloc_resp_buf, sizeof(alloc_resp) + sizeof(alloc_info));
                            
                            char dummy_buf[64];
                            uint32_t complete_imm = 0;
                            if (rdma_recv_imm(&client_ctx, dummy_buf, sizeof(dummy_buf), &complete_imm) != 0 ||
                                complete_imm != MSG_TYPE_PUT_COMPLETE) {
                                memory_pool_free(server->mem_pool, value_buf);
                                resp_header.msg_type = MSG_TYPE_PUT_RESPONSE;
                                resp_header.status = KV_ERR_INTERNAL;
                                break;
                            }
                            
                            kv_result_t result = hash_table_put_with_allocated(
                                server->hash_table,
                                key, req_header->key_len,
                                value_buf, req_header->value_len,
                                alloc_info.value_offset);
                            
                            resp_header.msg_type = MSG_TYPE_PUT_RESPONSE;
                            resp_header.status = result.status;
                            resp_header.version = result.version;
                        }
                    }
                    break;
                }
                
                case MSG_TYPE_DELETE_REQUEST: {
                    kv_result_t result = hash_table_delete(server->hash_table,
                                                          key, req_header->key_len);
                    resp_header.msg_type = MSG_TYPE_DELETE_RESPONSE;
                    resp_header.status = result.status;
                    resp_header.version = result.version;
                    break;
                }
                
                case MSG_TYPE_CAS_REQUEST: {
                    kv_result_t result = hash_table_cas(server->hash_table,
                                                       key, req_header->key_len,
                                                       req_header->version,
                                                       value, req_header->value_len);
                    resp_header.msg_type = MSG_TYPE_CAS_RESPONSE;
                    resp_header.status = result.status;
                    resp_header.version = result.version;
                    break;
                }
                
                default:
                    resp_header.status = KV_ERR_INTERNAL;
                    break;
            }
            
            memcpy(resp_buf, &resp_header, sizeof(resp_header));
            rdma_send(&client_ctx, resp_buf, resp_len);
        }
        
        // 根据模式断开连接
        if (server->config.conn_mode == CONN_MODE_SOCKET) {
            rdma_socket_disconnect(&client_ctx);
        } else {
            rdma_cm_disconnect(&client_ctx);
        }
        printf("Client disconnected\n");
    }
    
    return NULL;
}

// 创建服务端
kv_server_t* kv_server_create(const kv_config_t *config) {
    kv_server_t *server = (kv_server_t *)calloc(1, sizeof(kv_server_t));
    if (!server) {
        return NULL;
    }
    
    memcpy(&server->config, config, sizeof(kv_config_t));
    
    // 创建内存池
    server->mem_pool = memory_pool_create(config->memory_size, config->use_huge_pages);
    if (!server->mem_pool) {
        fprintf(stderr, "Failed to create memory pool\n");
        free(server);
        return NULL;
    }
    
    // 创建哈希表
    server->hash_table = hash_table_create(config->hash_table_size, server->mem_pool);
    if (!server->hash_table) {
        fprintf(stderr, "Failed to create hash table\n");
        memory_pool_destroy(server->mem_pool);
        free(server);
        return NULL;
    }
    
    // 初始化 RDMA 上下文
    if (rdma_init_context(&server->rdma_ctx, config->conn_mode) != 0) {
        fprintf(stderr, "Failed to initialize RDMA context\n");
        hash_table_destroy(server->hash_table);
        memory_pool_destroy(server->mem_pool);
        free(server);
        return NULL;
    }
    
    // 初始化客户端管理
    pthread_mutex_init(&server->clients.lock, NULL);
    server->clients.capacity = 64;
    server->clients.contexts = (rdma_context_t *)calloc(
        server->clients.capacity, sizeof(rdma_context_t));
    
    return server;
}

// 销毁服务端
void kv_server_destroy(kv_server_t *server) {
    if (!server) return;
    
    kv_server_stop(server);
    
    hash_table_destroy(server->hash_table);
    memory_pool_destroy(server->mem_pool);
    rdma_destroy_context(&server->rdma_ctx);
    
    pthread_mutex_destroy(&server->clients.lock);
    free(server->clients.contexts);
    free(server->workers);
    free(server);
}

// 启动服务端
int kv_server_start(kv_server_t *server) {
    if (!server) return -1;
    
    // 开始监听（根据建链模式选择）
    int ret;
    if (server->config.conn_mode == CONN_MODE_SOCKET) {
        const char *ib_dev = strlen(server->config.ib_dev) > 0 ? server->config.ib_dev : NULL;
        ret = rdma_socket_listen(&server->rdma_ctx, server->config.bind_ip, 
                                 server->config.port, ib_dev);
    } else {
        ret = rdma_cm_listen(&server->rdma_ctx, server->config.bind_ip, 
                          server->config.port);
    }
    
    if (ret != 0) {
        return -1;
    }
    
    server->running = true;
    
    // 创建工作线程
    int num_workers = server->config.num_workers > 0 ? server->config.num_workers : 4;
    server->workers = (pthread_t *)malloc(num_workers * sizeof(pthread_t));
    
    for (int i = 0; i < num_workers; i++) {
        pthread_create(&server->workers[i], NULL, server_worker_thread, server);
    }
    
    printf("KV server started with %d workers\n", num_workers);
    return 0;
}

// 停止服务端
void kv_server_stop(kv_server_t *server) {
    if (!server || !server->running) return;
    
    server->running = false;
    
    // 等待工作线程结束
    int num_workers = server->config.num_workers > 0 ? server->config.num_workers : 4;
    for (int i = 0; i < num_workers; i++) {
        pthread_join(server->workers[i], NULL);
    }
    
    printf("KV server stopped\n");
}

// ===================== 客户端实现 =====================

// 创建客户端
kv_client_t* kv_client_create(conn_mode_t mode) {
    kv_client_t *client = (kv_client_t *)calloc(1, sizeof(kv_client_t));
    if (!client) {
        return NULL;
    }
    
    // 创建本地内存池（用于 RDMA 操作缓冲区）
    client->mem_pool = memory_pool_create(64 * 1024 * 1024, false);  // 64MB
    if (!client->mem_pool) {
        free(client);
        return NULL;
    }
    
    if (rdma_init_context(&client->rdma_ctx, mode) != 0) {
        memory_pool_destroy(client->mem_pool);
        free(client);
        return NULL;
    }
    
    return client;
}

// 销毁客户端
void kv_client_destroy(kv_client_t *client) {
    if (!client) return;
    
    kv_client_disconnect(client);
    memory_pool_destroy(client->mem_pool);
    rdma_destroy_context(&client->rdma_ctx);
    free(client);
}

// 连接服务端
int kv_client_connect(kv_client_t *client, const char *server_ip, int port) {
    if (!client || !server_ip) return -1;
    
    strncpy(client->server_ip, server_ip, sizeof(client->server_ip) - 1);
    client->server_port = port;
    
    // 建立 RDMA 连接
    if (rdma_cm_connect(&client->rdma_ctx, server_ip, port) != 0) {
        return -1;
    }
    
    // 注册本地内存
    if (rdma_register_memory(&client->rdma_ctx,
                            memory_pool_get_base(client->mem_pool),
                            memory_pool_get_size(client->mem_pool)) != 0) {
        rdma_cm_disconnect(&client->rdma_ctx);
        return -1;
    }
    
    // 接收服务端的远程内存信息
    char recv_buf[1024];
    if (rdma_recv(&client->rdma_ctx, recv_buf, sizeof(recv_buf)) != 0) {
        rdma_cm_disconnect(&client->rdma_ctx);
        return -1;
    }
    
    rdma_msg_header_t *header = (rdma_msg_header_t *)recv_buf;
    if (header->msg_type == MSG_TYPE_RDMA_INFO) {
        rdma_mem_info_t *mem_info = (rdma_mem_info_t *)(recv_buf + sizeof(rdma_msg_header_t));
        client->rdma_ctx.remote_mem = *mem_info;
        
        // 解析写缓冲区信息（用于 1 RTT PUT）
        client_write_buf_t *write_buf = (client_write_buf_t *)(recv_buf + sizeof(rdma_msg_header_t) + sizeof(rdma_mem_info_t));
        client->write_buf = *write_buf;
        printf("Got write buffer: addr=0x%lx, size=%u\n", 
               client->write_buf.write_buf_addr, client->write_buf.write_buf_size);
    }
    
    client->connected = true;
    printf("Connected to KV server at %s:%d\n", server_ip, port);
    
    return 0;
}

// 断开连接
void kv_client_disconnect(kv_client_t *client) {
    if (!client || !client->connected) return;
    
    if (client->rdma_ctx.conn_mode == CONN_MODE_SOCKET) {
        rdma_socket_disconnect(&client->rdma_ctx);
    } else {
        rdma_cm_disconnect(&client->rdma_ctx);
    }
    client->connected = false;
}

// 连接服务端 (Socket 模式)
int kv_client_connect_socket(kv_client_t *client, const char *server_ip, int port, const char *ib_dev) {
    if (!client || !server_ip) return -1;
    
    strncpy(client->server_ip, server_ip, sizeof(client->server_ip) - 1);
    client->server_port = port;
    
    // 建立 RDMA 连接 (Socket 模式)
    if (rdma_socket_connect(&client->rdma_ctx, server_ip, port, ib_dev) != 0) {
        return -1;
    }
    
    // 注册本地内存
    if (rdma_register_memory(&client->rdma_ctx,
                            memory_pool_get_base(client->mem_pool),
                            memory_pool_get_size(client->mem_pool)) != 0) {
        rdma_socket_disconnect(&client->rdma_ctx);
        return -1;
    }
    
    // 接收服务端的远程内存信息
    char recv_buf[1024];
    if (rdma_recv(&client->rdma_ctx, recv_buf, sizeof(recv_buf)) != 0) {
        rdma_socket_disconnect(&client->rdma_ctx);
        return -1;
    }
    
    rdma_msg_header_t *header = (rdma_msg_header_t *)recv_buf;
    if (header->msg_type == MSG_TYPE_RDMA_INFO) {
        rdma_mem_info_t *mem_info = (rdma_mem_info_t *)(recv_buf + sizeof(rdma_msg_header_t));
        client->rdma_ctx.remote_mem = *mem_info;
        
        // 解析写缓冲区信息（用于 1 RTT PUT）
        client_write_buf_t *write_buf = (client_write_buf_t *)(recv_buf + sizeof(rdma_msg_header_t) + sizeof(rdma_mem_info_t));
        client->write_buf = *write_buf;
        printf("Got write buffer: addr=0x%lx, size=%u\n", 
               client->write_buf.write_buf_addr, client->write_buf.write_buf_size);
    }
    
    client->connected = true;
    printf("Connected to KV server at %s:%d (Socket mode)\n", server_ip, port);
    
    return 0;
}

// GET 操作
int kv_client_get(kv_client_t *client, const char *key, size_t key_len,
                  void *value_buf, size_t *value_len) {
    if (!client || !client->connected || !key || !value_buf || !value_len) {
        return KV_ERR_INTERNAL;
    }
    
    // 准备请求
    char *send_buf = (char *)memory_pool_alloc(client->mem_pool, 4096);
    rdma_msg_header_t *header = (rdma_msg_header_t *)send_buf;
    header->msg_type = MSG_TYPE_GET_REQUEST;
    header->key_len = key_len;
    header->value_len = 0;
    
    memcpy(send_buf + sizeof(rdma_msg_header_t), key, key_len);
    
    // 发送请求
    if (rdma_send(&client->rdma_ctx, send_buf, 
                  sizeof(rdma_msg_header_t) + key_len) != 0) {
        memory_pool_free(client->mem_pool, send_buf);
        return KV_ERR_INTERNAL;
    }
    
    // 接收响应
    char *recv_buf = (char *)memory_pool_alloc(client->mem_pool, 8192);
    if (rdma_recv(&client->rdma_ctx, recv_buf, 8192) != 0) {
        memory_pool_free(client->mem_pool, send_buf);
        memory_pool_free(client->mem_pool, recv_buf);
        return KV_ERR_INTERNAL;
    }
    
    rdma_msg_header_t *resp = (rdma_msg_header_t *)recv_buf;
    
    if (resp->status == KV_OK) {
        if (resp->value_len < RDMA_SMALL_DATA_THRESHOLD) {
            // 小数据：直接从响应中复制
            if (*value_len < resp->value_len) {
                memory_pool_free(client->mem_pool, send_buf);
                memory_pool_free(client->mem_pool, recv_buf);
                return KV_ERR_INTERNAL;
            }
            memcpy(value_buf, recv_buf + sizeof(rdma_msg_header_t), resp->value_len);
            *value_len = resp->value_len;
        } else {
            // 大数据：使用 RDMA Read 从服务端直接读取
            // 需要先获取远程地址（这里简化处理）
            // 实际应该在响应中包含远程地址信息
        }
    }
    
    int status = resp->status;
    memory_pool_free(client->mem_pool, send_buf);
    memory_pool_free(client->mem_pool, recv_buf);
    
    return status;
}

// PUT 操作
int kv_client_put(kv_client_t *client, const char *key, size_t key_len,
                  const void *value, size_t value_len) {
    if (!client || !client->connected || !key || !value) {
        return KV_ERR_INTERNAL;
    }
    
    transfer_mode_t mode = select_transfer_mode(value_len, true);
    
    if (mode == TRANSFER_SEND_RECV) {
        // 小数据：使用 Send/Recv
        size_t buf_size = sizeof(rdma_msg_header_t) + key_len + value_len;
        char *send_buf = (char *)memory_pool_alloc(client->mem_pool, buf_size);
        
        rdma_msg_header_t *header = (rdma_msg_header_t *)send_buf;
        header->msg_type = MSG_TYPE_PUT_REQUEST;
        header->key_len = key_len;
        header->value_len = value_len;
        
        memcpy(send_buf + sizeof(rdma_msg_header_t), key, key_len);
        memcpy(send_buf + sizeof(rdma_msg_header_t) + key_len, value, value_len);
        
        if (rdma_send(&client->rdma_ctx, send_buf, buf_size) != 0) {
            memory_pool_free(client->mem_pool, send_buf);
            return KV_ERR_INTERNAL;
        }
        
        // 接收响应
        char recv_buf[256];
        if (rdma_recv(&client->rdma_ctx, recv_buf, sizeof(recv_buf)) != 0) {
            memory_pool_free(client->mem_pool, send_buf);
            return KV_ERR_INTERNAL;
        }
        
        rdma_msg_header_t *resp = (rdma_msg_header_t *)recv_buf;
        memory_pool_free(client->mem_pool, send_buf);
        
        return resp->status;
    } else {
        // 大数据：使用 1 RTT 优化（write_imm -> recv）
        // 直接写入服务端预分配的写缓冲区
        
        // 检查数据是否超过写缓冲区大小
        size_t total_size = sizeof(rdma_msg_header_t) + key_len + value_len;
        if (total_size > client->write_buf.write_buf_size) {
            return KV_ERR_VALUE_TOO_LONG;
        }
        
        // Step 1: 构建请求数据 [header][key][value]
        void *local_buf = memory_pool_alloc(client->mem_pool, total_size);
        if (!local_buf) {
            return KV_ERR_NO_MEMORY;
        }
        
        rdma_msg_header_t *header = (rdma_msg_header_t *)local_buf;
        header->msg_type = MSG_TYPE_PUT_REQUEST;
        header->key_len = key_len;
        header->value_len = value_len;
        
        memcpy((char *)local_buf + sizeof(rdma_msg_header_t), key, key_len);
        memcpy((char *)local_buf + sizeof(rdma_msg_header_t) + key_len, value, value_len);
        
        // Step 2: 使用 write_imm 直接写入服务端预分配缓冲区
        // 立即数用于通知服务端有新请求
        uint32_t imm_data = MSG_TYPE_PUT_REQUEST;
        if (rdma_write_imm(&client->rdma_ctx, local_buf, total_size,
                           client->write_buf.write_buf_addr,
                           client->write_buf.write_buf_rkey,
                           imm_data) != 0) {
            memory_pool_free(client->mem_pool, local_buf);
            return KV_ERR_INTERNAL;
        }
        
        // Step 3: 接收响应
        char recv_buf[256];
        if (rdma_recv(&client->rdma_ctx, recv_buf, sizeof(recv_buf)) != 0) {
            memory_pool_free(client->mem_pool, local_buf);
            return KV_ERR_INTERNAL;
        }
        
        rdma_msg_header_t *resp = (rdma_msg_header_t *)recv_buf;
        int status = resp->status;
        
        memory_pool_free(client->mem_pool, local_buf);
        return status;
    }
}

// DELETE 操作
int kv_client_delete(kv_client_t *client, const char *key, size_t key_len) {
    if (!client || !client->connected || !key) {
        return KV_ERR_INTERNAL;
    }
    
    char *send_buf = (char *)memory_pool_alloc(client->mem_pool, 
                                                sizeof(rdma_msg_header_t) + key_len);
    rdma_msg_header_t *header = (rdma_msg_header_t *)send_buf;
    header->msg_type = MSG_TYPE_DELETE_REQUEST;
    header->key_len = key_len;
    header->value_len = 0;
    
    memcpy(send_buf + sizeof(rdma_msg_header_t), key, key_len);
    
    if (rdma_send(&client->rdma_ctx, send_buf, 
                  sizeof(rdma_msg_header_t) + key_len) != 0) {
        memory_pool_free(client->mem_pool, send_buf);
        return KV_ERR_INTERNAL;
    }
    
    char recv_buf[256];
    if (rdma_recv(&client->rdma_ctx, recv_buf, sizeof(recv_buf)) != 0) {
        memory_pool_free(client->mem_pool, send_buf);
        return KV_ERR_INTERNAL;
    }
    
    rdma_msg_header_t *resp = (rdma_msg_header_t *)recv_buf;
    memory_pool_free(client->mem_pool, send_buf);
    
    return resp->status;
}

// CAS 操作
int kv_client_cas(kv_client_t *client, const char *key, size_t key_len,
                  uint64_t expected_version,
                  const void *new_value, size_t new_value_len,
                  uint64_t *new_version) {
    if (!client || !client->connected || !key || !new_value) {
        return KV_ERR_INTERNAL;
    }
    
    size_t buf_size = sizeof(rdma_msg_header_t) + key_len + new_value_len;
    char *send_buf = (char *)memory_pool_alloc(client->mem_pool, buf_size);
    
    rdma_msg_header_t *header = (rdma_msg_header_t *)send_buf;
    header->msg_type = MSG_TYPE_CAS_REQUEST;
    header->key_len = key_len;
    header->value_len = new_value_len;
    header->version = expected_version;
    
    memcpy(send_buf + sizeof(rdma_msg_header_t), key, key_len);
    memcpy(send_buf + sizeof(rdma_msg_header_t) + key_len, new_value, new_value_len);
    
    if (rdma_send(&client->rdma_ctx, send_buf, buf_size) != 0) {
        memory_pool_free(client->mem_pool, send_buf);
        return KV_ERR_INTERNAL;
    }
    
    char recv_buf[256];
    if (rdma_recv(&client->rdma_ctx, recv_buf, sizeof(recv_buf)) != 0) {
        memory_pool_free(client->mem_pool, send_buf);
        return KV_ERR_INTERNAL;
    }
    
    rdma_msg_header_t *resp = (rdma_msg_header_t *)recv_buf;
    
    if (new_version) {
        *new_version = resp->version;
    }
    
    memory_pool_free(client->mem_pool, send_buf);
    return resp->status;
}

// 批量 GET
int kv_client_batch_get(kv_client_t *client,
                        const char **keys, const size_t *key_lens,
                        void **values, size_t *value_lens,
                        int count) {
    if (!client || !client->connected || count <= 0) {
        return KV_ERR_INTERNAL;
    }
    
    int success_count = 0;
    
    // 简单实现：串行执行
    // 优化版本应该使用流水线或批量 WR
    for (int i = 0; i < count; i++) {
        int ret = kv_client_get(client, keys[i], key_lens[i], 
                                values[i], &value_lens[i]);
        if (ret == KV_OK) {
            success_count++;
        }
    }
    
    return success_count;
}

// 批量 PUT
int kv_client_batch_put(kv_client_t *client,
                        const char **keys, const size_t *key_lens,
                        const void **values, const size_t *value_lens,
                        int count) {
    if (!client || !client->connected || count <= 0) {
        return KV_ERR_INTERNAL;
    }
    
    int success_count = 0;
    
    // 简单实现：串行执行
    for (int i = 0; i < count; i++) {
        int ret = kv_client_put(client, keys[i], key_lens[i],
                                values[i], value_lens[i]);
        if (ret == KV_OK) {
            success_count++;
        }
    }
    
    return success_count;
}