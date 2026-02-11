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
            ret = rdma_accept(&server->rdma_ctx, &client_ctx);
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
        
        // 发送远程内存信息给客户端
        rdma_mem_info_t mem_info = {
            .addr = (uint64_t)memory_pool_get_base(server->mem_pool),
            .rkey = client_ctx.mr->rkey,
            .size = memory_pool_get_size(server->mem_pool),
        };
        
        rdma_msg_header_t header = {
            .msg_type = MSG_TYPE_RDMA_INFO,
        };
        
        // 发送内存信息
        memcpy(client_ctx.buf, &header, sizeof(header));
        memcpy((char *)client_ctx.buf + sizeof(header), &mem_info, sizeof(mem_info));
        rdma_send(&client_ctx, client_ctx.buf, sizeof(header) + sizeof(mem_info));
        
        // 处理客户端请求
        while (server->running && client_ctx.connected) {
            // 预先 post recv
            char recv_buf[8192];
            if (rdma_recv(&client_ctx, recv_buf, sizeof(recv_buf)) != 0) {
                break;
            }
            
            rdma_msg_header_t *req_header = (rdma_msg_header_t *)recv_buf;
            char *key = recv_buf + sizeof(rdma_msg_header_t);
            char *value = key + req_header->key_len;
            
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
                    kv_result_t result = hash_table_put(server->hash_table,
                                                       key, req_header->key_len,
                                                       value, req_header->value_len);
                    resp_header.msg_type = MSG_TYPE_PUT_RESPONSE;
                    resp_header.status = result.status;
                    resp_header.version = result.version;
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
            rdma_disconnect(&client_ctx);
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
        ret = rdma_listen(&server->rdma_ctx, server->config.bind_ip, 
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
    if (rdma_connect(&client->rdma_ctx, server_ip, port) != 0) {
        return -1;
    }
    
    // 注册本地内存
    if (rdma_register_memory(&client->rdma_ctx,
                            memory_pool_get_base(client->mem_pool),
                            memory_pool_get_size(client->mem_pool)) != 0) {
        rdma_disconnect(&client->rdma_ctx);
        return -1;
    }
    
    // 接收服务端的远程内存信息
    char recv_buf[1024];
    if (rdma_recv(&client->rdma_ctx, recv_buf, sizeof(recv_buf)) != 0) {
        rdma_disconnect(&client->rdma_ctx);
        return -1;
    }
    
    rdma_msg_header_t *header = (rdma_msg_header_t *)recv_buf;
    if (header->msg_type == MSG_TYPE_RDMA_INFO) {
        rdma_mem_info_t *mem_info = (rdma_mem_info_t *)(recv_buf + sizeof(rdma_msg_header_t));
        client->rdma_ctx.remote_mem = *mem_info;
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
        rdma_disconnect(&client->rdma_ctx);
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
        // 大数据：使用 RDMA Write
        // 1. 先请求服务端分配远程内存
        // 2. 使用 RDMA Write 直接写入
        // 3. 发送完成通知
        
        // 这里简化实现，实际需要更复杂的协议
        char *send_buf = (char *)memory_pool_alloc(client->mem_pool, 
                                                    sizeof(rdma_msg_header_t) + key_len);
        rdma_msg_header_t *header = (rdma_msg_header_t *)send_buf;
        header->msg_type = MSG_TYPE_PUT_REQUEST;
        header->key_len = key_len;
        header->value_len = value_len;
        
        memcpy(send_buf + sizeof(rdma_msg_header_t), key, key_len);
        
        // 发送元数据
        rdma_send(&client->rdma_ctx, send_buf, sizeof(rdma_msg_header_t) + key_len);
        
        // 使用 RDMA Write 写入数据
        // rdma_write(&client->rdma_ctx, value, value_len, remote_addr, remote_rkey);
        
        // 接收确认
        char recv_buf[256];
        rdma_recv(&client->rdma_ctx, recv_buf, sizeof(recv_buf));
        
        rdma_msg_header_t *resp = (rdma_msg_header_t *)recv_buf;
        memory_pool_free(client->mem_pool, send_buf);
        
        return resp->status;
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