/**
 * RDMA KV Store Client Example
 * 
 * Usage: ./kv_client -a <server_ip> -p <port>
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>
#include <time.h>

#include "kv_store.h"

static void print_usage(const char *prog) {
    printf("Usage: %s [options]\n", prog);
    printf("Options:\n");
    printf("  -a, --address <ip>   Server IP address (required)\n");
    printf("  -p, --port <port>    Server port (default: 12345)\n");
    printf("  -S, --socket         Use Socket connection mode (default: RDMA CM)\n");
    printf("  -d, --device <dev>   IB device name for Socket mode (default: auto)\n");
    printf("  -h, --help           Show this help message\n");
}

// 简单的性能测试
static void run_benchmark(kv_client_t *client, int num_ops) {
    printf("\n=== Running Benchmark ===\n");
    printf("Operations: %d\n", num_ops);
    
    char key[64];
    char value[1024];
    char read_buf[1024];
    size_t read_len;
    
    // 填充测试数据
    memset(value, 'A', sizeof(value));
    
    // PUT 测试
    clock_t start = clock();
    int put_success = 0;
    
    for (int i = 0; i < num_ops; i++) {
        snprintf(key, sizeof(key), "benchmark_key_%08d", i);
        if (kv_client_put(client, key, strlen(key), value, sizeof(value)) == KV_OK) {
            put_success++;
        }
    }
    
    clock_t end = clock();
    double put_time = (double)(end - start) / CLOCKS_PER_SEC;
    printf("\nPUT: %d/%d successful in %.3f seconds\n", put_success, num_ops, put_time);
    printf("PUT throughput: %.2f ops/sec\n", num_ops / put_time);
    
    // GET 测试
    start = clock();
    int get_success = 0;
    
    for (int i = 0; i < num_ops; i++) {
        snprintf(key, sizeof(key), "benchmark_key_%08d", i);
        read_len = sizeof(read_buf);
        if (kv_client_get(client, key, strlen(key), read_buf, &read_len) == KV_OK) {
            get_success++;
        }
    }
    
    end = clock();
    double get_time = (double)(end - start) / CLOCKS_PER_SEC;
    printf("\nGET: %d/%d successful in %.3f seconds\n", get_success, num_ops, get_time);
    printf("GET throughput: %.2f ops/sec\n", num_ops / get_time);
    
    // DELETE 测试
    start = clock();
    int del_success = 0;
    
    for (int i = 0; i < num_ops; i++) {
        snprintf(key, sizeof(key), "benchmark_key_%08d", i);
        if (kv_client_delete(client, key, strlen(key)) == KV_OK) {
            del_success++;
        }
    }
    
    end = clock();
    double del_time = (double)(end - start) / CLOCKS_PER_SEC;
    printf("\nDELETE: %d/%d successful in %.3f seconds\n", del_success, num_ops, del_time);
    printf("DELETE throughput: %.2f ops/sec\n", num_ops / del_time);
    
    printf("\n=========================\n");
}

// 交互式命令行
static void interactive_mode(kv_client_t *client) {
    char line[4096];
    char cmd[32];
    char key[256];
    char value[4096];
    
    printf("\nInteractive mode. Commands:\n");
    printf("  put <key> <value>   - Store a key-value pair\n");
    printf("  get <key>           - Retrieve a value\n");
    printf("  del <key>           - Delete a key\n");
    printf("  cas <key> <ver> <value> - Compare and swap\n");
    printf("  bench <num>         - Run benchmark with <num> operations\n");
    printf("  quit                - Exit\n\n");
    
    while (1) {
        printf("kv> ");
        fflush(stdout);
        
        if (!fgets(line, sizeof(line), stdin)) {
            break;
        }
        
        // 解析命令
        if (sscanf(line, "%31s", cmd) != 1) {
            continue;
        }
        
        if (strcmp(cmd, "quit") == 0 || strcmp(cmd, "exit") == 0) {
            break;
        }
        
        if (strcmp(cmd, "put") == 0) {
            if (sscanf(line, "%*s %255s %4095[^\n]", key, value) != 2) {
                printf("Usage: put <key> <value>\n");
                continue;
            }
            
            int ret = kv_client_put(client, key, strlen(key), value, strlen(value));
            if (ret == KV_OK) {
                printf("OK\n");
            } else {
                printf("ERROR: %d\n", ret);
            }
        }
        else if (strcmp(cmd, "get") == 0) {
            if (sscanf(line, "%*s %255s", key) != 1) {
                printf("Usage: get <key>\n");
                continue;
            }
            
            char read_buf[4096];
            size_t read_len = sizeof(read_buf);
            
            int ret = kv_client_get(client, key, strlen(key), read_buf, &read_len);
            if (ret == KV_OK) {
                read_buf[read_len] = '\0';
                printf("%s\n", read_buf);
            } else if (ret == KV_ERR_NOT_FOUND) {
                printf("(nil)\n");
            } else {
                printf("ERROR: %d\n", ret);
            }
        }
        else if (strcmp(cmd, "del") == 0) {
            if (sscanf(line, "%*s %255s", key) != 1) {
                printf("Usage: del <key>\n");
                continue;
            }
            
            int ret = kv_client_delete(client, key, strlen(key));
            if (ret == KV_OK) {
                printf("OK\n");
            } else if (ret == KV_ERR_NOT_FOUND) {
                printf("(nil)\n");
            } else {
                printf("ERROR: %d\n", ret);
            }
        }
        else if (strcmp(cmd, "cas") == 0) {
            uint64_t expected_ver;
            if (sscanf(line, "%*s %255s %lu %4095[^\n]", key, &expected_ver, value) != 3) {
                printf("Usage: cas <key> <expected_version> <new_value>\n");
                continue;
            }
            
            uint64_t new_ver;
            int ret = kv_client_cas(client, key, strlen(key), expected_ver,
                                    value, strlen(value), &new_ver);
            if (ret == KV_OK) {
                printf("OK (new version: %lu)\n", new_ver);
            } else if (ret == KV_ERR_CAS_FAILED) {
                printf("CAS FAILED (current version: %lu)\n", new_ver);
            } else if (ret == KV_ERR_NOT_FOUND) {
                printf("(nil)\n");
            } else {
                printf("ERROR: %d\n", ret);
            }
        }
        else if (strcmp(cmd, "bench") == 0) {
            int num_ops = 1000;
            sscanf(line, "%*s %d", &num_ops);
            run_benchmark(client, num_ops);
        }
        else {
            printf("Unknown command: %s\n", cmd);
        }
    }
}

int main(int argc, char *argv[]) {
    char server_ip[64] = "";
    int port = 12345;
    conn_mode_t conn_mode = CONN_MODE_CM;  // 默认使用 RDMA CM
    char ib_dev[64] = "";
    
    static struct option long_options[] = {
        {"address", required_argument, 0, 'a'},
        {"port",    required_argument, 0, 'p'},
        {"socket",  no_argument,       0, 'S'},
        {"device",  required_argument, 0, 'd'},
        {"help",    no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };
    
    int opt;
    while ((opt = getopt_long(argc, argv, "a:p:Sd:h", long_options, NULL)) != -1) {
        switch (opt) {
            case 'a':
                strncpy(server_ip, optarg, sizeof(server_ip) - 1);
                break;
            case 'p':
                port = atoi(optarg);
                break;
            case 'S':
                conn_mode = CONN_MODE_SOCKET;
                break;
            case 'd':
                strncpy(ib_dev, optarg, sizeof(ib_dev) - 1);
                break;
            case 'h':
            default:
                print_usage(argv[0]);
                return opt == 'h' ? 0 : 1;
        }
    }
    
    if (strlen(server_ip) == 0) {
        fprintf(stderr, "Error: Server address is required\n");
        print_usage(argv[0]);
        return 1;
    }
    
    printf("=== RDMA KV Store Client ===\n");
    printf("Connecting to %s:%d (%s mode)...\n", server_ip, port, 
           conn_mode == CONN_MODE_SOCKET ? "Socket" : "RDMA CM");
    
    // 创建客户端
    kv_client_t *client = kv_client_create(conn_mode);
    if (!client) {
        fprintf(stderr, "Failed to create KV client\n");
        return 1;
    }
    
    // 连接服务端（根据模式选择）
    int ret;
    if (conn_mode == CONN_MODE_SOCKET) {
        const char *dev = strlen(ib_dev) > 0 ? ib_dev : NULL;
        ret = kv_client_connect_socket(client, server_ip, port, dev);
    } else {
        ret = kv_client_connect(client, server_ip, port);
    }
    
    if (ret != 0) {
        fprintf(stderr, "Failed to connect to server\n");
        kv_client_destroy(client);
        return 1;
    }
    
    printf("Connected!\n");
    
    // 进入交互模式
    interactive_mode(client);
    
    // 清理
    kv_client_disconnect(client);
    kv_client_destroy(client);
    
    printf("Goodbye!\n");
    return 0;
}