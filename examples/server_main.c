/**
 * RDMA KV Store Server Example
 * 
 * Usage: ./kv_server -a <bind_ip> -p <port> [-m <memory_mb>] [-h]
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <unistd.h>
#include <getopt.h>

#include "kv_store.h"

static kv_server_t *g_server = NULL;

static void signal_handler(int sig) {
    printf("\nReceived signal %d, shutting down...\n", sig);
    if (g_server) {
        kv_server_stop(g_server);
    }
}

static void print_usage(const char *prog) {
    printf("Usage: %s [options]\n", prog);
    printf("Options:\n");
    printf("  -a, --address <ip>     Bind IP address (default: 0.0.0.0)\n");
    printf("  -p, --port <port>      Listen port (default: 12345)\n");
    printf("  -m, --memory <mb>      Memory pool size in MB (default: 1024)\n");
    printf("  -t, --table <size>     Hash table size (default: 65536)\n");
    printf("  -w, --workers <num>    Number of worker threads (default: 4)\n");
    printf("  -H, --hugepages        Enable huge pages\n");
    printf("  -S, --socket           Use Socket connection mode (default: RDMA CM)\n");
    printf("  -d, --device <dev>     IB device name for Socket mode (default: auto)\n");
    printf("  -h, --help             Show this help message\n");
}

int main(int argc, char *argv[]) {
    kv_config_t config = {
        .bind_ip = "0.0.0.0",
        .port = 12345,
        .memory_size = 1024UL * 1024 * 1024,  // 1GB
        .use_huge_pages = false,
        .hash_table_size = 65536,
        .num_workers = 4,
        .conn_mode = CONN_MODE_CM,  // 默认使用 RDMA CM
        .ib_dev = "",
    };
    
    static struct option long_options[] = {
        {"address",   required_argument, 0, 'a'},
        {"port",      required_argument, 0, 'p'},
        {"memory",    required_argument, 0, 'm'},
        {"table",     required_argument, 0, 't'},
        {"workers",   required_argument, 0, 'w'},
        {"hugepages", no_argument,       0, 'H'},
        {"socket",    no_argument,       0, 'S'},
        {"device",    required_argument, 0, 'd'},
        {"help",      no_argument,       0, 'h'},
        {0, 0, 0, 0}
    };
    
    int opt;
    while ((opt = getopt_long(argc, argv, "a:p:m:t:w:HSd:h", long_options, NULL)) != -1) {
        switch (opt) {
            case 'a':
                strncpy(config.bind_ip, optarg, sizeof(config.bind_ip) - 1);
                break;
            case 'p':
                config.port = atoi(optarg);
                break;
            case 'm':
                config.memory_size = (size_t)atol(optarg) * 1024 * 1024;
                break;
            case 't':
                config.hash_table_size = atoi(optarg);
                break;
            case 'w':
                config.num_workers = atoi(optarg);
                break;
            case 'H':
                config.use_huge_pages = true;
                break;
            case 'S':
                config.conn_mode = CONN_MODE_SOCKET;
                break;
            case 'd':
                strncpy(config.ib_dev, optarg, sizeof(config.ib_dev) - 1);
                break;
            case 'h':
            default:
                print_usage(argv[0]);
                return opt == 'h' ? 0 : 1;
        }
    }
    
    // 设置信号处理
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);
    
    printf("=== RDMA KV Store Server ===\n");
    printf("Bind address: %s:%d\n", config.bind_ip, config.port);
    printf("Memory pool: %zu MB\n", config.memory_size / (1024 * 1024));
    printf("Hash table size: %zu\n", config.hash_table_size);
    printf("Worker threads: %d\n", config.num_workers);
    printf("Huge pages: %s\n", config.use_huge_pages ? "enabled" : "disabled");
    printf("Connection mode: %s\n", config.conn_mode == CONN_MODE_SOCKET ? "Socket" : "RDMA CM");
    if (config.conn_mode == CONN_MODE_SOCKET && strlen(config.ib_dev) > 0) {
        printf("IB device: %s\n", config.ib_dev);
    }
    printf("============================\n\n");
    
    // 创建服务端
    g_server = kv_server_create(&config);
    if (!g_server) {
        fprintf(stderr, "Failed to create KV server\n");
        return 1;
    }
    
    // 启动服务端
    if (kv_server_start(g_server) != 0) {
        fprintf(stderr, "Failed to start KV server\n");
        kv_server_destroy(g_server);
        return 1;
    }
    
    printf("Server is running. Press Ctrl+C to stop.\n\n");
    
    // 主循环：打印统计信息
    while (g_server->running) {
        sleep(10);
        if (g_server->running) {
            hash_table_print_stats(g_server->hash_table);
            memory_pool_print_stats(g_server->mem_pool);
        }
    }
    
    // 清理
    kv_server_destroy(g_server);
    g_server = NULL;
    
    printf("Server shutdown complete.\n");
    return 0;
}