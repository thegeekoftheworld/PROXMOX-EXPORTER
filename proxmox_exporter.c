#include <arpa/inet.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <math.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define DEFAULT_PORT 9109
#define LISTEN_BACKLOG 32
#define MAX_METRICS (16 * 1024 * 1024)
#define MAX_LINE 4096
#define MAX_LABEL 512
#define MAX_PATH 1024
#define MAX_CPU 512
#define MAX_IFACE 256
#define MAX_DISK 512
#define MAX_GUEST 2048
#define MAX_SMART_DEV 256
#define SAMPLE_USEC 200000

#define FAST_INTERVAL_SEC   10
#define GUEST_INTERVAL_SEC  30
#define SMART_INTERVAL_SEC  300
#define ZFS_INTERVAL_SEC    30
#define CEPH_INTERVAL_SEC   60

#define GUEST_WORKERS       8

struct buffer {
    char *data;
    size_t len;
    size_t cap;
};

struct cpu_times {
    char name[32];
    unsigned long long user, nice, system, idle, iowait, irq, softirq, steal;
};

struct net_prev {
    char name[64];
    unsigned long long rx_bytes;
    unsigned long long tx_bytes;
    double ts;
    int valid;
};

struct disk_prev {
    char name[64];
    unsigned long long read_sectors;
    unsigned long long write_sectors;
    unsigned long long io_ms;
    double ts;
    int valid;
};

struct guest_cpu_prev {
    char type[16];
    int vmid;
    double cpu_seconds_total;
    double ts;
    int valid;
};

struct cache_block {
    char *data;
    size_t len;
    double last_update;
    double last_duration;
    int success;
    pthread_mutex_t lock;
};

struct guest_item {
    int vmid;
    char type[16];
    char name[128];
    char status_hint[32];
};

struct guest_worker_arg {
    struct guest_item *items;
    int start_idx;
    int end_idx;
    char host[256];
    char **results;
    size_t *result_lens;
};

static struct net_prev g_net_prev[MAX_IFACE];
static size_t g_net_prev_count = 0;
static pthread_mutex_t g_net_prev_lock = PTHREAD_MUTEX_INITIALIZER;

static struct disk_prev g_disk_prev[MAX_DISK];
static size_t g_disk_prev_count = 0;
static pthread_mutex_t g_disk_prev_lock = PTHREAD_MUTEX_INITIALIZER;

static struct guest_cpu_prev g_guest_cpu_prev[MAX_GUEST];
static size_t g_guest_cpu_prev_count = 0;
static pthread_mutex_t g_guest_cpu_prev_lock = PTHREAD_MUTEX_INITIALIZER;

static struct cache_block g_cache_fast;
static struct cache_block g_cache_guest;
static struct cache_block g_cache_smart;
static struct cache_block g_cache_zfs;
static struct cache_block g_cache_ceph;

static volatile sig_atomic_t g_running = 1;

static void handle_signal(int sig) {
    (void)sig;
    g_running = 0;
}

static double now_seconds(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec + ((double)ts.tv_nsec / 1e9);
}

static void sleep_interruptible(double sec) {
    double end = now_seconds() + sec;
    while (g_running) {
        double rem = end - now_seconds();
        if (rem <= 0.0) break;
        if (rem > 0.25) rem = 0.25;
        usleep((useconds_t)(rem * 1000000.0));
    }
}

static void buf_init(struct buffer *b, size_t initial) {
    if (initial < 1024) initial = 1024;
    b->data = (char *)malloc(initial);
    if (!b->data) {
        perror("malloc");
        exit(1);
    }
    b->len = 0;
    b->cap = initial;
    b->data[0] = '\0';
}

static void buf_ensure(struct buffer *b, size_t need) {
    if (b->len + need + 1 <= b->cap) return;
    size_t newcap = b->cap;
    while (b->len + need + 1 > newcap) newcap *= 2;
    char *p = (char *)realloc(b->data, newcap);
    if (!p) {
        perror("realloc");
        exit(1);
    }
    b->data = p;
    b->cap = newcap;
}

static void buf_appendf(struct buffer *b, const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    va_list ap2;
    va_copy(ap2, ap);
    int n = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (n < 0) {
        va_end(ap2);
        return;
    }
    buf_ensure(b, (size_t)n);
    vsnprintf(b->data + b->len, b->cap - b->len, fmt, ap2);
    va_end(ap2);
    b->len += (size_t)n;
}

static void trim(char *s) {
    if (!s) return;
    size_t n = strlen(s);
    while (n > 0 && isspace((unsigned char)s[n - 1])) s[--n] = '\0';
    char *p = s;
    while (*p && isspace((unsigned char)*p)) p++;
    if (p != s) memmove(s, p, strlen(p) + 1);
}

static int starts_with(const char *s, const char *prefix) {
    return strncmp(s, prefix, strlen(prefix)) == 0;
}

static void sanitize_label_value(const char *src, char *dst, size_t dstsz) {
    size_t j = 0;
    for (size_t i = 0; src && src[i] && j + 2 < dstsz; i++) {
        unsigned char c = (unsigned char)src[i];
        if (c == '\\' || c == '"') {
            dst[j++] = '\\';
            dst[j++] = (char)c;
        } else if (c == '\n' || c == '\r') {
            dst[j++] = ' ';
        } else if (isprint(c)) {
            dst[j++] = (char)c;
        } else {
            dst[j++] = '_';
        }
    }
    dst[j] = '\0';
}

static char *read_file_trimmed(const char *path, char *buf, size_t bufsz) {
    FILE *f = fopen(path, "r");
    if (!f) return NULL;
    if (!fgets(buf, (int)bufsz, f)) {
        fclose(f);
        return NULL;
    }
    fclose(f);
    trim(buf);
    return buf;
}

static long long read_ll_file(const char *path, int *ok) {
    char buf[128];
    if (!read_file_trimmed(path, buf, sizeof(buf))) {
        if (ok) *ok = 0;
        return 0;
    }
    errno = 0;
    char *end = NULL;
    long long v = strtoll(buf, &end, 10);
    if (errno != 0 || end == buf) {
        if (ok) *ok = 0;
        return 0;
    }
    if (ok) *ok = 1;
    return v;
}

static FILE *popen_cmd(const char *cmd) {
    return popen(cmd, "r");
}

static int pclose_status(FILE *fp) {
    if (!fp) return -1;
    int st = pclose(fp);
    if (st == -1) return -1;
    if (WIFEXITED(st)) return WEXITSTATUS(st);
    return -1;
}

static char *read_cmd_output(const char *cmd, size_t *out_len) {
    FILE *fp = popen_cmd(cmd);
    if (!fp) return NULL;
    struct buffer b;
    buf_init(&b, 8192);
    char line[MAX_LINE];
    while (fgets(line, sizeof(line), fp)) {
        buf_appendf(&b, "%s", line);
    }
    pclose_status(fp);
    if (out_len) *out_len = b.len;
    return b.data;
}

static void metric_help_type(struct buffer *b, const char *name, const char *type, const char *help) {
    buf_appendf(b, "# HELP %s %s\n", name, help);
    buf_appendf(b, "# TYPE %s %s\n", name, type);
}

static void metric_value1(struct buffer *b, const char *name,
                          const char *k1, const char *v1,
                          double val) {
    char s1[MAX_LABEL];
    sanitize_label_value(v1, s1, sizeof(s1));
    buf_appendf(b, "%s{%s=\"%s\"} %.6f\n", name, k1, s1, val);
}

static void metric_value2(struct buffer *b, const char *name,
                          const char *k1, const char *v1,
                          const char *k2, const char *v2,
                          double val) {
    char s1[MAX_LABEL], s2[MAX_LABEL];
    sanitize_label_value(v1, s1, sizeof(s1));
    sanitize_label_value(v2, s2, sizeof(s2));
    buf_appendf(b, "%s{%s=\"%s\",%s=\"%s\"} %.6f\n", name, k1, s1, k2, s2, val);
}

static void metric_value3(struct buffer *b, const char *name,
                          const char *k1, const char *v1,
                          const char *k2, const char *v2,
                          const char *k3, const char *v3,
                          double val) {
    char s1[MAX_LABEL], s2[MAX_LABEL], s3[MAX_LABEL];
    sanitize_label_value(v1, s1, sizeof(s1));
    sanitize_label_value(v2, s2, sizeof(s2));
    sanitize_label_value(v3, s3, sizeof(s3));
    buf_appendf(b, "%s{%s=\"%s\",%s=\"%s\",%s=\"%s\"} %.6f\n",
                name, k1, s1, k2, s2, k3, s3, val);
}

static void metric_value4(struct buffer *b, const char *name,
                          const char *k1, const char *v1,
                          const char *k2, const char *v2,
                          const char *k3, const char *v3,
                          const char *k4, const char *v4,
                          double val) {
    char s1[MAX_LABEL], s2[MAX_LABEL], s3[MAX_LABEL], s4[MAX_LABEL];
    sanitize_label_value(v1, s1, sizeof(s1));
    sanitize_label_value(v2, s2, sizeof(s2));
    sanitize_label_value(v3, s3, sizeof(s3));
    sanitize_label_value(v4, s4, sizeof(s4));
    buf_appendf(b, "%s{%s=\"%s\",%s=\"%s\",%s=\"%s\",%s=\"%s\"} %.6f\n",
                name, k1, s1, k2, s2, k3, s3, k4, s4, val);
}

static void metric_value5(struct buffer *b, const char *name,
                          const char *k1, const char *v1,
                          const char *k2, const char *v2,
                          const char *k3, const char *v3,
                          const char *k4, const char *v4,
                          const char *k5, const char *v5,
                          double val) {
    char s1[MAX_LABEL], s2[MAX_LABEL], s3[MAX_LABEL], s4[MAX_LABEL], s5[MAX_LABEL];
    sanitize_label_value(v1, s1, sizeof(s1));
    sanitize_label_value(v2, s2, sizeof(s2));
    sanitize_label_value(v3, s3, sizeof(s3));
    sanitize_label_value(v4, s4, sizeof(s4));
    sanitize_label_value(v5, s5, sizeof(s5));
    buf_appendf(b, "%s{%s=\"%s\",%s=\"%s\",%s=\"%s\",%s=\"%s\",%s=\"%s\"} %.6f\n",
                name, k1, s1, k2, s2, k3, s3, k4, s4, k5, s5, val);
}

static void cache_init(struct cache_block *c) {
    c->data = strdup("");
    c->len = 0;
    c->last_update = 0.0;
    c->last_duration = 0.0;
    c->success = 0;
    pthread_mutex_init(&c->lock, NULL);
}

static void cache_store(struct cache_block *c, char *data, size_t len, double duration, int success) {
    pthread_mutex_lock(&c->lock);
    free(c->data);
    c->data = data;
    c->len = len;
    c->last_update = now_seconds();
    c->last_duration = duration;
    c->success = success;
    pthread_mutex_unlock(&c->lock);
}

static char *cache_snapshot(struct cache_block *c, size_t *len, double *age, double *duration, int *success) {
    pthread_mutex_lock(&c->lock);
    char *copy = NULL;
    if (c->data) {
        copy = (char *)malloc(c->len + 1);
        if (copy) {
            memcpy(copy, c->data, c->len);
            copy[c->len] = '\0';
        }
    } else {
        copy = strdup("");
    }
    *len = c->len;
    *age = (c->last_update > 0.0) ? (now_seconds() - c->last_update) : -1.0;
    *duration = c->last_duration;
    *success = c->success;
    pthread_mutex_unlock(&c->lock);
    return copy;
}

static size_t read_cpu_snapshot(struct cpu_times *arr, size_t max_items) {
    FILE *f = fopen("/proc/stat", "r");
    if (!f) return 0;
    char line[MAX_LINE];
    size_t n = 0;
    while (fgets(line, sizeof(line), f)) {
        if (!starts_with(line, "cpu")) break;
        if (n >= max_items) break;
        struct cpu_times *c = &arr[n];
        memset(c, 0, sizeof(*c));
        int rc = sscanf(line, "%31s %llu %llu %llu %llu %llu %llu %llu %llu",
                        c->name, &c->user, &c->nice, &c->system, &c->idle,
                        &c->iowait, &c->irq, &c->softirq, &c->steal);
        if (rc >= 5) n++;
    }
    fclose(f);
    return n;
}

static void emit_cpu_metrics(struct buffer *b, const char *host) {
    struct cpu_times a[MAX_CPU], c[MAX_CPU];
    size_t na = read_cpu_snapshot(a, MAX_CPU);
    usleep(SAMPLE_USEC);
    size_t nb = read_cpu_snapshot(c, MAX_CPU);
    size_t n = na < nb ? na : nb;

    metric_help_type(b, "proxmox_host_cpu_usage_percent", "gauge", "CPU usage percentage over the sample window.");
    metric_help_type(b, "proxmox_host_cpu_iowait_percent", "gauge", "CPU iowait percentage over the sample window.");

    for (size_t i = 0; i < n; i++) {
        unsigned long long idle1 = a[i].idle + a[i].iowait;
        unsigned long long idle2 = c[i].idle + c[i].iowait;
        unsigned long long non1 = a[i].user + a[i].nice + a[i].system + a[i].irq + a[i].softirq + a[i].steal;
        unsigned long long non2 = c[i].user + c[i].nice + c[i].system + c[i].irq + c[i].softirq + c[i].steal;
        unsigned long long total1 = idle1 + non1;
        unsigned long long total2 = idle2 + non2;
        unsigned long long dt = (total2 > total1) ? (total2 - total1) : 0;
        unsigned long long didle = (idle2 > idle1) ? (idle2 - idle1) : 0;
        unsigned long long diow = (c[i].iowait > a[i].iowait) ? (c[i].iowait - a[i].iowait) : 0;
        double usage = 0.0;
        double iowp = 0.0;
        if (dt > 0) {
            usage = 100.0 * ((double)(dt - didle) / (double)dt);
            iowp = 100.0 * ((double)diow / (double)dt);
        }
        const char *cpu_label = strcmp(c[i].name, "cpu") == 0 ? "total" : c[i].name + 3;
        metric_value2(b, "proxmox_host_cpu_usage_percent", "hostname", host, "cpu", cpu_label, usage);
        metric_value2(b, "proxmox_host_cpu_iowait_percent", "hostname", host, "cpu", cpu_label, iowp);
    }
}

static void emit_mem_metrics(struct buffer *b, const char *host) {
    FILE *f = fopen("/proc/meminfo", "r");
    if (!f) return;
    unsigned long long mem_total=0, mem_free=0, mem_avail=0, buffers=0, cached=0, swap_total=0, swap_free=0, dirty=0, writeback=0, sreclaimable=0;
    char key[128], unit[32], line[MAX_LINE];
    unsigned long long value;
    while (fgets(line, sizeof(line), f)) {
        key[0]=unit[0]='\0'; value=0;
        if (sscanf(line, "%127[^:]: %llu %31s", key, &value, unit) >= 2) {
            if      (!strcmp(key, "MemTotal")) mem_total = value * 1024ULL;
            else if (!strcmp(key, "MemFree")) mem_free = value * 1024ULL;
            else if (!strcmp(key, "MemAvailable")) mem_avail = value * 1024ULL;
            else if (!strcmp(key, "Buffers")) buffers = value * 1024ULL;
            else if (!strcmp(key, "Cached")) cached = value * 1024ULL;
            else if (!strcmp(key, "SwapTotal")) swap_total = value * 1024ULL;
            else if (!strcmp(key, "SwapFree")) swap_free = value * 1024ULL;
            else if (!strcmp(key, "Dirty")) dirty = value * 1024ULL;
            else if (!strcmp(key, "Writeback")) writeback = value * 1024ULL;
            else if (!strcmp(key, "SReclaimable")) sreclaimable = value * 1024ULL;
        }
    }
    fclose(f);

    metric_help_type(b, "proxmox_host_memory_bytes", "gauge", "Host memory values in bytes.");
    metric_help_type(b, "proxmox_host_memory_percent", "gauge", "Host memory usage percentages.");
    metric_value2(b, "proxmox_host_memory_bytes", "hostname", host, "type", "total", (double)mem_total);
    metric_value2(b, "proxmox_host_memory_bytes", "hostname", host, "type", "free", (double)mem_free);
    metric_value2(b, "proxmox_host_memory_bytes", "hostname", host, "type", "available", (double)mem_avail);
    metric_value2(b, "proxmox_host_memory_bytes", "hostname", host, "type", "buffers", (double)buffers);
    metric_value2(b, "proxmox_host_memory_bytes", "hostname", host, "type", "cached", (double)cached);
    metric_value2(b, "proxmox_host_memory_bytes", "hostname", host, "type", "dirty", (double)dirty);
    metric_value2(b, "proxmox_host_memory_bytes", "hostname", host, "type", "writeback", (double)writeback);
    metric_value2(b, "proxmox_host_memory_bytes", "hostname", host, "type", "sreclaimable", (double)sreclaimable);
    metric_value2(b, "proxmox_host_memory_bytes", "hostname", host, "type", "swap_total", (double)swap_total);
    metric_value2(b, "proxmox_host_memory_bytes", "hostname", host, "type", "swap_free", (double)swap_free);

    if (mem_total > 0) {
        metric_value2(b, "proxmox_host_memory_percent", "hostname", host, "type", "used", 100.0 * (double)(mem_total - mem_avail) / (double)mem_total);
        metric_value2(b, "proxmox_host_memory_percent", "hostname", host, "type", "available", 100.0 * (double)mem_avail / (double)mem_total);
    }
    if (swap_total > 0) {
        metric_value2(b, "proxmox_host_memory_percent", "hostname", host, "type", "swap_used", 100.0 * (double)(swap_total - swap_free) / (double)swap_total);
    }
}

static void emit_load_metrics(struct buffer *b, const char *host) {
    FILE *f = fopen("/proc/loadavg", "r");
    if (f) {
        double l1=0, l5=0, l15=0;
        if (fscanf(f, "%lf %lf %lf", &l1, &l5, &l15) == 3) {
            metric_help_type(b, "proxmox_host_load_average", "gauge", "Host load averages.");
            metric_value2(b, "proxmox_host_load_average", "hostname", host, "window", "1m", l1);
            metric_value2(b, "proxmox_host_load_average", "hostname", host, "window", "5m", l5);
            metric_value2(b, "proxmox_host_load_average", "hostname", host, "window", "15m", l15);
        }
        fclose(f);
    }

    f = fopen("/proc/uptime", "r");
    if (f) {
        double up=0, idle=0;
        if (fscanf(f, "%lf %lf", &up, &idle) == 2) {
            metric_help_type(b, "proxmox_host_uptime_seconds", "gauge", "Host uptime in seconds.");
            metric_value1(b, "proxmox_host_uptime_seconds", "hostname", host, up);
        }
        fclose(f);
    }
}

static struct net_prev *get_net_prev(const char *name) {
    for (size_t i = 0; i < g_net_prev_count; i++) {
        if (strcmp(g_net_prev[i].name, name) == 0) return &g_net_prev[i];
    }
    if (g_net_prev_count >= MAX_IFACE) return NULL;
    struct net_prev *p = &g_net_prev[g_net_prev_count++];
    memset(p, 0, sizeof(*p));
    snprintf(p->name, sizeof(p->name), "%s", name);
    return p;
}

static void emit_net_metrics(struct buffer *b, const char *host) {
    FILE *f = fopen("/proc/net/dev", "r");
    if (!f) return;

    metric_help_type(b, "proxmox_host_network_bytes_total", "counter", "Network bytes counters per interface.");
    metric_help_type(b, "proxmox_host_network_packets_total", "counter", "Network packet counters per interface.");
    metric_help_type(b, "proxmox_host_network_errors_total", "counter", "Network error counters per interface.");
    metric_help_type(b, "proxmox_host_network_drop_total", "counter", "Network drop counters per interface.");
    metric_help_type(b, "proxmox_host_network_bytes_per_second", "gauge", "Derived network throughput per interface.");

    char line[MAX_LINE];
    int line_no = 0;
    while (fgets(line, sizeof(line), f)) {
        line_no++;
        if (line_no <= 2) continue;
        char *colon = strchr(line, ':');
        if (!colon) continue;
        *colon = '\0';

        char iface[64];
        snprintf(iface, sizeof(iface), "%s", line);
        trim(iface);

        unsigned long long rx_bytes=0, rx_packets=0, rx_errs=0, rx_drop=0;
        unsigned long long tx_bytes=0, tx_packets=0, tx_errs=0, tx_drop=0;
        if (sscanf(colon + 1,
                   " %llu %llu %llu %llu %*llu %*llu %*llu %*llu %llu %llu %llu %llu",
                   &rx_bytes, &rx_packets, &rx_errs, &rx_drop,
                   &tx_bytes, &tx_packets, &tx_errs, &tx_drop) < 8) {
            continue;
        }

        metric_value3(b, "proxmox_host_network_bytes_total", "hostname", host, "interface", iface, "direction", "rx", (double)rx_bytes);
        metric_value3(b, "proxmox_host_network_bytes_total", "hostname", host, "interface", iface, "direction", "tx", (double)tx_bytes);
        metric_value3(b, "proxmox_host_network_packets_total", "hostname", host, "interface", iface, "direction", "rx", (double)rx_packets);
        metric_value3(b, "proxmox_host_network_packets_total", "hostname", host, "interface", iface, "direction", "tx", (double)tx_packets);
        metric_value3(b, "proxmox_host_network_errors_total", "hostname", host, "interface", iface, "direction", "rx", (double)rx_errs);
        metric_value3(b, "proxmox_host_network_errors_total", "hostname", host, "interface", iface, "direction", "tx", (double)tx_errs);
        metric_value3(b, "proxmox_host_network_drop_total", "hostname", host, "interface", iface, "direction", "rx", (double)rx_drop);
        metric_value3(b, "proxmox_host_network_drop_total", "hostname", host, "interface", iface, "direction", "tx", (double)tx_drop);

        pthread_mutex_lock(&g_net_prev_lock);
        struct net_prev *p = get_net_prev(iface);
        double t = now_seconds();
        if (p && p->valid) {
            double dt = t - p->ts;
            if (dt > 0.0) {
                double rxps = (rx_bytes >= p->rx_bytes) ? (double)(rx_bytes - p->rx_bytes) / dt : 0.0;
                double txps = (tx_bytes >= p->tx_bytes) ? (double)(tx_bytes - p->tx_bytes) / dt : 0.0;
                metric_value3(b, "proxmox_host_network_bytes_per_second", "hostname", host, "interface", iface, "direction", "rx", rxps);
                metric_value3(b, "proxmox_host_network_bytes_per_second", "hostname", host, "interface", iface, "direction", "tx", txps);
            }
        }
        if (p) {
            p->rx_bytes = rx_bytes;
            p->tx_bytes = tx_bytes;
            p->ts = t;
            p->valid = 1;
        }
        pthread_mutex_unlock(&g_net_prev_lock);
    }
    fclose(f);
}

static int is_real_block_device(const char *name) {
    if (starts_with(name, "loop") || starts_with(name, "ram") || starts_with(name, "zram")) return 0;
    return 1;
}

static struct disk_prev *get_disk_prev(const char *name) {
    for (size_t i = 0; i < g_disk_prev_count; i++) {
        if (strcmp(g_disk_prev[i].name, name) == 0) return &g_disk_prev[i];
    }
    if (g_disk_prev_count >= MAX_DISK) return NULL;
    struct disk_prev *p = &g_disk_prev[g_disk_prev_count++];
    memset(p, 0, sizeof(*p));
    snprintf(p->name, sizeof(p->name), "%s", name);
    return p;
}

static void emit_diskstats_metrics(struct buffer *b, const char *host) {
    FILE *f = fopen("/proc/diskstats", "r");
    if (!f) return;

    metric_help_type(b, "proxmox_host_disk_reads_completed_total", "counter", "Disk reads completed.");
    metric_help_type(b, "proxmox_host_disk_writes_completed_total", "counter", "Disk writes completed.");
    metric_help_type(b, "proxmox_host_disk_bytes_total", "counter", "Disk bytes read/written.");
    metric_help_type(b, "proxmox_host_disk_busy_percent", "gauge", "Approximate disk busy percentage over the sample interval.");
    metric_help_type(b, "proxmox_host_disk_bytes_per_second", "gauge", "Derived disk throughput over the sample interval.");

    char line[MAX_LINE];
    while (fgets(line, sizeof(line), f)) {
        unsigned int major=0, minor=0;
        char name[64];
        unsigned long long reads_completed=0, reads_merged=0, sectors_read=0, ms_reading=0;
        unsigned long long writes_completed=0, writes_merged=0, sectors_written=0, ms_writing=0;
        unsigned long long in_progress=0, io_ms=0, weighted_io_ms=0;
        int rc = sscanf(line,
                        "%u %u %63s %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu %llu",
                        &major, &minor, name,
                        &reads_completed, &reads_merged, &sectors_read, &ms_reading,
                        &writes_completed, &writes_merged, &sectors_written, &ms_writing,
                        &in_progress, &io_ms, &weighted_io_ms);
        if (rc < 14) continue;
        if (!is_real_block_device(name)) continue;

        double bytes_read = (double)sectors_read * 512.0;
        double bytes_written = (double)sectors_written * 512.0;

        metric_value2(b, "proxmox_host_disk_reads_completed_total", "hostname", host, "device", name, (double)reads_completed);
        metric_value2(b, "proxmox_host_disk_writes_completed_total", "hostname", host, "device", name, (double)writes_completed);
        metric_value3(b, "proxmox_host_disk_bytes_total", "hostname", host, "device", name, "direction", "read", bytes_read);
        metric_value3(b, "proxmox_host_disk_bytes_total", "hostname", host, "device", name, "direction", "write", bytes_written);

        pthread_mutex_lock(&g_disk_prev_lock);
        struct disk_prev *p = get_disk_prev(name);
        double t = now_seconds();
        if (p && p->valid) {
            double dt = t - p->ts;
            if (dt > 0.0) {
                double rbps = (sectors_read >= p->read_sectors) ? ((double)(sectors_read - p->read_sectors) * 512.0) / dt : 0.0;
                double wbps = (sectors_written >= p->write_sectors) ? ((double)(sectors_written - p->write_sectors) * 512.0) / dt : 0.0;
                double busy = (io_ms >= p->io_ms) ? (((double)(io_ms - p->io_ms) / 1000.0) / dt) * 100.0 : 0.0;
                if (busy < 0.0) busy = 0.0;
                if (busy > 100.0) busy = 100.0;
                metric_value3(b, "proxmox_host_disk_bytes_per_second", "hostname", host, "device", name, "direction", "read", rbps);
                metric_value3(b, "proxmox_host_disk_bytes_per_second", "hostname", host, "device", name, "direction", "write", wbps);
                metric_value2(b, "proxmox_host_disk_busy_percent", "hostname", host, "device", name, busy);
            }
        }
        if (p) {
            p->read_sectors = sectors_read;
            p->write_sectors = sectors_written;
            p->io_ms = io_ms;
            p->ts = t;
            p->valid = 1;
        }
        pthread_mutex_unlock(&g_disk_prev_lock);
    }
    fclose(f);
}

static int ignored_mount_fs(const char *fstype) {
    const char *ignore[] = {
        "proc","sysfs","devtmpfs","devpts","tmpfs","cgroup","cgroup2","overlay","squashfs",
        "rpc_pipefs","debugfs","tracefs","securityfs","pstore","configfs","hugetlbfs","mqueue",
        "fusectl","binfmt_misc","autofs","lxcfs","bpf"
    };
    for (size_t i = 0; i < sizeof(ignore)/sizeof(ignore[0]); i++) {
        if (strcmp(fstype, ignore[i]) == 0) return 1;
    }
    return 0;
}

static int ignored_mountpoint(const char *mnt) {
    if (!mnt || !*mnt) return 1;
    if (starts_with(mnt, "/run/credentials/")) return 1;
    if (starts_with(mnt, "/var/lib/lxcfs")) return 1;
    return 0;
}

static void emit_filesystem_metrics(struct buffer *b, const char *host) {
    FILE *f = fopen("/proc/mounts", "r");
    if (!f) return;
    metric_help_type(b, "proxmox_host_filesystem_bytes", "gauge", "Filesystem space metrics in bytes.");

    char dev[256], mnt[256], fstype[64], opts[256];
    int dump=0, pass=0;
    while (fscanf(f, "%255s %255s %63s %255s %d %d\n", dev, mnt, fstype, opts, &dump, &pass) == 6) {
        if (ignored_mount_fs(fstype)) continue;
        if (ignored_mountpoint(mnt)) continue;
        struct statvfs sv;
        if (statvfs(mnt, &sv) != 0) continue;
        double total = (double)sv.f_blocks * (double)sv.f_frsize;
        double avail = (double)sv.f_bavail * (double)sv.f_frsize;
        double freeb = (double)sv.f_bfree * (double)sv.f_frsize;
        double used = total - freeb;
        if (total <= 0.0) continue;
        metric_value3(b, "proxmox_host_filesystem_bytes", "hostname", host, "mountpoint", mnt, "type", "total", total);
        metric_value3(b, "proxmox_host_filesystem_bytes", "hostname", host, "mountpoint", mnt, "type", "used", used);
        metric_value3(b, "proxmox_host_filesystem_bytes", "hostname", host, "mountpoint", mnt, "type", "available", avail);
        metric_value3(b, "proxmox_host_filesystem_bytes", "hostname", host, "mountpoint", mnt, "type", "free", freeb);
    }
    fclose(f);
}

static void emit_hwmon_metrics(struct buffer *b, const char *host) {
    metric_help_type(b, "proxmox_host_sensor_temperature_celsius", "gauge", "Temperature sensors from hwmon and thermal zones.");
    metric_help_type(b, "proxmox_host_sensor_fan_rpm", "gauge", "Fan speeds from hwmon.");

    DIR *d = opendir("/sys/class/hwmon");
    if (d) {
        struct dirent *de;
        while ((de = readdir(d))) {
            if (de->d_name[0] == '.') continue;
            char base[MAX_PATH];
            snprintf(base, sizeof(base), "/sys/class/hwmon/%s", de->d_name);
            char chip[128] = "unknown";
            char namepath[MAX_PATH];
            snprintf(namepath, sizeof(namepath), "%s/name", base);
            read_file_trimmed(namepath, chip, sizeof(chip));

            DIR *dd = opendir(base);
            if (!dd) continue;
            struct dirent *e2;
            while ((e2 = readdir(dd))) {
                if (starts_with(e2->d_name, "temp") && strstr(e2->d_name, "_input")) {
                    char p[MAX_PATH], labelp[MAX_PATH], label[128] = "";
                    snprintf(p, sizeof(p), "%s/%s", base, e2->d_name);
                    int ok = 0;
                    long long mv = read_ll_file(p, &ok);
                    if (!ok) continue;
                    char inputcopy[128];
                    snprintf(inputcopy, sizeof(inputcopy), "%s", e2->d_name);
                    char *us = strstr(inputcopy, "_input");
                    if (us) *us = '\0';
                    snprintf(labelp, sizeof(labelp), "%s/%s_label", base, inputcopy);
                    if (!read_file_trimmed(labelp, label, sizeof(label))) snprintf(label, sizeof(label), "%s", inputcopy);
                    metric_value3(b, "proxmox_host_sensor_temperature_celsius", "hostname", host, "chip", chip, "sensor", label, (double)mv / 1000.0);
                } else if (starts_with(e2->d_name, "fan") && strstr(e2->d_name, "_input")) {
                    char p[MAX_PATH], labelp[MAX_PATH], label[128] = "";
                    snprintf(p, sizeof(p), "%s/%s", base, e2->d_name);
                    int ok = 0;
                    long long rpm = read_ll_file(p, &ok);
                    if (!ok) continue;
                    char inputcopy[128];
                    snprintf(inputcopy, sizeof(inputcopy), "%s", e2->d_name);
                    char *us = strstr(inputcopy, "_input");
                    if (us) *us = '\0';
                    snprintf(labelp, sizeof(labelp), "%s/%s_label", base, inputcopy);
                    if (!read_file_trimmed(labelp, label, sizeof(label))) snprintf(label, sizeof(label), "%s", inputcopy);
                    metric_value3(b, "proxmox_host_sensor_fan_rpm", "hostname", host, "chip", chip, "sensor", label, (double)rpm);
                }
            }
            closedir(dd);
        }
        closedir(d);
    }
}

static void emit_mdraid_metrics(struct buffer *b, const char *host) {
    FILE *f = fopen("/proc/mdstat", "r");
    if (!f) return;

    metric_help_type(b, "proxmox_host_mdraid_state", "gauge", "MD RAID state flags: active, degraded, resyncing.");

    char line[MAX_LINE];
    char current_md[64] = "";
    int active = 0, degraded = 0, resync = 0;

    while (fgets(line, sizeof(line), f)) {
        if (starts_with(line, "md")) {
            if (current_md[0]) {
                metric_value3(b, "proxmox_host_mdraid_state", "hostname", host, "array", current_md, "state", "active", active);
                metric_value3(b, "proxmox_host_mdraid_state", "hostname", host, "array", current_md, "state", "degraded", degraded);
                metric_value3(b, "proxmox_host_mdraid_state", "hostname", host, "array", current_md, "state", "resyncing", resync);
            }
            current_md[0] = '\0';
            active = degraded = resync = 0;
            sscanf(line, "%63s", current_md);
            if (strstr(line, " active ")) active = 1;
        } else if (current_md[0]) {
            if (strstr(line, "recovery") || strstr(line, "resync") || strstr(line, "reshape")) resync = 1;
            char *lb = strchr(line, '[');
            while (lb) {
                char *rb = strchr(lb, ']');
                if (!rb) break;
                if (memchr(lb, '_', (size_t)(rb - lb))) degraded = 1;
                lb = strchr(rb + 1, '[');
            }
        }
    }

    if (current_md[0]) {
        metric_value3(b, "proxmox_host_mdraid_state", "hostname", host, "array", current_md, "state", "active", active);
        metric_value3(b, "proxmox_host_mdraid_state", "hostname", host, "array", current_md, "state", "degraded", degraded);
        metric_value3(b, "proxmox_host_mdraid_state", "hostname", host, "array", current_md, "state", "resyncing", resync);
    }

    fclose(f);
}

static int list_block_devices(char devs[][64], size_t maxdev) {
    DIR *d = opendir("/sys/block");
    if (!d) return 0;
    struct dirent *de;
    int n = 0;
    while ((de = readdir(d))) {
        if (de->d_name[0] == '.') continue;
        if (!is_real_block_device(de->d_name)) continue;
        if (n < (int)maxdev) snprintf(devs[n++], 64, "%s", de->d_name);
    }
    closedir(d);
    return n;
}

static void normalize_numeric_string(char *s) {
    char *r = s, *w = s;
    while (*r) {
        if (*r != ',') *w++ = *r;
        r++;
    }
    *w = '\0';
}

static int parse_smart_text_line_value(const char *line, const char *prefix, double *out) {
    const char *p = strstr(line, prefix);
    if (!p) return 0;
    p += strlen(prefix);
    while (*p && !(isdigit((unsigned char)*p) || *p == '-' || *p == '.')) p++;
    if (!*p) return 0;

    char buf[128];
    size_t j = 0;
    while (*p && j + 1 < sizeof(buf)) {
        unsigned char c = (unsigned char)*p;
        if (isdigit(c) || c == '-' || c == '.' || c == ',') {
            buf[j++] = (char)c;
            p++;
        } else {
            break;
        }
    }
    buf[j] = '\0';
    normalize_numeric_string(buf);
    char *end = NULL;
    double v = strtod(buf, &end);
    if (end == buf) return 0;
    *out = v;
    return 1;
}

static void emit_smart_for_device(struct buffer *b, const char *host, const char *dev) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "smartctl -a /dev/%s 2>/dev/null", dev);
    FILE *fp = popen_cmd(cmd);
    if (!fp) return;

    char line[MAX_LINE];
    int seen = 0;
    int health = -1;
    double temp = NAN, poh = NAN, pct_used = NAN, media_err = NAN;

    while (fgets(line, sizeof(line), fp)) {
        if (strstr(line, "SMART overall-health self-assessment test result:")) {
            seen = 1;
            health = strstr(line, "PASSED") ? 1 : 0;
        }
        if (strstr(line, "SMART Health Status:")) {
            seen = 1;
            health = strstr(line, "OK") ? 1 : 0;
        }
        if (strstr(line, "Temperature:") && isnan(temp)) {
            double v;
            if (parse_smart_text_line_value(line, "Temperature:", &v)) temp = v;
        }
        if (strstr(line, "Temperature_Celsius") || strstr(line, "Airflow_Temperature_Cel")) {
            char *last = strrchr(line, ' ');
            if (last) {
                double v = atof(last);
                if (v > 0) temp = v;
            }
        }
        if (strstr(line, "Power_On_Hours")) {
            char *last = strrchr(line, ' ');
            if (last) {
                double v = atof(last);
                if (v >= 0) poh = v;
            }
        }
        if (strstr(line, "Percentage Used:")) {
            double v;
            if (parse_smart_text_line_value(line, "Percentage Used:", &v)) pct_used = v;
        }
        if (strstr(line, "Media and Data Integrity Errors:")) {
            double v;
            if (parse_smart_text_line_value(line, "Media and Data Integrity Errors:", &v)) media_err = v;
        }
        if (strstr(line, "Power On Hours:")) {
            double v;
            if (parse_smart_text_line_value(line, "Power On Hours:", &v)) poh = v;
        }
    }
    int rc = pclose_status(fp);
    if (rc == 0 || seen || !isnan(temp) || !isnan(poh)) {
        metric_value2(b, "proxmox_host_smart_device_present", "hostname", host, "device", dev, 1.0);
        if (health >= 0) metric_value2(b, "proxmox_host_smart_health_passed", "hostname", host, "device", dev, (double)health);
        if (!isnan(temp)) metric_value2(b, "proxmox_host_smart_temperature_celsius", "hostname", host, "device", dev, temp);
        if (!isnan(poh)) metric_value2(b, "proxmox_host_smart_power_on_hours", "hostname", host, "device", dev, poh);
        if (!isnan(pct_used)) metric_value2(b, "proxmox_host_smart_percentage_used", "hostname", host, "device", dev, pct_used);
        if (!isnan(media_err)) metric_value2(b, "proxmox_host_smart_media_errors_total", "hostname", host, "device", dev, media_err);
    }
}

static void build_smart_metrics(struct buffer *b, const char *host) {
    metric_help_type(b, "proxmox_host_smart_device_present", "gauge", "Whether SMART data for a device was collected.");
    metric_help_type(b, "proxmox_host_smart_health_passed", "gauge", "SMART health self-assessment passed flag.");
    metric_help_type(b, "proxmox_host_smart_temperature_celsius", "gauge", "SMART-reported device temperature.");
    metric_help_type(b, "proxmox_host_smart_power_on_hours", "gauge", "SMART-reported power on hours.");
    metric_help_type(b, "proxmox_host_smart_percentage_used", "gauge", "NVMe percentage used.");
    metric_help_type(b, "proxmox_host_smart_media_errors_total", "counter", "NVMe media and data integrity errors.");

    char devs[MAX_SMART_DEV][64];
    int n = list_block_devices(devs, MAX_SMART_DEV);
    for (int i = 0; i < n; i++) emit_smart_for_device(b, host, devs[i]);
}

static void emit_process_metrics(struct buffer *b, const char *host) {
    DIR *d = opendir("/proc");
    if (!d) return;
    int count = 0;
    struct dirent *de;
    while ((de = readdir(d))) {
        if (isdigit((unsigned char)de->d_name[0])) count++;
    }
    closedir(d);
    metric_help_type(b, "proxmox_host_process_count", "gauge", "Number of processes visible in /proc.");
    metric_value1(b, "proxmox_host_process_count", "hostname", host, (double)count);
}

static void emit_os_metrics(struct buffer *b, const char *host) {
    FILE *f = fopen("/etc/os-release", "r");
    if (!f) return;
    char line[MAX_LINE], name[128] = "", ver[128] = "";
    while (fgets(line, sizeof(line), f)) {
        trim(line);
        if (starts_with(line, "PRETTY_NAME=")) snprintf(name, sizeof(name), "%s", line + 12);
        else if (starts_with(line, "VERSION_ID=")) snprintf(ver, sizeof(ver), "%s", line + 11);
    }
    fclose(f);

    trim(name);
    trim(ver);
    if (name[0] == '"') memmove(name, name + 1, strlen(name));
    if (ver[0] == '"') memmove(ver, ver + 1, strlen(ver));
    size_t ln = strlen(name), lv = strlen(ver);
    if (ln && name[ln - 1] == '"') name[ln - 1] = '\0';
    if (lv && ver[lv - 1] == '"') ver[lv - 1] = '\0';

    metric_help_type(b, "proxmox_host_info", "gauge", "Static host information.");
    metric_value3(b, "proxmox_host_info", "hostname", host, "os", name[0] ? name : "unknown", "version", ver[0] ? ver : "unknown", 1.0);
}

static void extract_kv(const char *line, char *k, size_t ksz, char *v, size_t vsz) {
    const char *p = strchr(line, ':');
    if (!p) { k[0]='\0'; v[0]='\0'; return; }
    snprintf(k, ksz, "%.*s", (int)(p - line), line);
    snprintf(v, vsz, "%s", p + 1);
    trim(k);
    trim(v);
}

static int parse_first_int(const char *line, int *out) {
    while (*line && isspace((unsigned char)*line)) line++;
    if (!isdigit((unsigned char)*line)) return 0;
    char *end = NULL;
    long v = strtol(line, &end, 10);
    if (end == line) return 0;
    *out = (int)v;
    return 1;
}

static int read_guest_name_from_conf(const char *type, int vmid, char *name, size_t namesz) {
    char path[MAX_PATH];
    if (strcmp(type, "qemu") == 0)
        snprintf(path, sizeof(path), "/etc/pve/qemu-server/%d.conf", vmid);
    else
        snprintf(path, sizeof(path), "/etc/pve/lxc/%d.conf", vmid);

    FILE *f = fopen(path, "r");
    if (!f) return 0;

    char line[MAX_LINE];
    while (fgets(line, sizeof(line), f)) {
        trim(line);
        if (strcmp(type, "qemu") == 0 && starts_with(line, "name:")) {
            snprintf(name, namesz, "%s", line + 5);
            trim(name);
            fclose(f);
            return 1;
        }
        if (strcmp(type, "lxc") == 0 && starts_with(line, "hostname:")) {
            snprintf(name, namesz, "%s", line + 9);
            trim(name);
            fclose(f);
            return 1;
        }
    }
    fclose(f);
    return 0;
}

static struct guest_cpu_prev *get_guest_cpu_prev(const char *type, int vmid) {
    for (size_t i = 0; i < g_guest_cpu_prev_count; i++) {
        if (g_guest_cpu_prev[i].vmid == vmid && strcmp(g_guest_cpu_prev[i].type, type) == 0)
            return &g_guest_cpu_prev[i];
    }
    if (g_guest_cpu_prev_count >= MAX_GUEST) return NULL;
    struct guest_cpu_prev *p = &g_guest_cpu_prev[g_guest_cpu_prev_count++];
    memset(p, 0, sizeof(*p));
    snprintf(p->type, sizeof(p->type), "%s", type);
    p->vmid = vmid;
    return p;
}

static int read_qemu_pid(int vmid, pid_t *pid_out) {
    char path[MAX_PATH], buf[64];
    snprintf(path, sizeof(path), "/run/qemu-server/%d.pid", vmid);
    if (!read_file_trimmed(path, buf, sizeof(buf))) return 0;
    long v = strtol(buf, NULL, 10);
    if (v <= 0) return 0;
    *pid_out = (pid_t)v;
    return 1;
}

static int read_proc_pid_cpu_seconds(pid_t pid, double *cpu_seconds_out) {
    char path[MAX_PATH];
    snprintf(path, sizeof(path), "/proc/%ld/stat", (long)pid);
    FILE *f = fopen(path, "r");
    if (!f) return 0;

    char buf[8192];
    if (!fgets(buf, sizeof(buf), f)) {
        fclose(f);
        return 0;
    }
    fclose(f);

    char *rp = strrchr(buf, ')');
    if (!rp) return 0;
    char *p = rp + 1;
    while (*p == ' ') p++;

    unsigned long long utime = 0, stime = 0;
    int field = 3;
    char *save = NULL;
    char *tok = strtok_r(p, " ", &save);
    while (tok) {
        if (field == 14) utime = strtoull(tok, NULL, 10);
        else if (field == 15) {
            stime = strtoull(tok, NULL, 10);
            break;
        }
        field++;
        tok = strtok_r(NULL, " ", &save);
    }

    long ticks = sysconf(_SC_CLK_TCK);
    if (ticks <= 0) return 0;
    *cpu_seconds_out = (double)(utime + stime) / (double)ticks;
    return 1;
}

static int parse_cpu_stat_usage_file(const char *path, double *cpu_seconds_out) {
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    char line[MAX_LINE];
    while (fgets(line, sizeof(line), f)) {
        unsigned long long v = 0ULL;
        if (sscanf(line, "usage_usec %llu", &v) == 1) {
            fclose(f);
            *cpu_seconds_out = (double)v / 1000000.0;
            return 1;
        }
        if (sscanf(line, "usage_nsec %llu", &v) == 1) {
            fclose(f);
            *cpu_seconds_out = (double)v / 1000000000.0;
            return 1;
        }
    }
    fclose(f);
    return 0;
}

static int read_lxc_cpu_seconds(int vmid, double *cpu_seconds_out) {
    char path[MAX_PATH];

    snprintf(path, sizeof(path), "/sys/fs/cgroup/system.slice/pve-container@%d.service/cpu.stat", vmid);
    if (parse_cpu_stat_usage_file(path, cpu_seconds_out)) return 1;

    snprintf(path, sizeof(path), "/sys/fs/cgroup/lxc.payload.%d/cpu.stat", vmid);
    if (parse_cpu_stat_usage_file(path, cpu_seconds_out)) return 1;

    snprintf(path, sizeof(path), "/sys/fs/cgroup/lxc/%d/cpu.stat", vmid);
    if (parse_cpu_stat_usage_file(path, cpu_seconds_out)) return 1;

    snprintf(path, sizeof(path), "/sys/fs/cgroup/machine.slice/machine-lxc\\x2d%d.scope/cpu.stat", vmid);
    if (parse_cpu_stat_usage_file(path, cpu_seconds_out)) return 1;

    return 0;
}

static int read_guest_cpu_time_seconds(const char *type, int vmid, double *cpu_seconds_out) {
    if (strcmp(type, "qemu") == 0) {
        pid_t pid = 0;
        if (!read_qemu_pid(vmid, &pid)) return 0;
        return read_proc_pid_cpu_seconds(pid, cpu_seconds_out);
    } else if (strcmp(type, "lxc") == 0) {
        return read_lxc_cpu_seconds(vmid, cpu_seconds_out);
    }
    return 0;
}

static double compute_guest_cpu_usage_fraction(const char *type, int vmid, double cpu_seconds_total, double cpus, int *have_rate) {
    *have_rate = 0;
    double usage = 0.0;
    pthread_mutex_lock(&g_guest_cpu_prev_lock);
    struct guest_cpu_prev *p = get_guest_cpu_prev(type, vmid);
    double t = now_seconds();
    if (p && p->valid) {
        double dt = t - p->ts;
        double dcpu = cpu_seconds_total - p->cpu_seconds_total;
        if (dt > 0.0 && dcpu >= 0.0) {
            usage = (cpus > 0.0) ? (dcpu / (dt * cpus)) : (dcpu / dt);
            if (usage < 0.0) usage = 0.0;
            *have_rate = 1;
        }
    }
    if (p) {
        p->cpu_seconds_total = cpu_seconds_total;
        p->ts = t;
        p->valid = 1;
    }
    pthread_mutex_unlock(&g_guest_cpu_prev_lock);
    return usage;
}

static void emit_guest_metric_set(struct buffer *b, const char *host, const char *type, int id,
                                  const char *name, const char *status,
                                  double cpu_usage_fraction, double cpu_time_seconds_total, int cpu_time_valid,
                                  double cpus, double mem, double maxmem, double disk, double maxdisk,
                                  double netin, double netout, double diskread, double diskwrite, double uptime) {
    char idbuf[32];
    snprintf(idbuf, sizeof(idbuf), "%d", id);
    const char *gname = (name && *name) ? name : "unknown";
    const char *gstatus = (status && *status) ? status : "unknown";

    metric_value5(b, "proxmox_guest_status",
                  "hostname", host, "guest_type", type, "vmid", idbuf, "guest_name", gname, "status", gstatus,
                  strcmp(gstatus, "running") == 0 ? 1.0 : 0.0);

    metric_value4(b, "proxmox_guest_cpu_usage_fraction",
                  "hostname", host, "guest_type", type, "vmid", idbuf, "guest_name", gname,
                  cpu_usage_fraction);

    if (cpu_time_valid) {
        metric_value4(b, "proxmox_guest_cpu_time_seconds_total",
                      "hostname", host, "guest_type", type, "vmid", idbuf, "guest_name", gname,
                      cpu_time_seconds_total);
    }

    metric_value4(b, "proxmox_guest_cpu_count",
                  "hostname", host, "guest_type", type, "vmid", idbuf, "guest_name", gname,
                  cpus);

    metric_value5(b, "proxmox_guest_memory_bytes",
                  "hostname", host, "guest_type", type, "vmid", idbuf, "guest_name", gname, "kind", "used", mem);
    metric_value5(b, "proxmox_guest_memory_bytes",
                  "hostname", host, "guest_type", type, "vmid", idbuf, "guest_name", gname, "kind", "max", maxmem);

    metric_value5(b, "proxmox_guest_disk_bytes",
                  "hostname", host, "guest_type", type, "vmid", idbuf, "guest_name", gname, "kind", "used", disk);
    metric_value5(b, "proxmox_guest_disk_bytes",
                  "hostname", host, "guest_type", type, "vmid", idbuf, "guest_name", gname, "kind", "max", maxdisk);

    metric_value5(b, "proxmox_guest_network_bytes_total",
                  "hostname", host, "guest_type", type, "vmid", idbuf, "guest_name", gname, "direction", "in", netin);
    metric_value5(b, "proxmox_guest_network_bytes_total",
                  "hostname", host, "guest_type", type, "vmid", idbuf, "guest_name", gname, "direction", "out", netout);

    metric_value5(b, "proxmox_guest_disk_io_bytes_total",
                  "hostname", host, "guest_type", type, "vmid", idbuf, "guest_name", gname, "direction", "read", diskread);
    metric_value5(b, "proxmox_guest_disk_io_bytes_total",
                  "hostname", host, "guest_type", type, "vmid", idbuf, "guest_name", gname, "direction", "write", diskwrite);

    metric_value4(b, "proxmox_guest_uptime_seconds",
                  "hostname", host, "guest_type", type, "vmid", idbuf, "guest_name", gname, uptime);

    metric_value5(b, "proxmox_guest_info",
                  "hostname", host, "guest_type", type, "vmid", idbuf, "guest_name", gname, "status", gstatus, 1.0);
}

static int parse_qm_list_line(const char *line, int *vmid, char *name, size_t namesz, char *status, size_t statsz) {
    int id = 0;
    char nm[128] = "";
    char st[64] = "";
    int mem = 0, bootdisk = 0, pid = 0;
    if (sscanf(line, "%d %127s %63s %d %d %d", &id, nm, st, &mem, &bootdisk, &pid) >= 3) {
        *vmid = id;
        snprintf(name, namesz, "%s", nm);
        snprintf(status, statsz, "%s", st);
        return 1;
    }
    return 0;
}

static int parse_pct_list_line(const char *line, int *vmid, char *name, size_t namesz, char *status, size_t statsz) {
    char copy[MAX_LINE];
    snprintf(copy, sizeof(copy), "%s", line);
    trim(copy);
    if (!copy[0]) return 0;

    char *tokens[16];
    int nt = 0;
    char *save = NULL;
    char *tok = strtok_r(copy, " \t", &save);
    while (tok && nt < 16) {
        tokens[nt++] = tok;
        tok = strtok_r(NULL, " \t", &save);
    }
    if (nt < 2) return 0;
    if (!isdigit((unsigned char)tokens[0][0])) return 0;

    *vmid = atoi(tokens[0]);
    snprintf(status, statsz, "%s", tokens[1]);

    if (nt >= 4) snprintf(name, namesz, "%s", tokens[3]);
    else if (nt >= 3) snprintf(name, namesz, "%s", tokens[2]);
    else snprintf(name, namesz, "lxc-%d", *vmid);
    return 1;
}

static int collect_guest_list(struct guest_item *items, int max_items, const char *type) {
    char cmd[128];
    snprintf(cmd, sizeof(cmd), "%s list 2>/dev/null", strcmp(type, "qemu") == 0 ? "qm" : "pct");
    FILE *fp = popen_cmd(cmd);
    if (!fp) return 0;

    char line[MAX_LINE];
    int first = 1, n = 0;
    while (fgets(line, sizeof(line), fp)) {
        trim(line);
        if (!line[0]) continue;
        if (first) { first = 0; continue; }
        if (n >= max_items) break;

        int id = 0;
        char name[128] = "";
        char status[32] = "";
        int ok = 0;

        if (strcmp(type, "qemu") == 0) ok = parse_qm_list_line(line, &id, name, sizeof(name), status, sizeof(status));
        else ok = parse_pct_list_line(line, &id, name, sizeof(name), status, sizeof(status));
        if (!ok) continue;

        items[n].vmid = id;
        snprintf(items[n].type, sizeof(items[n].type), "%s", type);
        snprintf(items[n].status_hint, sizeof(items[n].status_hint), "%s", status[0] ? status : "unknown");

        char better_name[128] = "";
        if (read_guest_name_from_conf(type, id, better_name, sizeof(better_name))) {
            snprintf(items[n].name, sizeof(items[n].name), "%s", better_name);
        } else {
            snprintf(items[n].name, sizeof(items[n].name), "%s", name[0] ? name : "unknown");
        }
        n++;
    }
    pclose_status(fp);
    return n;
}

static void emit_guest_stopped_fast(struct buffer *b, const char *host, const struct guest_item *item) {
    emit_guest_metric_set(b, host, item->type, item->vmid, item->name, "stopped",
                          0.0, 0.0, 0,
                          0.0, 0.0, 0.0, 0.0, 0.0,
                          0.0, 0.0, 0.0, 0.0, 0.0);
}

static void poll_one_guest(struct buffer *b, const char *host, const struct guest_item *item) {
    if (strcmp(item->status_hint, "stopped") == 0) {
        emit_guest_stopped_fast(b, host, item);
        return;
    }

    char cmd[256];
    snprintf(cmd, sizeof(cmd), "%s status %d --verbose 2>/dev/null",
             strcmp(item->type, "qemu") == 0 ? "qm" : "pct", item->vmid);

    FILE *fp = popen_cmd(cmd);
    if (!fp) {
        emit_guest_stopped_fast(b, host, item);
        return;
    }

    char line[MAX_LINE], k[128], v[256];
    char status[64] = "unknown";
    char name[128] = "";
    snprintf(name, sizeof(name), "%s", item->name);

    double cli_cpu = 0.0, cpus = 0.0, mem = 0.0, maxmem = 0.0, disk = 0.0, maxdisk = 0.0;
    double netin = 0.0, netout = 0.0, diskread = 0.0, diskwrite = 0.0, uptime = 0.0;

    while (fgets(line, sizeof(line), fp)) {
        extract_kv(line, k, sizeof(k), v, sizeof(v));
        if (!k[0]) continue;
        if (!strcmp(k, "status")) snprintf(status, sizeof(status), "%s", v);
        else if (!strcmp(k, "name") && strcmp(item->type, "qemu") == 0 && v[0]) snprintf(name, sizeof(name), "%s", v);
        else if (!strcmp(k, "hostname") && strcmp(item->type, "lxc") == 0 && v[0]) snprintf(name, sizeof(name), "%s", v);
        else if (!strcmp(k, "cpu")) cli_cpu = atof(v);
        else if (!strcmp(k, "cpus")) cpus = atof(v);
        else if (!strcmp(k, "mem")) mem = atof(v);
        else if (!strcmp(k, "maxmem")) maxmem = atof(v);
        else if (!strcmp(k, "disk")) disk = atof(v);
        else if (!strcmp(k, "maxdisk")) maxdisk = atof(v);
        else if (!strcmp(k, "netin")) netin = atof(v);
        else if (!strcmp(k, "netout")) netout = atof(v);
        else if (!strcmp(k, "diskread")) diskread = atof(v);
        else if (!strcmp(k, "diskwrite")) diskwrite = atof(v);
        else if (!strcmp(k, "uptime")) uptime = atof(v);
    }
    pclose_status(fp);

    double cpu_time_seconds_total = 0.0;
    int cpu_time_valid = read_guest_cpu_time_seconds(item->type, item->vmid, &cpu_time_seconds_total);
    int have_rate = 0;
    double cpu_usage_fraction = 0.0;

    if (cpu_time_valid) {
        cpu_usage_fraction = compute_guest_cpu_usage_fraction(item->type, item->vmid, cpu_time_seconds_total, cpus, &have_rate);
        if (!have_rate) cpu_usage_fraction = cli_cpu;
    } else {
        cpu_usage_fraction = cli_cpu;
    }
    if (cpu_usage_fraction < 0.0) cpu_usage_fraction = 0.0;

    emit_guest_metric_set(b, host, item->type, item->vmid, name, status,
                          cpu_usage_fraction, cpu_time_seconds_total, cpu_time_valid,
                          cpus, mem, maxmem, disk, maxdisk, netin, netout, diskread, diskwrite, uptime);
}

static void *guest_worker_thread(void *arg) {
    struct guest_worker_arg *wa = (struct guest_worker_arg *)arg;
    for (int i = wa->start_idx; i < wa->end_idx; i++) {
        struct buffer b;
        buf_init(&b, 8192);
        poll_one_guest(&b, wa->host, &wa->items[i]);
        wa->results[i] = b.data;
        wa->result_lens[i] = b.len;
    }
    return NULL;
}

static void build_guest_metrics(struct buffer *b, const char *host) {
    metric_help_type(b, "proxmox_guest_status", "gauge", "Guest running status as 1 for running, 0 otherwise.");
    metric_help_type(b, "proxmox_guest_cpu_usage_fraction", "gauge", "Guest CPU usage fraction derived from cumulative CPU time when available, otherwise Proxmox instantaneous CPU fraction.");
    metric_help_type(b, "proxmox_guest_cpu_time_seconds_total", "counter", "Guest cumulative CPU time in seconds when available from host-side accounting.");
    metric_help_type(b, "proxmox_guest_cpu_count", "gauge", "Configured vCPU count.");
    metric_help_type(b, "proxmox_guest_memory_bytes", "gauge", "Guest memory values in bytes.");
    metric_help_type(b, "proxmox_guest_disk_bytes", "gauge", "Guest disk values in bytes.");
    metric_help_type(b, "proxmox_guest_network_bytes_total", "counter", "Guest network byte counters from Proxmox.");
    metric_help_type(b, "proxmox_guest_disk_io_bytes_total", "counter", "Guest disk IO byte counters from Proxmox.");
    metric_help_type(b, "proxmox_guest_uptime_seconds", "gauge", "Guest uptime in seconds.");
    metric_help_type(b, "proxmox_guest_info", "gauge", "Static guest information.");

    struct guest_item items[MAX_GUEST];
    int n = 0;
    n += collect_guest_list(items + n, MAX_GUEST - n, "qemu");
    n += collect_guest_list(items + n, MAX_GUEST - n, "lxc");
    if (n <= 0) return;

    char **results = (char **)calloc((size_t)n, sizeof(char *));
    size_t *result_lens = (size_t *)calloc((size_t)n, sizeof(size_t));
    if (!results || !result_lens) {
        free(results);
        free(result_lens);
        return;
    }

    int workers = n < GUEST_WORKERS ? n : GUEST_WORKERS;
    pthread_t tids[GUEST_WORKERS];
    struct guest_worker_arg args[GUEST_WORKERS];
    int chunk = (n + workers - 1) / workers;

    for (int w = 0; w < workers; w++) {
        int start = w * chunk;
        int end = start + chunk;
        if (end > n) end = n;
        args[w].items = items;
        args[w].start_idx = start;
        args[w].end_idx = end;
        snprintf(args[w].host, sizeof(args[w].host), "%s", host);
        args[w].results = results;
        args[w].result_lens = result_lens;
        pthread_create(&tids[w], NULL, guest_worker_thread, &args[w]);
    }

    for (int w = 0; w < workers; w++) {
        pthread_join(tids[w], NULL);
    }

    for (int i = 0; i < n; i++) {
        if (results[i] && result_lens[i]) {
            buf_appendf(b, "%s", results[i]);
        }
        free(results[i]);
    }

    free(results);
    free(result_lens);
}

static void build_zfs_metrics(struct buffer *b, const char *host) {
    metric_help_type(b, "proxmox_zfs_pool_size_bytes", "gauge", "ZFS pool total size.");
    metric_help_type(b, "proxmox_zfs_pool_allocated_bytes", "gauge", "ZFS pool allocated bytes.");
    metric_help_type(b, "proxmox_zfs_pool_free_bytes", "gauge", "ZFS pool free bytes.");
    metric_help_type(b, "proxmox_zfs_pool_fragmentation_percent", "gauge", "ZFS pool fragmentation percent.");
    metric_help_type(b, "proxmox_zfs_pool_capacity_percent", "gauge", "ZFS pool capacity percent.");
    metric_help_type(b, "proxmox_zfs_pool_health", "gauge", "ZFS pool health info metric.");
    metric_help_type(b, "proxmox_zfs_dataset_used_bytes", "gauge", "ZFS dataset used bytes.");
    metric_help_type(b, "proxmox_zfs_dataset_available_bytes", "gauge", "ZFS dataset available bytes.");
    metric_help_type(b, "proxmox_zfs_dataset_referenced_bytes", "gauge", "ZFS dataset referenced bytes.");
    metric_help_type(b, "proxmox_zfs_dataset_info", "gauge", "ZFS dataset info.");

    FILE *fp = popen_cmd("zpool list -Hp -o name,size,alloc,free,frag,cap,health 2>/dev/null");
    if (fp) {
        char line[MAX_LINE];
        while (fgets(line, sizeof(line), fp)) {
            char name[128], health[64];
            unsigned long long size=0, alloc=0, freeb=0;
            double frag=0.0, cap=0.0;
            if (sscanf(line, "%127s %llu %llu %llu %lf %lf %63s",
                       name, &size, &alloc, &freeb, &frag, &cap, health) >= 7) {
                metric_value2(b, "proxmox_zfs_pool_size_bytes", "hostname", host, "pool", name, (double)size);
                metric_value2(b, "proxmox_zfs_pool_allocated_bytes", "hostname", host, "pool", name, (double)alloc);
                metric_value2(b, "proxmox_zfs_pool_free_bytes", "hostname", host, "pool", name, (double)freeb);
                metric_value2(b, "proxmox_zfs_pool_fragmentation_percent", "hostname", host, "pool", name, frag);
                metric_value2(b, "proxmox_zfs_pool_capacity_percent", "hostname", host, "pool", name, cap);
                metric_value3(b, "proxmox_zfs_pool_health", "hostname", host, "pool", name, "health", health, 1.0);
            }
        }
        pclose_status(fp);
    }

    fp = popen_cmd("zfs list -Hp -o name,used,avail,refer,type,mountpoint 2>/dev/null");
    if (fp) {
        char line[MAX_LINE];
        while (fgets(line, sizeof(line), fp)) {
            char name[256], type[64], mountpoint[256];
            unsigned long long used=0, avail=0, refer=0;
            if (sscanf(line, "%255s %llu %llu %llu %63s %255s",
                       name, &used, &avail, &refer, type, mountpoint) >= 6) {
                metric_value2(b, "proxmox_zfs_dataset_used_bytes", "hostname", host, "dataset", name, (double)used);
                metric_value2(b, "proxmox_zfs_dataset_available_bytes", "hostname", host, "dataset", name, (double)avail);
                metric_value2(b, "proxmox_zfs_dataset_referenced_bytes", "hostname", host, "dataset", name, (double)refer);
                metric_value4(b, "proxmox_zfs_dataset_info", "hostname", host, "dataset", name, "type", type, "mountpoint", mountpoint, 1.0);
            }
        }
        pclose_status(fp);
    }
}

static int json_find_number_value(const char *json, const char *key, double *out) {
    char pattern[256];
    snprintf(pattern, sizeof(pattern), "\"%s\"", key);
    const char *p = strstr(json, pattern);
    if (!p) return 0;
    p = strchr(p, ':');
    if (!p) return 0;
    p++;
    while (*p && (isspace((unsigned char)*p) || *p == '"')) p++;
    char *end = NULL;
    double v = strtod(p, &end);
    if (end == p) return 0;
    *out = v;
    return 1;
}

static int json_find_string_value(const char *json, const char *key, char *out, size_t outsz) {
    char pattern[256];
    snprintf(pattern, sizeof(pattern), "\"%s\"", key);
    const char *p = strstr(json, pattern);
    if (!p) return 0;
    p = strchr(p, ':');
    if (!p) return 0;
    p++;
    while (*p && *p != '"') p++;
    if (*p != '"') return 0;
    p++;
    size_t j = 0;
    while (*p && *p != '"' && j + 1 < outsz) {
        out[j++] = *p++;
    }
    out[j] = '\0';
    return j > 0;
}

static void build_ceph_metrics(struct buffer *b, const char *host) {
    metric_help_type(b, "proxmox_ceph_health_status", "gauge", "Ceph cluster health state.");
    metric_help_type(b, "proxmox_ceph_osds_total", "gauge", "Ceph total OSD count.");
    metric_help_type(b, "proxmox_ceph_osds_up", "gauge", "Ceph OSDs up.");
    metric_help_type(b, "proxmox_ceph_osds_in", "gauge", "Ceph OSDs in.");
    metric_help_type(b, "proxmox_ceph_raw_storage_bytes", "gauge", "Ceph raw storage bytes by type.");
    metric_help_type(b, "proxmox_ceph_pool_bytes", "gauge", "Ceph per-pool bytes by type.");
    metric_help_type(b, "proxmox_ceph_pool_objects", "gauge", "Ceph per-pool objects.");
    metric_help_type(b, "proxmox_ceph_pool_percent_used", "gauge", "Ceph per-pool percent used.");

    size_t len = 0;
    char *json = read_cmd_output("ceph status --format json-pretty 2>/dev/null", &len);
    if (json) {
        char health[64] = "";
        if (json_find_string_value(json, "status", health, sizeof(health))) {
            metric_value2(b, "proxmox_ceph_health_status", "hostname", host, "state", health, 1.0);
        }
        free(json);
    }

    json = read_cmd_output("ceph osd stat --format json-pretty 2>/dev/null", &len);
    if (json) {
        double v = 0.0;
        if (json_find_number_value(json, "num_osds", &v)) metric_value1(b, "proxmox_ceph_osds_total", "hostname", host, v);
        if (json_find_number_value(json, "num_up_osds", &v)) metric_value1(b, "proxmox_ceph_osds_up", "hostname", host, v);
        if (json_find_number_value(json, "num_in_osds", &v)) metric_value1(b, "proxmox_ceph_osds_in", "hostname", host, v);
        free(json);
    }

    json = read_cmd_output("ceph df --format json-pretty 2>/dev/null", &len);
    if (json) {
        double v = 0.0;
        if (json_find_number_value(json, "total_bytes", &v)) metric_value2(b, "proxmox_ceph_raw_storage_bytes", "hostname", host, "type", "total", v);
        if (json_find_number_value(json, "total_used_raw_bytes", &v)) metric_value2(b, "proxmox_ceph_raw_storage_bytes", "hostname", host, "type", "used_raw", v);
        if (json_find_number_value(json, "total_avail_bytes", &v)) metric_value2(b, "proxmox_ceph_raw_storage_bytes", "hostname", host, "type", "available", v);

        const char *p = json;
        while ((p = strstr(p, "\"name\"")) != NULL) {
            char pool[128] = "";
            if (!json_find_string_value(p, "name", pool, sizeof(pool))) {
                p += 6;
                continue;
            }

            const char *next = strstr(p + 6, "\"name\"");
            size_t block_len = next ? (size_t)(next - p) : strlen(p);
            char *block = (char *)malloc(block_len + 1);
            if (!block) break;
            memcpy(block, p, block_len);
            block[block_len] = '\0';

            double stored = 0.0, bytes_used = 0.0, max_avail = 0.0, objects = 0.0, percent_used = 0.0;
            int have_stored = json_find_number_value(block, "stored", &stored);
            int have_used = json_find_number_value(block, "bytes_used", &bytes_used);
            int have_avail = json_find_number_value(block, "max_avail", &max_avail);
            int have_objects = json_find_number_value(block, "objects", &objects);
            int have_pct = json_find_number_value(block, "percent_used", &percent_used);

            if (have_stored) metric_value3(b, "proxmox_ceph_pool_bytes", "hostname", host, "pool", pool, "type", "stored", stored);
            if (have_used) metric_value3(b, "proxmox_ceph_pool_bytes", "hostname", host, "pool", pool, "type", "used", bytes_used);
            if (have_avail) metric_value3(b, "proxmox_ceph_pool_bytes", "hostname", host, "pool", pool, "type", "max_avail", max_avail);
            if (have_objects) metric_value2(b, "proxmox_ceph_pool_objects", "hostname", host, "pool", pool, objects);
            if (have_pct) metric_value2(b, "proxmox_ceph_pool_percent_used", "hostname", host, "pool", pool, percent_used);

            free(block);
            p += 6;
        }
        free(json);
    }
}

static void build_fast_metrics(struct buffer *b, const char *host) {
    emit_os_metrics(b, host);
    emit_cpu_metrics(b, host);
    emit_mem_metrics(b, host);
    emit_load_metrics(b, host);
    emit_process_metrics(b, host);
    emit_net_metrics(b, host);
    emit_diskstats_metrics(b, host);
    emit_filesystem_metrics(b, host);
    emit_hwmon_metrics(b, host);
    emit_mdraid_metrics(b, host);
}

static void build_exporter_meta_metrics(struct buffer *b, const char *host) {
    size_t len = 0;
    double age = 0.0, duration = 0.0;
    int success = 0;

    metric_help_type(b, "proxmox_exporter_up", "gauge", "Exporter is running.");
    metric_help_type(b, "proxmox_exporter_scrape_timestamp_seconds", "gauge", "Exporter scrape generation time.");
    metric_help_type(b, "proxmox_exporter_cache_age_seconds", "gauge", "Age of cached collector section.");
    metric_help_type(b, "proxmox_exporter_collect_duration_seconds", "gauge", "Duration of last collector section run.");
    metric_help_type(b, "proxmox_exporter_collect_success", "gauge", "Whether last collector section run succeeded.");

    metric_value1(b, "proxmox_exporter_up", "hostname", host, 1.0);
    metric_value1(b, "proxmox_exporter_scrape_timestamp_seconds", "hostname", host, (double)time(NULL));

    free(cache_snapshot(&g_cache_fast, &len, &age, &duration, &success));
    metric_value2(b, "proxmox_exporter_cache_age_seconds", "hostname", host, "section", "fast", age);
    metric_value2(b, "proxmox_exporter_collect_duration_seconds", "hostname", host, "section", "fast", duration);
    metric_value2(b, "proxmox_exporter_collect_success", "hostname", host, "section", "fast", success);

    free(cache_snapshot(&g_cache_guest, &len, &age, &duration, &success));
    metric_value2(b, "proxmox_exporter_cache_age_seconds", "hostname", host, "section", "guest", age);
    metric_value2(b, "proxmox_exporter_collect_duration_seconds", "hostname", host, "section", "guest", duration);
    metric_value2(b, "proxmox_exporter_collect_success", "hostname", host, "section", "guest", success);

    free(cache_snapshot(&g_cache_smart, &len, &age, &duration, &success));
    metric_value2(b, "proxmox_exporter_cache_age_seconds", "hostname", host, "section", "smart", age);
    metric_value2(b, "proxmox_exporter_collect_duration_seconds", "hostname", host, "section", "smart", duration);
    metric_value2(b, "proxmox_exporter_collect_success", "hostname", host, "section", "smart", success);

    free(cache_snapshot(&g_cache_zfs, &len, &age, &duration, &success));
    metric_value2(b, "proxmox_exporter_cache_age_seconds", "hostname", host, "section", "zfs", age);
    metric_value2(b, "proxmox_exporter_collect_duration_seconds", "hostname", host, "section", "zfs", duration);
    metric_value2(b, "proxmox_exporter_collect_success", "hostname", host, "section", "zfs", success);

    free(cache_snapshot(&g_cache_ceph, &len, &age, &duration, &success));
    metric_value2(b, "proxmox_exporter_cache_age_seconds", "hostname", host, "section", "ceph", age);
    metric_value2(b, "proxmox_exporter_collect_duration_seconds", "hostname", host, "section", "ceph", duration);
    metric_value2(b, "proxmox_exporter_collect_success", "hostname", host, "section", "ceph", success);
}

static void update_cache_with_builder(struct cache_block *cache,
                                      void (*builder)(struct buffer *, const char *),
                                      const char *host) {
    double start = now_seconds();
    struct buffer b;
    buf_init(&b, 65536);
    int ok = 1;
    builder(&b, host);
    double dur = now_seconds() - start;
    cache_store(cache, b.data, b.len, dur, ok);
}

static void *fast_collector_thread(void *arg) {
    const char *host = (const char *)arg;
    while (g_running) {
        update_cache_with_builder(&g_cache_fast, build_fast_metrics, host);
        sleep_interruptible(FAST_INTERVAL_SEC);
    }
    return NULL;
}

static void *guest_collector_thread(void *arg) {
    const char *host = (const char *)arg;
    while (g_running) {
        update_cache_with_builder(&g_cache_guest, build_guest_metrics, host);
        sleep_interruptible(GUEST_INTERVAL_SEC);
    }
    return NULL;
}

static void *smart_collector_thread(void *arg) {
    const char *host = (const char *)arg;
    while (g_running) {
        update_cache_with_builder(&g_cache_smart, build_smart_metrics, host);
        sleep_interruptible(SMART_INTERVAL_SEC);
    }
    return NULL;
}

static void *zfs_collector_thread(void *arg) {
    const char *host = (const char *)arg;
    while (g_running) {
        update_cache_with_builder(&g_cache_zfs, build_zfs_metrics, host);
        sleep_interruptible(ZFS_INTERVAL_SEC);
    }
    return NULL;
}

static void *ceph_collector_thread(void *arg) {
    const char *host = (const char *)arg;
    while (g_running) {
        update_cache_with_builder(&g_cache_ceph, build_ceph_metrics, host);
        sleep_interruptible(CEPH_INTERVAL_SEC);
    }
    return NULL;
}

static char *generate_metrics(size_t *out_len) {
    char host[256] = "unknown";
    gethostname(host, sizeof(host) - 1);
    host[sizeof(host) - 1] = '\0';

    struct buffer b;
    buf_init(&b, 262144);

    build_exporter_meta_metrics(&b, host);

    size_t len = 0;
    double age = 0.0, dur = 0.0;
    int success = 0;
    char *snap = NULL;

    snap = cache_snapshot(&g_cache_fast, &len, &age, &dur, &success);
    if (snap && len) buf_appendf(&b, "%s", snap);
    free(snap);

    snap = cache_snapshot(&g_cache_guest, &len, &age, &dur, &success);
    if (snap && len) buf_appendf(&b, "%s", snap);
    free(snap);

    snap = cache_snapshot(&g_cache_smart, &len, &age, &dur, &success);
    if (snap && len) buf_appendf(&b, "%s", snap);
    free(snap);

    snap = cache_snapshot(&g_cache_zfs, &len, &age, &dur, &success);
    if (snap && len) buf_appendf(&b, "%s", snap);
    free(snap);

    snap = cache_snapshot(&g_cache_ceph, &len, &age, &dur, &success);
    if (snap && len) buf_appendf(&b, "%s", snap);
    free(snap);

    *out_len = b.len;
    return b.data;
}

static void send_all(int fd, const char *data, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = send(fd, data + off, len - off, 0);
        if (n <= 0) return;
        off += (size_t)n;
    }
}

static void send_response(int fd, int code, const char *status, const char *ctype, const char *body, size_t bodylen) {
    char hdr[1024];
    int n = snprintf(hdr, sizeof(hdr),
                     "HTTP/1.1 %d %s\r\n"
                     "Content-Type: %s\r\n"
                     "Content-Length: %zu\r\n"
                     "Connection: close\r\n"
                     "Cache-Control: no-cache\r\n\r\n",
                     code, status, ctype, bodylen);
    if (n < 0) return;
    send_all(fd, hdr, (size_t)n);
    if (body && bodylen) send_all(fd, body, bodylen);
}

static void handle_client(int cfd) {
    char req[8192];
    ssize_t n = recv(cfd, req, sizeof(req) - 1, 0);
    if (n <= 0) return;
    req[n] = '\0';

    char method[16] = "", path[1024] = "";
    sscanf(req, "%15s %1023s", method, path);

    if (strcmp(method, "GET") != 0) {
        const char *body = "method not allowed\n";
        send_response(cfd, 405, "Method Not Allowed", "text/plain; charset=utf-8", body, strlen(body));
        return;
    }

    if (strcmp(path, "/") == 0) {
        const char *body = "proxmox_exporter ok\nGET /metrics\n";
        send_response(cfd, 200, "OK", "text/plain; charset=utf-8", body, strlen(body));
        return;
    }

    if (strcmp(path, "/metrics") == 0) {
        size_t len = 0;
        char *metrics = generate_metrics(&len);
        send_response(cfd, 200, "OK", "text/plain; version=0.0.4; charset=utf-8", metrics, len);
        free(metrics);
        return;
    }

    {
        const char *body = "not found\n";
        send_response(cfd, 404, "Not Found", "text/plain; charset=utf-8", body, strlen(body));
    }
}

int main(int argc, char **argv) {
    int port = DEFAULT_PORT;
    if (argc > 1) port = atoi(argv[1]);
    if (port <= 0 || port > 65535) port = DEFAULT_PORT;

    signal(SIGINT, handle_signal);
    signal(SIGTERM, handle_signal);
    signal(SIGPIPE, SIG_IGN);

    cache_init(&g_cache_fast);
    cache_init(&g_cache_guest);
    cache_init(&g_cache_smart);
    cache_init(&g_cache_zfs);
    cache_init(&g_cache_ceph);

    char *host = (char *)malloc(256);
    if (!host) {
        perror("malloc");
        return 1;
    }
    gethostname(host, 255);
    host[255] = '\0';

    pthread_t th_fast, th_guest, th_smart, th_zfs, th_ceph;
    pthread_create(&th_fast, NULL, fast_collector_thread, host);
    pthread_create(&th_guest, NULL, guest_collector_thread, host);
    pthread_create(&th_smart, NULL, smart_collector_thread, host);
    pthread_create(&th_zfs, NULL, zfs_collector_thread, host);
    pthread_create(&th_ceph, NULL, ceph_collector_thread, host);

    sleep_interruptible(0.2);

    int s = socket(AF_INET, SOCK_STREAM, 0);
    if (s < 0) {
        perror("socket");
        g_running = 0;
        return 1;
    }

    int opt = 1;
    setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(s, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        perror("bind");
        close(s);
        g_running = 0;
        return 1;
    }
    if (listen(s, LISTEN_BACKLOG) != 0) {
        perror("listen");
        close(s);
        g_running = 0;
        return 1;
    }

    fprintf(stderr, "proxmox_exporter listening on 0.0.0.0:%d\n", port);

    while (g_running) {
        struct sockaddr_in cli;
        socklen_t clilen = sizeof(cli);
        int cfd = accept(s, (struct sockaddr *)&cli, &clilen);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            break;
        }
        handle_client(cfd);
        close(cfd);
    }

    close(s);
    g_running = 0;

    pthread_join(th_fast, NULL);
    pthread_join(th_guest, NULL);
    pthread_join(th_smart, NULL);
    pthread_join(th_zfs, NULL);
    pthread_join(th_ceph, NULL);

    free(host);
    return 0;
}