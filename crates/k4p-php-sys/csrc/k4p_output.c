/*
 * The log sink, and script output (SAPI ub_write) forwarded to it one line at a time
 * (DESIGN.md §5.1.1, "Log callback"). No PHP.
 */
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "k4p_internal.h"

#define K4P_LINE_MAX 8192

/* Set once by k4p_runtime_init(), before any PHP thread exists. */
static k4p_log_fn k4p_log_fn_ptr;
static void *k4p_log_user_data;

/* Per-thread stdout line buffer, K4P_LINE_MAX bytes, lazily allocated. */
static __thread char *k4p_line;
static __thread size_t k4p_line_len;

void k4p_log_init(k4p_log_fn log, void *user_data)
{
    k4p_log_fn_ptr = log;
    k4p_log_user_data = user_data;
}

void k4p_log_raw(k4p_log_level level, const char *source, const char *msg, size_t len)
{
    if (k4p_log_fn_ptr) {
        k4p_log_fn_ptr(k4p_log_user_data, level, source, (const uint8_t *)msg, len);
    }
}

void k4p_logf(k4p_log_level level, const char *fmt, ...)
{
    char buf[1024];
    va_list ap;
    int n;

    va_start(ap, fmt);
    n = vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    if (n < 0) {
        return;
    }
    k4p_log_raw(level, "sapi", buf, (size_t)n < sizeof(buf) ? (size_t)n : sizeof(buf) - 1);
}

static void k4p_line_emit(void)
{
    size_t len = k4p_line_len;

    if (len && k4p_line[len - 1] == '\r') {
        len--;
    }
    if (len) {
        k4p_log_raw(K4P_LOG_INFO, "stdout", k4p_line, len);
    }
    k4p_line_len = 0;
}

static void k4p_line_append(const char *s, size_t n)
{
    if (!k4p_line) {
        k4p_line = malloc(K4P_LINE_MAX);
        if (!k4p_line) {
            return;
        }
        k4p_line_len = 0;
    }
    while (n) {
        size_t take = K4P_LINE_MAX - k4p_line_len;
        if (take > n) {
            take = n;
        }
        memcpy(k4p_line + k4p_line_len, s, take);
        k4p_line_len += take;
        s += take;
        n -= take;
        if (k4p_line_len == K4P_LINE_MAX) {
            k4p_line_emit(); /* overlong line: emitted in chunks */
        }
    }
}

size_t k4p_output_write(const char *str, size_t len)
{
    const char *p = str;
    size_t left = len;

    while (left) {
        const char *nl = memchr(p, '\n', left);
        if (!nl) {
            k4p_line_append(p, left);
            break;
        }
        k4p_line_append(p, (size_t)(nl - p));
        k4p_line_emit();
        left -= (size_t)(nl - p) + 1;
        p = nl + 1;
    }
    return len;
}

void k4p_output_flush(void)
{
    if (k4p_line && k4p_line_len) {
        k4p_line_emit();
    }
}

void k4p_output_release(void)
{
    k4p_output_flush();
    free(k4p_line);
    k4p_line = NULL;
    k4p_line_len = 0;
}
