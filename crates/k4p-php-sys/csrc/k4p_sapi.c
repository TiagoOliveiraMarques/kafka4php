/*
 * kafka4php SAPI (DESIGN.md §5.1-5.3).
 *
 * The custom "kafka4php" SAPI module, the native module with
 * Kafka4php\Native\message() / result(), the per-thread request state, the outcome
 * classification and the C ABI declared in k4p_sapi.h.
 *
 * References: PHP's sapi/embed/php_embed.c, main/main.c (php_execute_script_ex) and
 * Zend/zend.c (zend_execute_script); FrankenPHP's frankenphp.c (MIT).
 *
 * Built with ZEND_ENABLE_STATIC_TSRMLS_CACHE (see build.rs): every thread that touches
 * PHP globals runs ZEND_TSRMLS_CACHE_UPDATE() after getting its interpreter globals.
 */
#include "k4p_sapi.h"

#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <syslog.h>

#include "php.h"
#include "php_main.h"
#include "php_variables.h"
#include "SAPI.h"
#include "TSRM.h"
#include "zend_exceptions.h"
#include "zend_observer.h"
#include "zend_smart_str.h"
#include "ext/spl/spl_exceptions.h"
#include "ext/standard/basic_functions.h"

#ifndef ZTS
#error "kafka4php requires a ZTS build of PHP"
#endif

ZEND_TSRMLS_CACHE_DEFINE()

#define K4P_LINE_MAX 8192
#define K4P_ERR_MAX 512

/* ------------------------------------------------------------------------------ */
/* Process-wide runtime state. Written only by k4p_runtime_init/shutdown.          */

static struct {
    int initialized;
    int shut_down;
    char *ini_file;
    char *ini_entries;
    char *entrypoint;
    k4p_log_fn log;
    void *log_user_data;
} k4p_rt;

/* ------------------------------------------------------------------------------ */
/* Per-thread state. Plain C thread-local storage: no zval or other request memory  */
/* is ever stored here, only malloc()ed copies.                                     */

enum { K4P_RESULT_NONE = 0, K4P_RESULT_VALID, K4P_RESULT_INVALID };

typedef struct k4p_thread_state {
    int is_runtime_thread; /* called k4p_runtime_init() */
    int thread_inited;     /* called k4p_thread_init() */
    int broken;            /* php_request_startup() failed; thread is unusable */

    const k4p_message *msg; /* non-NULL from request startup until request shutdown */
    int accepting_result;   /* result() allowed: script + shutdown functions */

    int result_state;
    k4p_outcome result;     /* when VALID */
    char invalid[K4P_ERR_MAX]; /* when INVALID */
    uint32_t invalid_handle;   /* object handle of the \ValueError thrown by result() */

    int has_uncaught;
    uint32_t uncaught_handle;
    k4p_buf uncaught_class;
    k4p_buf uncaught_message;

    char *line; /* stdout line buffer, K4P_LINE_MAX bytes, lazily allocated */
    size_t line_len;
} k4p_thread_state;

static __thread k4p_thread_state k4p_ts;

/* ------------------------------------------------------------------------------ */
/* Small helpers.                                                                   */

static void k4p_log_raw(k4p_log_level level, const char *source, const char *msg, size_t len)
{
    if (k4p_rt.log) {
        k4p_rt.log(k4p_rt.log_user_data, level, source, (const uint8_t *)msg, len);
    }
}

static void k4p_logf(k4p_log_level level, const char *fmt, ...)
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

static char *k4p_strdup(const char *s)
{
    char *d;
    size_t n;

    if (!s) {
        return NULL;
    }
    n = strlen(s) + 1;
    d = malloc(n);
    if (d) {
        memcpy(d, s, n);
    }
    return d;
}

static k4p_buf k4p_buf_null(void)
{
    k4p_buf b = {NULL, 0, 1};
    return b;
}

/* malloc()ed copy of `len` bytes. Never returns a NULL `data` for a non-null buffer
 * (an empty buffer still gets a 1-byte allocation) so Rust can build slices safely. */
static k4p_buf k4p_buf_copy(const char *data, size_t len)
{
    k4p_buf b;
    uint8_t *p = malloc(len ? len : 1);

    if (!p) {
        /* Out of process memory: there is no sane way to continue. */
        fprintf(stderr, "kafka4php: out of memory\n");
        abort();
    }
    if (len) {
        memcpy(p, data, len);
    }
    b.data = p;
    b.len = len;
    b.is_null = 0;
    return b;
}

static k4p_buf k4p_buf_cstr(const char *s)
{
    return k4p_buf_copy(s, strlen(s));
}

static void k4p_buf_free(k4p_buf *b)
{
    free((void *)b->data);
    b->data = NULL;
    b->len = 0;
    b->is_null = 1;
}

static void k4p_headers_free(k4p_header *h, size_t n)
{
    size_t i;

    for (i = 0; i < n; i++) {
        k4p_buf_free(&h[i].key);
        k4p_buf_free(&h[i].value);
    }
    free(h);
}

void k4p_outcome_free(k4p_outcome *out)
{
    size_t i;

    if (!out) {
        return;
    }
    for (i = 0; i < out->messages_len; i++) {
        k4p_outbound *m = &out->messages[i];
        k4p_buf_free(&m->topic);
        k4p_buf_free(&m->key);
        k4p_buf_free(&m->value);
        k4p_headers_free(m->headers, m->headers_len);
    }
    free(out->messages);
    k4p_buf_free(&out->text);
    k4p_buf_free(&out->class_name);
    memset(out, 0, sizeof(*out));
    out->kind = K4P_OUTCOME_NO_CONTENT;
    out->text.is_null = 1;
    out->class_name.is_null = 1;
}

static void k4p_outcome_init(k4p_outcome *out, k4p_outcome_kind kind)
{
    memset(out, 0, sizeof(*out));
    out->kind = kind;
    out->text = k4p_buf_null();
    out->class_name = k4p_buf_null();
}

static void k4p_outcome_bootstrap(k4p_outcome *out, const char *why)
{
    k4p_outcome_init(out, K4P_OUTCOME_BOOTSTRAP);
    out->text = k4p_buf_cstr(why);
}

/* Strict UTF-8 validation (no overlongs, no surrogates, max U+10FFFF). */
static int k4p_utf8_valid(const unsigned char *s, size_t len)
{
    size_t i = 0;

    while (i < len) {
        unsigned char c = s[i];
        size_t n;
        uint32_t cp;

        if (c < 0x80) {
            i++;
            continue;
        } else if ((c & 0xE0) == 0xC0) {
            n = 1;
            cp = c & 0x1F;
        } else if ((c & 0xF0) == 0xE0) {
            n = 2;
            cp = c & 0x0F;
        } else if ((c & 0xF8) == 0xF0) {
            n = 3;
            cp = c & 0x07;
        } else {
            return 0;
        }
        if (len - i <= n) { /* truncated sequence */
            return 0;
        }
        for (size_t k = 1; k <= n; k++) {
            if ((s[i + k] & 0xC0) != 0x80) {
                return 0;
            }
            cp = (cp << 6) | (s[i + k] & 0x3F);
        }
        if ((n == 1 && cp < 0x80) || (n == 2 && cp < 0x800) || (n == 3 && cp < 0x10000) ||
            cp > 0x10FFFF || (cp >= 0xD800 && cp <= 0xDFFF)) {
            return 0;
        }
        i += n + 1;
    }
    return 1;
}

/* ------------------------------------------------------------------------------ */
/* Output: ub_write -> log callback, one call per line.                             */

static void k4p_line_emit(void)
{
    size_t len = k4p_ts.line_len;

    if (len && k4p_ts.line[len - 1] == '\r') {
        len--;
    }
    if (len) {
        k4p_log_raw(K4P_LOG_INFO, "stdout", k4p_ts.line, len);
    }
    k4p_ts.line_len = 0;
}

static void k4p_line_append(const char *s, size_t n)
{
    if (!k4p_ts.line) {
        k4p_ts.line = malloc(K4P_LINE_MAX);
        if (!k4p_ts.line) {
            return;
        }
        k4p_ts.line_len = 0;
    }
    while (n) {
        size_t take = K4P_LINE_MAX - k4p_ts.line_len;
        if (take > n) {
            take = n;
        }
        memcpy(k4p_ts.line + k4p_ts.line_len, s, take);
        k4p_ts.line_len += take;
        s += take;
        n -= take;
        if (k4p_ts.line_len == K4P_LINE_MAX) {
            k4p_line_emit(); /* overlong line: emitted in chunks */
        }
    }
}

static void k4p_line_flush(void)
{
    if (k4p_ts.line && k4p_ts.line_len) {
        k4p_line_emit();
    }
}

static void k4p_line_release(void)
{
    k4p_line_flush();
    free(k4p_ts.line);
    k4p_ts.line = NULL;
    k4p_ts.line_len = 0;
}

static size_t k4p_ub_write(const char *str, size_t len)
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

/* ------------------------------------------------------------------------------ */
/* SAPI module callbacks.                                                           */

static zend_module_entry k4p_native_module;

static int k4p_sapi_startup(sapi_module_struct *module)
{
    return php_module_startup(module, &k4p_native_module);
}

static void k4p_sapi_flush(void *server_context)
{
    (void)server_context;
}

static int k4p_sapi_header_handler(sapi_header_struct *h, sapi_header_op_enum op,
                                   sapi_headers_struct *headers)
{
    (void)h;
    (void)op;
    (void)headers;
    return 0; /* discard: there is no HTTP response */
}

static int k4p_sapi_send_headers(sapi_headers_struct *headers)
{
    (void)headers;
    return SAPI_HEADER_SENT_SUCCESSFULLY;
}

static void k4p_sapi_send_header(sapi_header_struct *h, void *server_context)
{
    (void)h;
    (void)server_context;
}

static char *k4p_sapi_read_cookies(void)
{
    return NULL;
}

static void k4p_sapi_register_variables(zval *track_vars_array)
{
    php_import_environment_variables(track_vars_array);
    if (k4p_rt.entrypoint) {
        php_register_variable("SCRIPT_FILENAME", k4p_rt.entrypoint, track_vars_array);
        php_register_variable("SCRIPT_NAME", k4p_rt.entrypoint, track_vars_array);
        php_register_variable("PHP_SELF", k4p_rt.entrypoint, track_vars_array);
        php_register_variable("PATH_TRANSLATED", k4p_rt.entrypoint, track_vars_array);
    }
}

static void k4p_sapi_log_message(const char *message, int syslog_type_int)
{
    k4p_log_level level = syslog_type_int <= LOG_ERR ? K4P_LOG_ERROR : K4P_LOG_WARN;
    k4p_log_raw(level, "error", message, strlen(message));
}

/* Lowest-precedence INI defaults: any ini file or override wins. */
#define K4P_INI_DEFAULT(name, value)                                                 \
    ZVAL_NEW_STR(&tmp, zend_string_init(value, sizeof(value) - 1, 1));                \
    zend_hash_str_update(configuration_hash, name, sizeof(name) - 1, &tmp);

static void k4p_sapi_ini_defaults(HashTable *configuration_hash)
{
    zval tmp;
    K4P_INI_DEFAULT("display_errors", "0");
    K4P_INI_DEFAULT("log_errors", "1");
    K4P_INI_DEFAULT("html_errors", "0");
    K4P_INI_DEFAULT("register_argc_argv", "0");
    K4P_INI_DEFAULT("output_buffering", "0");
}

static sapi_module_struct k4p_sapi_module = {
    "kafka4php",                       /* name (PHP_SAPI) */
    "kafka4php Kafka consumer host",   /* pretty name */
    k4p_sapi_startup,                  /* startup */
    php_module_shutdown_wrapper,       /* shutdown */
    NULL,                              /* activate */
    NULL,                              /* deactivate */
    k4p_ub_write,                      /* unbuffered write */
    k4p_sapi_flush,                    /* flush */
    NULL,                              /* get uid */
    NULL,                              /* getenv */
    php_error,                         /* error handler */
    k4p_sapi_header_handler,           /* header handler */
    k4p_sapi_send_headers,             /* send headers handler */
    k4p_sapi_send_header,              /* send header handler */
    NULL,                              /* read POST data */
    k4p_sapi_read_cookies,             /* read Cookies */
    k4p_sapi_register_variables,       /* register server variables */
    k4p_sapi_log_message,              /* Log message */
    NULL,                              /* Get request time */
    NULL,                              /* Child terminate */
    STANDARD_SAPI_MODULE_PROPERTIES
};

/* ------------------------------------------------------------------------------ */
/* Native functions: Kafka4php\Native\message() / result().                         */

static void k4p_add_buf(zval *arr, const char *key, const k4p_buf *b)
{
    if (b->is_null) {
        add_assoc_null_ex(arr, key, strlen(key));
    } else {
        add_assoc_stringl_ex(arr, key, strlen(key), (const char *)b->data, b->len);
    }
}

static void k4p_add_next_buf(zval *arr, const k4p_buf *b)
{
    if (b->is_null) {
        add_next_index_null(arr);
    } else {
        add_next_index_stringl(arr, (const char *)b->data, b->len);
    }
}

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_k4p_message, 0, 0, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_k4p_result, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, result, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

static ZEND_FUNCTION(k4p_native_message)
{
    const k4p_message *m = k4p_ts.msg;
    zval headers;
    size_t i;

    ZEND_PARSE_PARAMETERS_NONE();

    if (!m) {
        zend_throw_exception(spl_ce_LogicException,
                             "Kafka4php\\Native\\message() called outside of a kafka4php request",
                             0);
        RETURN_THROWS();
    }

    array_init_size(return_value, 9);
    k4p_add_buf(return_value, "topic", &m->topic);
    k4p_add_buf(return_value, "delivery_topic", &m->delivery_topic);
    add_assoc_long_ex(return_value, "partition", sizeof("partition") - 1, m->partition);
    add_assoc_long_ex(return_value, "offset", sizeof("offset") - 1, (zend_long)m->offset);
    if (m->has_timestamp) {
        add_assoc_long_ex(return_value, "timestamp", sizeof("timestamp") - 1,
                          (zend_long)m->timestamp_ms);
    } else {
        add_assoc_null_ex(return_value, "timestamp", sizeof("timestamp") - 1);
    }
    k4p_add_buf(return_value, "key", &m->key);
    k4p_add_buf(return_value, "value", &m->value);

    array_init_size(&headers, (uint32_t)m->headers_len);
    for (i = 0; i < m->headers_len; i++) {
        zval pair;
        array_init_size(&pair, 2);
        k4p_add_next_buf(&pair, &m->headers[i].key);
        k4p_add_next_buf(&pair, &m->headers[i].value);
        add_next_index_zval(&headers, &pair);
    }
    add_assoc_zval_ex(return_value, "headers", sizeof("headers") - 1, &headers);
    add_assoc_long_ex(return_value, "attempt", sizeof("attempt") - 1, (zend_long)m->attempt);
}

/* --- result() validation + conversion into malloc()ed C structs ------------------ */

typedef struct {
    char *buf;
    size_t cap;
} k4p_err;

static int k4p_fail(k4p_err *e, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(e->buf, e->cap, fmt, ap);
    va_end(ap);
    return FAILURE;
}

static zval *k4p_find(HashTable *ht, const char *key)
{
    zval *zv = zend_hash_str_find(ht, key, strlen(key));
    if (zv) {
        ZVAL_DEREF(zv);
    }
    return zv;
}

/* Rejects keys not in `allowed` (NULL-terminated). */
static int k4p_check_keys(HashTable *ht, const char *const *allowed, const char *path, k4p_err *e)
{
    zend_string *key;
    zend_ulong idx;

    ZEND_HASH_FOREACH_KEY(ht, idx, key) {
        const char *const *a;
        int ok = 0;
        if (key) {
            for (a = allowed; *a; a++) {
                if (zend_string_equals_cstr(key, *a, strlen(*a))) {
                    ok = 1;
                    break;
                }
            }
        }
        if (!ok) {
            if (key) {
                return k4p_fail(e, "%sunexpected key \"%s\"", path, ZSTR_VAL(key));
            }
            return k4p_fail(e, "%sunexpected key " ZEND_ULONG_FMT, path, idx);
        }
    } ZEND_HASH_FOREACH_END();
    return SUCCESS;
}

/* `zv` may be NULL (missing). Required strings: missing or non-string fails. */
static int k4p_req_string(zval *zv, const char *path, const char *name, k4p_buf *out, k4p_err *e)
{
    if (!zv || Z_TYPE_P(zv) != IS_STRING) {
        return k4p_fail(e, "%s%s must be a string", path, name);
    }
    *out = k4p_buf_copy(Z_STRVAL_P(zv), Z_STRLEN_P(zv));
    return SUCCESS;
}

/* Nullable strings: missing counts as null when `optional`. */
static int k4p_nullable_string(zval *zv, int optional, const char *path, const char *name,
                               k4p_buf *out, k4p_err *e)
{
    if (!zv) {
        if (!optional) {
            return k4p_fail(e, "%s%s is required (string or null)", path, name);
        }
        *out = k4p_buf_null();
        return SUCCESS;
    }
    if (Z_TYPE_P(zv) == IS_NULL) {
        *out = k4p_buf_null();
        return SUCCESS;
    }
    if (Z_TYPE_P(zv) != IS_STRING) {
        return k4p_fail(e, "%s%s must be a string or null", path, name);
    }
    *out = k4p_buf_copy(Z_STRVAL_P(zv), Z_STRLEN_P(zv));
    return SUCCESS;
}

static int k4p_convert_headers(zval *zv, const char *path, k4p_header **out, size_t *out_len,
                               k4p_err *e)
{
    HashTable *ht;
    zval *pair;
    size_t n, i = 0;

    *out = NULL;
    *out_len = 0;
    if (!zv) {
        return SUCCESS;
    }
    if (Z_TYPE_P(zv) != IS_ARRAY || !zend_array_is_list(Z_ARRVAL_P(zv))) {
        return k4p_fail(e, "%sheaders must be a list of [name, value] pairs", path);
    }
    ht = Z_ARRVAL_P(zv);
    n = zend_hash_num_elements(ht);
    if (!n) {
        return SUCCESS;
    }
    *out = calloc(n, sizeof(k4p_header));
    if (!*out) {
        return k4p_fail(e, "out of memory");
    }
    for (i = 0; i < n; i++) {
        (*out)[i].key = k4p_buf_null();
        (*out)[i].value = k4p_buf_null();
    }
    *out_len = n;

    i = 0;
    ZEND_HASH_FOREACH_VAL(ht, pair) {
        zval *name, *value;
        ZVAL_DEREF(pair);
        if (Z_TYPE_P(pair) != IS_ARRAY || !zend_array_is_list(Z_ARRVAL_P(pair)) ||
            zend_hash_num_elements(Z_ARRVAL_P(pair)) != 2) {
            return k4p_fail(e, "%sheaders[%zu] must be a [name, value] pair", path, i);
        }
        name = zend_hash_index_find(Z_ARRVAL_P(pair), 0);
        value = zend_hash_index_find(Z_ARRVAL_P(pair), 1);
        ZVAL_DEREF(name);
        ZVAL_DEREF(value);
        if (Z_TYPE_P(name) != IS_STRING ||
            !k4p_utf8_valid((const unsigned char *)Z_STRVAL_P(name), Z_STRLEN_P(name))) {
            return k4p_fail(e, "%sheaders[%zu][0] (name) must be a valid UTF-8 string", path, i);
        }
        if (Z_TYPE_P(value) != IS_NULL && Z_TYPE_P(value) != IS_STRING) {
            return k4p_fail(e, "%sheaders[%zu][1] (value) must be a string or null", path, i);
        }
        (*out)[i].key = k4p_buf_copy(Z_STRVAL_P(name), Z_STRLEN_P(name));
        if (Z_TYPE_P(value) == IS_STRING) {
            (*out)[i].value = k4p_buf_copy(Z_STRVAL_P(value), Z_STRLEN_P(value));
        }
        i++;
    } ZEND_HASH_FOREACH_END();
    return SUCCESS;
}

static int k4p_convert_outbound(zval *zv, size_t idx, k4p_outbound *m, k4p_err *e)
{
    static const char *const allowed[] = {"topic", "value", "key", "headers", "partition", NULL};
    char path[64];
    HashTable *ht;
    zval *topic, *partition;

    snprintf(path, sizeof(path), "messages[%zu].", idx);
    ZVAL_DEREF(zv);
    if (Z_TYPE_P(zv) != IS_ARRAY) {
        return k4p_fail(e, "messages[%zu] must be an array", idx);
    }
    ht = Z_ARRVAL_P(zv);
    if (k4p_check_keys(ht, allowed, path, e) != SUCCESS) {
        return FAILURE;
    }

    topic = k4p_find(ht, "topic");
    if (!topic || Z_TYPE_P(topic) != IS_STRING || Z_STRLEN_P(topic) == 0 ||
        !k4p_utf8_valid((const unsigned char *)Z_STRVAL_P(topic), Z_STRLEN_P(topic))) {
        return k4p_fail(e, "%stopic must be a non-empty valid UTF-8 string", path);
    }
    m->topic = k4p_buf_copy(Z_STRVAL_P(topic), Z_STRLEN_P(topic));

    if (k4p_nullable_string(k4p_find(ht, "value"), 0, path, "value", &m->value, e) != SUCCESS ||
        k4p_nullable_string(k4p_find(ht, "key"), 1, path, "key", &m->key, e) != SUCCESS ||
        k4p_convert_headers(k4p_find(ht, "headers"), path, &m->headers, &m->headers_len, e) !=
            SUCCESS) {
        return FAILURE;
    }

    partition = k4p_find(ht, "partition");
    if (partition && Z_TYPE_P(partition) != IS_NULL) {
        if (Z_TYPE_P(partition) != IS_LONG || Z_LVAL_P(partition) < 0 ||
            Z_LVAL_P(partition) > INT32_MAX) {
            return k4p_fail(e, "%spartition must be null or an int between 0 and %d", path,
                            INT32_MAX);
        }
        m->has_partition = 1;
        m->partition = (int32_t)Z_LVAL_P(partition);
    }
    return SUCCESS;
}

/* On failure, `out` may be partially filled; the caller frees it. */
static int k4p_convert_result(HashTable *ht, k4p_outcome *out, k4p_err *e)
{
    static const char *const keys_no_content[] = {"type", NULL};
    static const char *const keys_content[] = {"type", "messages", NULL};
    static const char *const keys_fail[] = {"type", "reason", "error_class", NULL};
    static const char *const keys_retry[] = {"type", "reason", NULL};
    zval *type = k4p_find(ht, "type");

    if (!type || Z_TYPE_P(type) != IS_STRING) {
        return k4p_fail(e, "type must be one of \"no_content\", \"content\", \"fail\", "
                           "\"retry_later\"");
    }

    if (zend_string_equals_literal(Z_STR_P(type), "no_content")) {
        k4p_outcome_init(out, K4P_OUTCOME_NO_CONTENT);
        return k4p_check_keys(ht, keys_no_content, "", e);
    }

    if (zend_string_equals_literal(Z_STR_P(type), "content")) {
        zval *messages, *m;
        size_t n, i = 0;

        k4p_outcome_init(out, K4P_OUTCOME_CONTENT);
        if (k4p_check_keys(ht, keys_content, "", e) != SUCCESS) {
            return FAILURE;
        }
        messages = k4p_find(ht, "messages");
        if (!messages || Z_TYPE_P(messages) != IS_ARRAY ||
            !zend_array_is_list(Z_ARRVAL_P(messages))) {
            return k4p_fail(e, "messages must be a list of message arrays");
        }
        n = zend_hash_num_elements(Z_ARRVAL_P(messages));
        if (n) {
            out->messages = calloc(n, sizeof(k4p_outbound));
            if (!out->messages) {
                return k4p_fail(e, "out of memory");
            }
            for (i = 0; i < n; i++) {
                out->messages[i].topic = k4p_buf_null();
                out->messages[i].key = k4p_buf_null();
                out->messages[i].value = k4p_buf_null();
            }
            out->messages_len = n;
        }
        i = 0;
        ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(messages), m) {
            if (k4p_convert_outbound(m, i, &out->messages[i], e) != SUCCESS) {
                return FAILURE;
            }
            i++;
        } ZEND_HASH_FOREACH_END();
        return SUCCESS;
    }

    if (zend_string_equals_literal(Z_STR_P(type), "fail")) {
        k4p_outcome_init(out, K4P_OUTCOME_FAIL);
        if (k4p_check_keys(ht, keys_fail, "", e) != SUCCESS ||
            k4p_req_string(k4p_find(ht, "reason"), "", "reason", &out->text, e) != SUCCESS ||
            k4p_nullable_string(k4p_find(ht, "error_class"), 1, "", "error_class",
                                &out->class_name, e) != SUCCESS) {
            return FAILURE;
        }
        return SUCCESS;
    }

    if (zend_string_equals_literal(Z_STR_P(type), "retry_later")) {
        k4p_outcome_init(out, K4P_OUTCOME_RETRY_LATER);
        if (k4p_check_keys(ht, keys_retry, "", e) != SUCCESS ||
            k4p_req_string(k4p_find(ht, "reason"), "", "reason", &out->text, e) != SUCCESS) {
            return FAILURE;
        }
        return SUCCESS;
    }

    return k4p_fail(e, "type must be one of \"no_content\", \"content\", \"fail\", "
                       "\"retry_later\"");
}

static void k4p_result_clear(void)
{
    if (k4p_ts.result_state == K4P_RESULT_VALID) {
        k4p_outcome_free(&k4p_ts.result);
    }
    k4p_ts.result_state = K4P_RESULT_NONE;
    k4p_ts.invalid[0] = '\0';
    k4p_ts.invalid_handle = 0;
}

static ZEND_FUNCTION(k4p_native_result)
{
    HashTable *ht;
    k4p_outcome res;
    char errbuf[K4P_ERR_MAX];
    k4p_err e = {errbuf, sizeof(errbuf)};

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_ARRAY_HT(ht)
    ZEND_PARSE_PARAMETERS_END();

    if (!k4p_ts.accepting_result) {
        zend_throw_exception(spl_ce_LogicException,
                             k4p_ts.msg ? "Kafka4php\\Native\\result() called after the "
                                          "request's outcome was determined"
                                        : "Kafka4php\\Native\\result() called outside of a "
                                          "kafka4php request",
                             0);
        RETURN_THROWS();
    }

    k4p_outcome_init(&res, K4P_OUTCOME_NO_CONTENT);
    errbuf[0] = '\0';
    if (k4p_convert_result(ht, &res, &e) != SUCCESS) {
        k4p_outcome_free(&res);
        k4p_result_clear();
        k4p_ts.result_state = K4P_RESULT_INVALID;
        snprintf(k4p_ts.invalid, sizeof(k4p_ts.invalid), "%s", errbuf);
        zend_argument_value_error(1, "is not a valid result: %s", errbuf);
        if (EG(exception)) {
            k4p_ts.invalid_handle = EG(exception)->handle;
        }
        RETURN_THROWS();
    }

    k4p_result_clear();
    k4p_ts.result = res;
    k4p_ts.result_state = K4P_RESULT_VALID;
}

static const zend_function_entry k4p_native_functions[] = {
    ZEND_NS_RAW_FENTRY("Kafka4php\\Native", "message", ZEND_FN(k4p_native_message),
                       arginfo_k4p_message, 0)
    ZEND_NS_RAW_FENTRY("Kafka4php\\Native", "result", ZEND_FN(k4p_native_result),
                       arginfo_k4p_result, 0)
    ZEND_FE_END
};

static zend_module_entry k4p_native_module = {
    STANDARD_MODULE_HEADER,
    "kafka4php",
    k4p_native_functions,
    NULL, /* MINIT */
    NULL, /* MSHUTDOWN */
    NULL, /* RINIT */
    NULL, /* RSHUTDOWN */
    NULL, /* MINFO */
    "0.1.0",
    STANDARD_MODULE_PROPERTIES
};

/* ------------------------------------------------------------------------------ */
/* Script execution.                                                                */

static void k4p_uncaught_clear(void)
{
    if (k4p_ts.has_uncaught) {
        k4p_buf_free(&k4p_ts.uncaught_class);
        k4p_buf_free(&k4p_ts.uncaught_message);
    }
    k4p_ts.has_uncaught = 0;
    k4p_ts.uncaught_handle = 0;
}

/* Records class + message of an exception about to be reported as uncaught. exit()
 * (unwind/graceful exit) is not an error; ParseError/CompileError are reported by
 * PHP as E_PARSE/E_COMPILE_ERROR and so classified as fatal errors. */
static void k4p_capture_uncaught(zend_object *ex)
{
    zval rv, *message;
    zend_class_entry *base;

    if (zend_is_unwind_exit(ex) || zend_is_graceful_exit(ex) ||
        instanceof_function(ex->ce, zend_ce_compile_error)) {
        return;
    }
    k4p_uncaught_clear();
    base = zend_get_exception_base(ex);
    message = zend_read_property_ex(base, ex, ZSTR_KNOWN(ZEND_STR_MESSAGE), 1, &rv);
    ZVAL_DEREF(message);
    k4p_ts.uncaught_class = k4p_buf_copy(ZSTR_VAL(ex->ce->name), ZSTR_LEN(ex->ce->name));
    if (message && Z_TYPE_P(message) == IS_STRING) {
        k4p_ts.uncaught_message = k4p_buf_copy(Z_STRVAL_P(message), Z_STRLEN_P(message));
    } else {
        k4p_ts.uncaught_message = k4p_buf_copy("", 0);
    }
    k4p_ts.uncaught_handle = ex->handle;
    k4p_ts.has_uncaught = 1;
}

/* Mirrors zend_execute_script(), capturing the uncaught exception (if any) before it
 * is reported. */
static int k4p_execute_file(zend_file_handle *fh)
{
    zend_op_array *op_array = zend_compile_file(fh, ZEND_REQUIRE);
    int ok = 1;

    if (fh->opened_path) {
        zend_hash_add_empty_element(&EG(included_files), fh->opened_path);
    }
    if (!op_array) {
        return 0;
    }
    zend_execute(op_array, NULL);
    zend_exception_restore();
    if (UNEXPECTED(EG(exception))) {
        if (Z_TYPE(EG(user_exception_handler)) != IS_UNDEF) {
            zend_user_exception_handler();
        }
        if (EG(exception)) {
            k4p_capture_uncaught(EG(exception));
            ok = zend_exception_error(EG(exception), E_ERROR) == SUCCESS;
        }
    }
    zend_destroy_static_vars(op_array);
    destroy_op_array(op_array);
    efree_size(op_array, sizeof(zend_op_array));
    return ok;
}

/* Mirrors php_execute_script_ex() (auto_prepend_file / auto_append_file, timeout),
 * minus chdir (SAPI_OPTION_NO_CHDIR, like the CLI and embed SAPIs). */
static void k4p_run_script(void)
{
    zend_file_handle primary, prepend, append;
    int has_prepend = PG(auto_prepend_file) && PG(auto_prepend_file)[0];
    int has_append = PG(auto_append_file) && PG(auto_append_file)[0];

    zend_stream_init_filename(&primary, k4p_rt.entrypoint);
    if (has_prepend) {
        zend_stream_init_filename(&prepend, PG(auto_prepend_file));
    }
    if (has_append) {
        zend_stream_init_filename(&append, PG(auto_append_file));
    }

    zend_try {
        int ok = 1;
        PG(during_request_startup) = 0;
        if (PG(max_input_time) != -1) {
            zend_set_timeout(INI_INT("max_execution_time"), 0);
        }
        if (has_prepend) {
            ok = k4p_execute_file(&prepend);
        }
        if (ok) {
            ok = k4p_execute_file(&primary);
        }
        if (ok && has_append) {
            k4p_execute_file(&append);
        }
    } zend_catch {
        /* bailout: fatal error, timeout, ... — classified from PHP's error state */
    } zend_end_try();

    if (has_prepend) {
        zend_destroy_file_handle(&prepend);
    }
    if (has_append) {
        zend_destroy_file_handle(&append);
    }
    zend_destroy_file_handle(&primary);

    if (EG(exception)) {
        k4p_capture_uncaught(EG(exception));
        zend_try {
            zend_exception_error(EG(exception), E_ERROR);
        } zend_end_try();
    }
}

/* Register_shutdown_function() callbacks are part of the script: run them now (as
 * php_request_shutdown() would, first thing) so that what they do counts. */
static void k4p_run_shutdown_functions(void)
{
    EG(current_execute_data) = NULL; /* may point into a frame after a bailout */
    if (ZEND_OBSERVER_ENABLED) {
        zend_observer_fcall_end_all();
    }
    if (PG(modules_activated)) {
        php_call_shutdown_functions();
        zend_try {
            php_free_shutdown_functions();
        } zend_end_try();
    }
}

static void k4p_text_from_zstr(k4p_buf *b, zend_string *s)
{
    *b = s ? k4p_buf_copy(ZSTR_VAL(s), ZSTR_LEN(s)) : k4p_buf_copy("", 0);
}

/* DESIGN.md §5.3. */
static void k4p_classify(k4p_outcome *out)
{
    if (PG(connection_status) & PHP_CONNECTION_TIMEOUT) {
        k4p_outcome_init(out, K4P_OUTCOME_SOFT_TIMEOUT);
        return;
    }

    if (k4p_ts.has_uncaught) {
        if (k4p_ts.result_state == K4P_RESULT_INVALID &&
            k4p_ts.uncaught_handle == k4p_ts.invalid_handle) {
            /* the \ValueError thrown by result() itself */
            k4p_outcome_init(out, K4P_OUTCOME_INVALID_RESULT);
            out->text = k4p_buf_cstr(k4p_ts.invalid);
            return;
        }
        k4p_outcome_init(out, K4P_OUTCOME_UNCAUGHT_EXCEPTION);
        out->class_name = k4p_ts.uncaught_class;
        out->text = k4p_ts.uncaught_message;
        k4p_ts.uncaught_class = k4p_buf_null();
        k4p_ts.uncaught_message = k4p_buf_null();
        k4p_ts.has_uncaught = 0;
        return;
    }

    if ((PG(last_error_type) & E_FATAL_ERRORS) && PG(last_error_message)) {
        smart_str s = {0};
        smart_str_append(&s, PG(last_error_message));
        if (PG(last_error_file)) {
            smart_str_appends(&s, " in ");
            smart_str_append(&s, PG(last_error_file));
            smart_str_appends(&s, " on line ");
            smart_str_append_long(&s, (zend_long)PG(last_error_lineno));
        }
        smart_str_0(&s);
        k4p_outcome_init(out, K4P_OUTCOME_FATAL_ERROR);
        k4p_text_from_zstr(&out->text, s.s);
        smart_str_free(&s);
        return;
    }

    if (k4p_ts.result_state == K4P_RESULT_VALID) {
        *out = k4p_ts.result; /* ownership moves to the caller */
        k4p_outcome_init(&k4p_ts.result, K4P_OUTCOME_NO_CONTENT);
        k4p_ts.result_state = K4P_RESULT_NONE;
        return;
    }

    if (k4p_ts.result_state == K4P_RESULT_INVALID) {
        k4p_outcome_init(out, K4P_OUTCOME_INVALID_RESULT);
        out->text = k4p_buf_cstr(k4p_ts.invalid);
        return;
    }

    k4p_outcome_init(out, K4P_OUTCOME_NO_RESULT);
}

static void k4p_request_state_reset(void)
{
    k4p_result_clear();
    k4p_uncaught_clear();
    k4p_ts.accepting_result = 0;
    k4p_ts.msg = NULL;
}

void k4p_execute(const k4p_message *msg, k4p_outcome *out)
{
    if (!out) {
        return;
    }
    if (!msg) {
        k4p_outcome_bootstrap(out, "k4p_execute: message is NULL");
        return;
    }
    if (!k4p_rt.initialized) {
        k4p_outcome_bootstrap(out, "PHP runtime is not initialized");
        return;
    }
    if (!k4p_ts.is_runtime_thread && !k4p_ts.thread_inited) {
        k4p_outcome_bootstrap(out, "k4p_thread_init() was not called on this thread");
        return;
    }
    if (k4p_ts.broken) {
        k4p_outcome_bootstrap(out, "PHP thread is unusable after a failed request startup");
        return;
    }
    if (k4p_ts.msg) {
        k4p_outcome_bootstrap(out, "k4p_execute: re-entrant call");
        return;
    }

    k4p_request_state_reset();
    k4p_ts.msg = msg;

    /* Request info: no HTTP request, no headers, no chdir. */
    SG(server_context) = (void *)&k4p_ts;
    SG(options) |= SAPI_OPTION_NO_CHDIR;
    SG(request_info).request_method = NULL;
    SG(request_info).query_string = NULL;
    SG(request_info).request_uri = NULL;
    SG(request_info).content_type = NULL;
    SG(request_info).content_length = 0;
    SG(request_info).path_translated = k4p_rt.entrypoint;
    SG(request_info).argc = 0;
    SG(request_info).argv = NULL;
    SG(headers_sent) = 0;
    SG(sapi_headers).http_response_code = 200;

    if (php_request_startup() == FAILURE) {
        /* Like FPM and FrankenPHP: a failed request startup leaves the thread in an
         * unknown state. Clean up what we can and refuse further work. */
        k4p_ts.broken = 1;
        zend_try {
            php_request_shutdown(NULL);
        } zend_end_try();
        k4p_line_flush();
        SG(server_context) = NULL;
        SG(request_info).path_translated = NULL;
        k4p_request_state_reset();
        k4p_outcome_bootstrap(out, "php_request_startup() failed");
        return;
    }

    /* sapi_activate() resets it: no headers, so header() after output doesn't warn. */
    SG(request_info).no_headers = 1;

    k4p_ts.accepting_result = 1;
    k4p_run_script();
    k4p_run_shutdown_functions();
    k4p_ts.accepting_result = 0;

    k4p_classify(out);

    php_request_shutdown(NULL);
    k4p_line_flush();

    SG(server_context) = NULL;
    SG(request_info).path_translated = NULL;
    k4p_request_state_reset();
}

/* ------------------------------------------------------------------------------ */
/* Runtime / thread lifecycle.                                                      */

static void k4p_rt_free(void)
{
    free(k4p_rt.ini_file);
    free(k4p_rt.ini_entries);
    free(k4p_rt.entrypoint);
    k4p_rt.ini_file = NULL;
    k4p_rt.ini_entries = NULL;
    k4p_rt.entrypoint = NULL;
}

int k4p_runtime_init(const k4p_runtime_config *config)
{
    if (!config || !config->entrypoint || !config->entrypoint[0]) {
        return K4P_ERR_INVALID_ARGUMENT;
    }
    if (k4p_rt.initialized || k4p_rt.shut_down) {
        return K4P_ERR_STATE;
    }

    k4p_rt.ini_file = k4p_strdup(config->ini_file);
    k4p_rt.ini_entries = k4p_strdup(config->ini_entries);
    k4p_rt.entrypoint = k4p_strdup(config->entrypoint);
    k4p_rt.log = config->log;
    k4p_rt.log_user_data = config->log_user_data;

    php_tsrm_startup();
    ZEND_TSRMLS_CACHE_UPDATE();
    zend_signal_startup();

    sapi_startup(&k4p_sapi_module);
    /* After sapi_startup(), which resets ini_entries. */
    k4p_sapi_module.ini_defaults = k4p_sapi_ini_defaults;
    k4p_sapi_module.php_ini_path_override = k4p_rt.ini_file;
    k4p_sapi_module.php_ini_ignore_cwd = 1;
    k4p_sapi_module.phpinfo_as_text = 1;
    k4p_sapi_module.ini_entries = k4p_rt.ini_entries;

    k4p_ts.is_runtime_thread = 1;

    if (k4p_sapi_module.startup(&k4p_sapi_module) == FAILURE) {
        k4p_logf(K4P_LOG_ERROR, "PHP module startup failed");
        sapi_shutdown();
        tsrm_shutdown();
        k4p_line_release();
        k4p_rt_free();
        k4p_ts.is_runtime_thread = 0;
        k4p_rt.shut_down = 1;
        return K4P_ERR_STARTUP;
    }
    SG(options) |= SAPI_OPTION_NO_CHDIR;
    k4p_line_flush();

    k4p_rt.initialized = 1;
    return K4P_OK;
}

int k4p_runtime_shutdown(void)
{
    if (!k4p_rt.initialized || !k4p_ts.is_runtime_thread) {
        return K4P_ERR_STATE;
    }
    k4p_sapi_module.shutdown(&k4p_sapi_module);
    sapi_shutdown();
    tsrm_shutdown();
    k4p_line_release();
    k4p_rt.initialized = 0;
    k4p_rt.shut_down = 1;
    k4p_ts.is_runtime_thread = 0;
    k4p_rt_free();
    k4p_sapi_module.ini_entries = NULL;
    k4p_sapi_module.php_ini_path_override = NULL;
    return K4P_OK;
}

int k4p_thread_init(void)
{
    if (!k4p_rt.initialized) {
        return K4P_ERR_STATE;
    }
    if (k4p_ts.is_runtime_thread || k4p_ts.thread_inited) {
        return K4P_OK;
    }
    (void)ts_resource(0);
    ZEND_TSRMLS_CACHE_UPDATE();
    k4p_ts.thread_inited = 1;
    k4p_ts.broken = 0;
    return K4P_OK;
}

int k4p_thread_shutdown(void)
{
    if (k4p_ts.is_runtime_thread || !k4p_ts.thread_inited) {
        return K4P_OK;
    }
    if (k4p_ts.msg) {
        return K4P_ERR_STATE;
    }
    ts_free_thread();
    k4p_line_release();
    k4p_ts.thread_inited = 0;
    return K4P_OK;
}
