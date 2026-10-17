/*
 * kafka4php SAPI: the C ABI between the Rust host and embedded PHP (DESIGN.md §5.1-5.3).
 *
 * Plain C types only; no PHP headers. This is the only header bindgen sees.
 *
 * Threading contract:
 *   - k4p_runtime_init() once per process, before any other call. The calling thread
 *     becomes the "runtime thread"; k4p_runtime_shutdown() must run on it, after every
 *     other thread called k4p_thread_shutdown() (or is known never to run PHP again).
 *   - Every other thread that executes messages calls k4p_thread_init() first and
 *     k4p_thread_shutdown() last. The runtime thread may execute messages without
 *     k4p_thread_init() (it already has its interpreter globals).
 *   - k4p_execute() runs one PHP request on the calling thread. One at a time per
 *     thread; any number of threads concurrently.
 *   - The log callback is invoked synchronously on the thread that produced the
 *     output (during k4p_execute() on that thread, or during init/shutdown).
 */
#ifndef K4P_SAPI_H
#define K4P_SAPI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* A byte buffer. `is_null` distinguishes a null value from an empty one; when it is
 * set, `data`/`len` are NULL/0. Not NUL-terminated; may contain NUL bytes. */
typedef struct k4p_buf {
    const uint8_t *data;
    size_t len;
    uint8_t is_null;
} k4p_buf;

/* A Kafka header. `key` is never null (UTF-8); `value` may be null. */
typedef struct k4p_header {
    k4p_buf key;
    k4p_buf value;
} k4p_header;

/* InboundMessage (k4p-core). Borrowed by k4p_execute() for the duration of the call
 * only; PHP copies everything it needs. */
typedef struct k4p_message {
    k4p_buf topic;          /* logical topic, UTF-8 */
    k4p_buf delivery_topic; /* UTF-8 */
    int32_t partition;
    int64_t offset;
    uint8_t has_timestamp;
    int64_t timestamp_ms;
    k4p_buf key;
    k4p_buf value;
    const k4p_header *headers;
    size_t headers_len;
    uint32_t attempt;
} k4p_message;

/* OutboundMessage (k4p-core). Owned by the enclosing k4p_outcome. */
typedef struct k4p_outbound {
    k4p_buf topic; /* UTF-8, non-empty */
    uint8_t has_partition;
    int32_t partition; /* >= 0 when has_partition */
    k4p_buf key;
    k4p_buf value;
    k4p_header *headers;
    size_t headers_len;
} k4p_outbound;

/* Outcome kinds: HandlerResult variants, then ExecutionError variants. */
typedef enum k4p_outcome_kind {
    K4P_OUTCOME_NO_CONTENT = 0,
    K4P_OUTCOME_CONTENT = 1,      /* messages / messages_len */
    K4P_OUTCOME_FAIL = 2,         /* text = reason, class_name = error_class (nullable) */
    K4P_OUTCOME_RETRY_LATER = 3,  /* text = reason */
    K4P_OUTCOME_NO_RESULT = 10,
    K4P_OUTCOME_INVALID_RESULT = 11,     /* text = what was wrong */
    K4P_OUTCOME_UNCAUGHT_EXCEPTION = 12, /* class_name = class, text = message */
    K4P_OUTCOME_FATAL_ERROR = 13,        /* text = "<message> in <file> on line <n>" */
    K4P_OUTCOME_SOFT_TIMEOUT = 14,
    K4P_OUTCOME_BOOTSTRAP = 15           /* text = why the request could not run */
} k4p_outcome_kind;

/* Result of one k4p_execute(). All memory is malloc()ed by the C side and released by
 * k4p_outcome_free(). Fields not used by `kind` are null/0. Strings (`text`,
 * `class_name`) come from PHP and are not guaranteed to be valid UTF-8. */
typedef struct k4p_outcome {
    k4p_outcome_kind kind;
    k4p_outbound *messages;
    size_t messages_len;
    k4p_buf text;
    k4p_buf class_name;
} k4p_outcome;

/* Log levels passed to the log callback. */
typedef enum k4p_log_level {
    K4P_LOG_ERROR = 1,
    K4P_LOG_WARN = 2,
    K4P_LOG_INFO = 3
} k4p_log_level;

/* Log callback. `source` is a static NUL-terminated string:
 *   "stdout" - script output (echo, print, var_dump...), one call per line, without
 *              the line terminator; empty lines are dropped. Level INFO.
 *   "error"  - PHP's error log (warnings, notices, fatal errors, uncaught exceptions,
 *              error_log()), when the `error_log` INI setting is unset. Level ERROR
 *              for errors, WARN for everything else.
 *   "sapi"   - diagnostics of the kafka4php SAPI itself.
 * `msg` is not NUL-terminated and may be invalid UTF-8; it is only valid during the
 * call. The callback must not call back into this API. */
typedef void (*k4p_log_fn)(void *user_data, k4p_log_level level, const char *source,
                           const uint8_t *msg, size_t len);

typedef struct k4p_runtime_config {
    /* php.ini path; NULL = PHP's default search (PHP_CONFIG_FILE_PATH). The scan dir
     * (conf.d, used by docker-php-ext-install) is always read. */
    const char *ini_file;
    /* INI overrides, "key=value\n" lines, applied after all ini files. NULL = none. */
    const char *ini_entries;
    /* Entry script, executed once per message. Required. */
    const char *entrypoint;
    k4p_log_fn log;     /* NULL = discard */
    void *log_user_data;
} k4p_runtime_config;

/* Return codes. */
#define K4P_OK 0
#define K4P_ERR_INVALID_ARGUMENT 1 /* NULL config / entrypoint */
#define K4P_ERR_STATE 2            /* already initialized, or not initialized */
#define K4P_ERR_STARTUP 3          /* PHP module startup failed (details logged) */

/* Starts PHP: TSRM, SAPI, INI, module startup (incl. extensions and OPcache). All
 * strings are copied. Once per process; a runtime that was shut down cannot be
 * started again. */
int k4p_runtime_init(const k4p_runtime_config *config);

/* Shuts PHP down. Runtime thread only, after all other PHP threads are done. */
int k4p_runtime_shutdown(void);

/* Allocates this thread's interpreter globals. Idempotent. */
int k4p_thread_init(void);

/* Frees this thread's interpreter globals. No-op on the runtime thread. */
int k4p_thread_shutdown(void);

/* Runs one PHP request for `msg` on the calling thread and fills `*out` (always, also
 * on error: then kind = K4P_OUTCOME_BOOTSTRAP). Release `*out` with
 * k4p_outcome_free(). */
void k4p_execute(const k4p_message *msg, k4p_outcome *out);

/* Frees everything owned by `*out` and resets it to K4P_OUTCOME_NO_CONTENT with no
 * data. Safe to call twice. */
void k4p_outcome_free(k4p_outcome *out);

#ifdef __cplusplus
}
#endif

#endif /* K4P_SAPI_H */
