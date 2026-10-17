/*
 * kafka4php SAPI (DESIGN.md §5.1-5.3): the "kafka4php" SAPI module, the native module
 * with Kafka4php\Native\message() / result(), request execution and outcome
 * classification, and the runtime / thread lifecycle of the C ABI (k4p_sapi.h).
 *
 * References: PHP's sapi/embed/php_embed.c, main/main.c (php_execute_script_ex) and
 * Zend/zend.c (zend_execute_script); FrankenPHP's frankenphp.c (MIT).
 *
 * Built with ZEND_ENABLE_STATIC_TSRMLS_CACHE (see build.rs): every thread that touches
 * PHP globals runs ZEND_TSRMLS_CACHE_UPDATE() after getting its interpreter globals.
 */
#include <pthread.h>
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

#include "k4p_internal.h"

#ifndef ZTS
#error "kafka4php requires a ZTS build of PHP"
#endif

ZEND_TSRMLS_CACHE_DEFINE()

#define K4P_ERR_MAX 512

/* ------------------------------------------------------------------------------ */
/* Process-wide runtime state. Written only by k4p_runtime_init/shutdown.          */

static struct {
    int initialized;
    int shut_down;
    pthread_t thread; /* the runtime thread, while initialized */
    char *ini_file;
    char *ini_entries;
    char *entrypoint;
} k4p_rt;

static int k4p_on_runtime_thread(void)
{
    return k4p_rt.initialized && pthread_equal(k4p_rt.thread, pthread_self());
}

/* ------------------------------------------------------------------------------ */
/* Per-thread state. Plain C thread-local storage: no zval or other request memory  */
/* is ever stored here, only malloc()ed copies.                                     */

typedef enum k4p_phase {
    K4P_PHASE_NONE = 0,  /* no interpreter globals on this thread */
    K4P_PHASE_IDLE,      /* ready to execute a message */
    K4P_PHASE_SCRIPT,    /* entrypoint + shutdown functions running: result() accepted */
    K4P_PHASE_FINISHING, /* outcome determined; request shutting down */
    K4P_PHASE_BROKEN     /* php_request_startup() failed: the thread is unusable */
} k4p_phase;

typedef struct k4p_thread_state {
    k4p_phase phase;
    const k4p_message *msg; /* during SCRIPT and FINISHING */

    /* What the request produced, each resolved when it happened:
     *   result:   NO_RESULT until result() is called; then the converted result, or
     *             INVALID_RESULT if the array was malformed.
     *   uncaught: NO_RESULT unless an exception went uncaught; then
     *             UNCAUGHT_EXCEPTION, or INVALID_RESULT if it was the \ValueError
     *             result() threw. */
    k4p_outcome result;
    k4p_outcome uncaught;
    uint32_t value_error_handle; /* the \ValueError result() threw; 0 = none */
} k4p_thread_state;

static __thread k4p_thread_state k4p_ts;

static void k4p_request_state_reset(void)
{
    k4p_outcome_free(&k4p_ts.result);
    k4p_outcome_free(&k4p_ts.uncaught);
    k4p_outcome_init(&k4p_ts.result, K4P_OUTCOME_NO_RESULT);
    k4p_outcome_init(&k4p_ts.uncaught, K4P_OUTCOME_NO_RESULT);
    k4p_ts.value_error_handle = 0;
    k4p_ts.msg = NULL;
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

/* Every pcntl function of PHP 8.4 and 8.5: signals, fork and exec belong to the host
 * (DESIGN.md §5.6). Disabling a function that isn't installed is a no-op. */
#define K4P_DISABLED_PCNTL                                                           \
    "pcntl_alarm,pcntl_async_signals,pcntl_errno,pcntl_exec,pcntl_fork,pcntl_forkx,"  \
    "pcntl_get_last_error,pcntl_getcpu,pcntl_getcpuaffinity,pcntl_getpriority,"       \
    "pcntl_getqos_class,pcntl_rfork,pcntl_setcpuaffinity,pcntl_setns,"               \
    "pcntl_setpriority,pcntl_setqos_class,pcntl_signal,pcntl_signal_dispatch,"        \
    "pcntl_signal_get_handler,pcntl_sigprocmask,pcntl_sigtimedwait,"                 \
    "pcntl_sigwaitinfo,pcntl_strerror,pcntl_unshare,pcntl_wait,pcntl_waitid,"         \
    "pcntl_waitpid,pcntl_wexitstatus,pcntl_wifcontinued,pcntl_wifexited,"             \
    "pcntl_wifsignaled,pcntl_wifstopped,pcntl_wstopsig,pcntl_wtermsig"

/* The host's default INI (DESIGN.md §5.1.2): what the SAPI needs, then the defaults
 * for running handlers. */
static void k4p_sapi_ini_defaults(HashTable *configuration_hash)
{
    zval tmp;
    K4P_INI_DEFAULT("display_errors", "0");
    K4P_INI_DEFAULT("log_errors", "1");
    K4P_INI_DEFAULT("html_errors", "0");
    K4P_INI_DEFAULT("register_argc_argv", "0");
    K4P_INI_DEFAULT("output_buffering", "0");

    K4P_INI_DEFAULT("memory_limit", "128M");
    K4P_INI_DEFAULT("max_execution_time", "30");
    K4P_INI_DEFAULT("disable_functions", K4P_DISABLED_PCNTL);
    K4P_INI_DEFAULT("opcache.enable", "1");
}

static sapi_module_struct k4p_sapi_module = {
    "kafka4php",                       /* name (PHP_SAPI) */
    "kafka4php Kafka consumer host",   /* pretty name */
    k4p_sapi_startup,                  /* startup */
    php_module_shutdown_wrapper,       /* shutdown */
    NULL,                              /* activate */
    NULL,                              /* deactivate */
    k4p_output_write,                  /* unbuffered write */
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
    for (size_t i = 0; i < m->headers_len; i++) {
        zval pair;
        array_init_size(&pair, 2);
        k4p_add_next_buf(&pair, &m->headers[i].key);
        k4p_add_next_buf(&pair, &m->headers[i].value);
        add_next_index_zval(&headers, &pair);
    }
    add_assoc_zval_ex(return_value, "headers", sizeof("headers") - 1, &headers);
    add_assoc_long_ex(return_value, "attempt", sizeof("attempt") - 1, (zend_long)m->attempt);
}

static ZEND_FUNCTION(k4p_native_result)
{
    HashTable *ht;
    k4p_outcome res;
    char err[K4P_ERR_MAX];

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_ARRAY_HT(ht)
    ZEND_PARSE_PARAMETERS_END();

    switch (k4p_ts.phase) {
    case K4P_PHASE_SCRIPT:
        break;
    case K4P_PHASE_FINISHING:
        zend_throw_exception(spl_ce_LogicException,
                             "Kafka4php\\Native\\result() called after the request's outcome "
                             "was determined",
                             0);
        RETURN_THROWS();
    default:
        zend_throw_exception(spl_ce_LogicException,
                             "Kafka4php\\Native\\result() called outside of a kafka4php request",
                             0);
        RETURN_THROWS();
    }

    k4p_outcome_free(&k4p_ts.result);
    k4p_ts.value_error_handle = 0;
    if (k4p_result_from_array(ht, &res, err, sizeof(err)) == SUCCESS) {
        k4p_ts.result = res;
        return;
    }
    k4p_outcome_with_text(&k4p_ts.result, K4P_OUTCOME_INVALID_RESULT, err);
    zend_argument_value_error(1, "is not a valid result: %s", err);
    if (EG(exception)) {
        k4p_ts.value_error_handle = EG(exception)->handle;
    }
    RETURN_THROWS();
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

/* Records an exception about to be reported as uncaught. exit() (unwind/graceful
 * exit) is not an error; ParseError/CompileError are reported by PHP as
 * E_PARSE/E_COMPILE_ERROR and so classified as fatal errors. */
static void k4p_capture_uncaught(zend_object *ex)
{
    zval rv, *message;

    if (zend_is_unwind_exit(ex) || zend_is_graceful_exit(ex) ||
        instanceof_function(ex->ce, zend_ce_compile_error)) {
        return;
    }
    k4p_outcome_free(&k4p_ts.uncaught);

    if (k4p_ts.value_error_handle && ex->handle == k4p_ts.value_error_handle) {
        /* The \ValueError result() threw: the result is invalid. */
        k4p_outcome_init(&k4p_ts.uncaught, K4P_OUTCOME_INVALID_RESULT);
        k4p_ts.uncaught.text = k4p_buf_copy((const char *)k4p_ts.result.text.data,
                                            k4p_ts.result.text.len);
        return;
    }

    k4p_outcome_init(&k4p_ts.uncaught, K4P_OUTCOME_UNCAUGHT_EXCEPTION);
    k4p_ts.uncaught.class_name = k4p_buf_copy(ZSTR_VAL(ex->ce->name), ZSTR_LEN(ex->ce->name));
    message = zend_read_property_ex(zend_get_exception_base(ex), ex,
                                    ZSTR_KNOWN(ZEND_STR_MESSAGE), 1, &rv);
    ZVAL_DEREF(message);
    k4p_ts.uncaught.text = message && Z_TYPE_P(message) == IS_STRING
                               ? k4p_buf_copy(Z_STRVAL_P(message), Z_STRLEN_P(message))
                               : k4p_buf_copy("", 0);
}

/* Mirrors zend_execute_script(), capturing the uncaught exception (if any) before it
 * is reported. Re-check against Zend/zend.c on every PHP minor version. */
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
 * minus chdir (SAPI_OPTION_NO_CHDIR, like the CLI and embed SAPIs). Re-check against
 * main/main.c on every PHP minor version. */
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

static void k4p_fatal_error(k4p_outcome *out)
{
    smart_str s = {0};

    smart_str_append(&s, PG(last_error_message));
    if (PG(last_error_file)) {
        smart_str_appends(&s, " in ");
        smart_str_append(&s, PG(last_error_file));
        smart_str_appends(&s, " on line ");
        smart_str_append_long(&s, (zend_long)PG(last_error_lineno));
    }
    k4p_outcome_init(out, K4P_OUTCOME_FATAL_ERROR);
    out->text = s.s ? k4p_buf_copy(ZSTR_VAL(s.s), ZSTR_LEN(s.s)) : k4p_buf_copy("", 0);
    smart_str_free(&s);
}

/* DESIGN.md §5.3. An uncaught exception is also reported as a fatal error, so it is
 * checked first. */
static void k4p_classify(k4p_outcome *out)
{
    if (PG(connection_status) & PHP_CONNECTION_TIMEOUT) {
        k4p_outcome_init(out, K4P_OUTCOME_SOFT_TIMEOUT);
    } else if (k4p_ts.uncaught.kind != K4P_OUTCOME_NO_RESULT) {
        k4p_outcome_move(out, &k4p_ts.uncaught);
    } else if ((PG(last_error_type) & E_FATAL_ERRORS) && PG(last_error_message)) {
        k4p_fatal_error(out);
    } else {
        k4p_outcome_move(out, &k4p_ts.result); /* NO_RESULT if result() was never called */
    }
}

static void k4p_request_begin(const k4p_message *msg)
{
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
}

/* After php_request_shutdown(), on every path. */
static void k4p_request_end(k4p_phase next)
{
    k4p_output_flush();
    SG(server_context) = NULL;
    SG(request_info).path_translated = NULL;
    k4p_request_state_reset();
    k4p_ts.phase = next;
}

void k4p_execute(const k4p_message *msg, k4p_outcome *out)
{
    if (!out) {
        return;
    }
    if (!msg) {
        k4p_outcome_with_text(out, K4P_OUTCOME_BOOTSTRAP, "k4p_execute: message is NULL");
        return;
    }
    switch (k4p_ts.phase) {
    case K4P_PHASE_IDLE:
        break;
    case K4P_PHASE_NONE:
        k4p_outcome_with_text(out, K4P_OUTCOME_BOOTSTRAP,
                              k4p_rt.initialized
                                  ? "k4p_thread_init() was not called on this thread"
                                  : "PHP runtime is not initialized");
        return;
    case K4P_PHASE_BROKEN:
        k4p_outcome_with_text(out, K4P_OUTCOME_BOOTSTRAP,
                              "PHP thread is unusable after a failed request startup");
        return;
    default:
        k4p_outcome_with_text(out, K4P_OUTCOME_BOOTSTRAP, "k4p_execute: re-entrant call");
        return;
    }

    k4p_request_begin(msg);
    if (php_request_startup() == FAILURE) {
        /* Like FPM and FrankenPHP: a failed request startup leaves the thread in an
         * unknown state. Clean up what we can and refuse further work. */
        zend_try {
            php_request_shutdown(NULL);
        } zend_end_try();
        k4p_request_end(K4P_PHASE_BROKEN);
        k4p_outcome_with_text(out, K4P_OUTCOME_BOOTSTRAP, "php_request_startup() failed");
        return;
    }
    /* sapi_activate() resets it: no headers, so header() after output doesn't warn. */
    SG(request_info).no_headers = 1;

    k4p_ts.phase = K4P_PHASE_SCRIPT;
    k4p_run_script();
    k4p_run_shutdown_functions();
    k4p_ts.phase = K4P_PHASE_FINISHING;

    k4p_classify(out);
    php_request_shutdown(NULL);
    k4p_request_end(K4P_PHASE_IDLE);
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
    k4p_log_init(config->log, config->log_user_data);

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

    if (k4p_sapi_module.startup(&k4p_sapi_module) == FAILURE) {
        k4p_logf(K4P_LOG_ERROR, "PHP module startup failed");
        sapi_shutdown();
        tsrm_shutdown();
        k4p_output_release();
        k4p_rt_free();
        k4p_rt.shut_down = 1;
        return K4P_ERR_STARTUP;
    }
    SG(options) |= SAPI_OPTION_NO_CHDIR;
    k4p_output_flush();

    k4p_rt.thread = pthread_self();
    k4p_rt.initialized = 1;
    k4p_ts.phase = K4P_PHASE_IDLE; /* the runtime thread has its interpreter globals */
    return K4P_OK;
}

int k4p_runtime_shutdown(void)
{
    if (!k4p_on_runtime_thread()) {
        return K4P_ERR_STATE;
    }
    k4p_sapi_module.shutdown(&k4p_sapi_module);
    sapi_shutdown();
    tsrm_shutdown();
    k4p_output_release();
    k4p_ts.phase = K4P_PHASE_NONE;
    k4p_rt.initialized = 0;
    k4p_rt.shut_down = 1;
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
    if (k4p_ts.phase != K4P_PHASE_NONE) {
        return K4P_OK; /* idempotent; the runtime thread is always initialized */
    }
    (void)ts_resource(0);
    ZEND_TSRMLS_CACHE_UPDATE();
    k4p_ts.phase = K4P_PHASE_IDLE;
    return K4P_OK;
}

int k4p_thread_shutdown(void)
{
    switch (k4p_ts.phase) {
    case K4P_PHASE_NONE:
        return K4P_OK;
    case K4P_PHASE_SCRIPT:
    case K4P_PHASE_FINISHING:
        return K4P_ERR_STATE;
    default:
        break;
    }
    if (k4p_on_runtime_thread()) {
        return K4P_OK; /* its globals go with k4p_runtime_shutdown() */
    }
    ts_free_thread();
    k4p_output_release();
    k4p_ts.phase = K4P_PHASE_NONE;
    return K4P_OK;
}
