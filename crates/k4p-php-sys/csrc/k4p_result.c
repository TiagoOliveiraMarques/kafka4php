/*
 * Kafka4php\Native\result() arrays -> k4p_outcome (DESIGN.md §5.2). Validates
 * strictly (unknown keys rejected) and copies everything into malloc()ed memory, so
 * the result survives the PHP request. The only C code that knows the result shapes.
 */
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "php.h"

#include "k4p_internal.h"

ZEND_TSRMLS_CACHE_EXTERN()

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

/* NULL when missing. */
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
        int ok = 0;
        for (const char *const *a = allowed; key && *a; a++) {
            if (zend_string_equals_cstr(key, *a, strlen(*a))) {
                ok = 1;
                break;
            }
        }
        if (!ok) {
            return key ? k4p_fail(e, "%sunexpected key \"%s\"", path, ZSTR_VAL(key))
                       : k4p_fail(e, "%sunexpected key " ZEND_ULONG_FMT, path, idx);
        }
    } ZEND_HASH_FOREACH_END();
    return SUCCESS;
}

/* --- Field readers. `zv` is NULL when the key is missing. -------------------------- */

static int k4p_string(zval *zv, const char *path, const char *name, k4p_buf *out, k4p_err *e)
{
    if (!zv || Z_TYPE_P(zv) != IS_STRING) {
        return k4p_fail(e, "%s%s must be a string", path, name);
    }
    *out = k4p_buf_copy(Z_STRVAL_P(zv), Z_STRLEN_P(zv));
    return SUCCESS;
}

/* Key required, value string or null. */
static int k4p_string_or_null(zval *zv, const char *path, const char *name, k4p_buf *out,
                              k4p_err *e)
{
    if (!zv) {
        return k4p_fail(e, "%s%s is required (string or null)", path, name);
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

/* Key optional (missing = null), value string or null. */
static int k4p_optional_string_or_null(zval *zv, const char *path, const char *name,
                                       k4p_buf *out, k4p_err *e)
{
    if (!zv) {
        *out = k4p_buf_null();
        return SUCCESS;
    }
    return k4p_string_or_null(zv, path, name, out, e);
}

static int k4p_is_utf8_string(zval *zv)
{
    return Z_TYPE_P(zv) == IS_STRING &&
           k4p_utf8_valid((const unsigned char *)Z_STRVAL_P(zv), Z_STRLEN_P(zv));
}

/* list<array{0: string, 1: ?string}>; missing = []. */
static int k4p_headers(zval *zv, const char *path, k4p_header **out, size_t *out_len, k4p_err *e)
{
    HashTable *ht;
    zval *pair;
    size_t n, i = 0;

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
        if (!k4p_is_utf8_string(name)) {
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

/* --- Result shapes ------------------------------------------------------------- */

static int k4p_outbound_message(zval *zv, size_t idx, k4p_outbound *m, k4p_err *e)
{
    static const char *const keys[] = {"topic", "value", "key", "headers", "partition", NULL};
    char path[64];
    HashTable *ht;
    zval *topic, *partition;

    snprintf(path, sizeof(path), "messages[%zu].", idx);
    ZVAL_DEREF(zv);
    if (Z_TYPE_P(zv) != IS_ARRAY) {
        return k4p_fail(e, "messages[%zu] must be an array", idx);
    }
    ht = Z_ARRVAL_P(zv);
    if (k4p_check_keys(ht, keys, path, e) != SUCCESS) {
        return FAILURE;
    }

    topic = k4p_find(ht, "topic");
    if (!topic || !k4p_is_utf8_string(topic) || Z_STRLEN_P(topic) == 0) {
        return k4p_fail(e, "%stopic must be a non-empty valid UTF-8 string", path);
    }
    m->topic = k4p_buf_copy(Z_STRVAL_P(topic), Z_STRLEN_P(topic));

    if (k4p_string_or_null(k4p_find(ht, "value"), path, "value", &m->value, e) != SUCCESS ||
        k4p_optional_string_or_null(k4p_find(ht, "key"), path, "key", &m->key, e) != SUCCESS ||
        k4p_headers(k4p_find(ht, "headers"), path, &m->headers, &m->headers_len, e) != SUCCESS) {
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

static int k4p_content_messages(HashTable *ht, k4p_outcome *out, k4p_err *e)
{
    zval *messages = k4p_find(ht, "messages"), *m;
    size_t n, i = 0;

    if (!messages || Z_TYPE_P(messages) != IS_ARRAY || !zend_array_is_list(Z_ARRVAL_P(messages))) {
        return k4p_fail(e, "messages must be a list of message arrays");
    }
    n = zend_hash_num_elements(Z_ARRVAL_P(messages));
    if (!n) {
        return SUCCESS;
    }
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

    i = 0;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(messages), m) {
        if (k4p_outbound_message(m, i, &out->messages[i], e) != SUCCESS) {
            return FAILURE;
        }
        i++;
    } ZEND_HASH_FOREACH_END();
    return SUCCESS;
}

static const struct k4p_result_type {
    const char *name;
    k4p_outcome_kind kind;
    const char *const *keys;
} k4p_result_types[] = {
    {"no_content", K4P_OUTCOME_NO_CONTENT, (const char *const[]){"type", NULL}},
    {"content", K4P_OUTCOME_CONTENT, (const char *const[]){"type", "messages", NULL}},
    {"fail", K4P_OUTCOME_FAIL, (const char *const[]){"type", "reason", "error_class", NULL}},
    {"retry_later", K4P_OUTCOME_RETRY_LATER, (const char *const[]){"type", "reason", NULL}},
};

static const struct k4p_result_type *k4p_result_type(HashTable *ht)
{
    zval *type = k4p_find(ht, "type");

    if (!type || Z_TYPE_P(type) != IS_STRING) {
        return NULL;
    }
    for (size_t i = 0; i < sizeof(k4p_result_types) / sizeof(k4p_result_types[0]); i++) {
        if (zend_string_equals_cstr(Z_STR_P(type), k4p_result_types[i].name,
                                    strlen(k4p_result_types[i].name))) {
            return &k4p_result_types[i];
        }
    }
    return NULL;
}

static int k4p_result_fields(HashTable *ht, k4p_outcome *out, k4p_err *e)
{
    const struct k4p_result_type *t = k4p_result_type(ht);

    if (!t) {
        return k4p_fail(e, "type must be one of \"no_content\", \"content\", \"fail\", "
                           "\"retry_later\"");
    }
    k4p_outcome_init(out, t->kind);
    if (k4p_check_keys(ht, t->keys, "", e) != SUCCESS) {
        return FAILURE;
    }
    switch (t->kind) {
    case K4P_OUTCOME_CONTENT:
        return k4p_content_messages(ht, out, e);
    case K4P_OUTCOME_FAIL:
        if (k4p_string(k4p_find(ht, "reason"), "", "reason", &out->text, e) != SUCCESS) {
            return FAILURE;
        }
        return k4p_optional_string_or_null(k4p_find(ht, "error_class"), "", "error_class",
                                           &out->class_name, e);
    case K4P_OUTCOME_RETRY_LATER:
        return k4p_string(k4p_find(ht, "reason"), "", "reason", &out->text, e);
    default:
        return SUCCESS;
    }
}

int k4p_result_from_array(HashTable *ht, k4p_outcome *out, char *err, size_t err_cap)
{
    k4p_err e = {err, err_cap};

    err[0] = '\0';
    k4p_outcome_init(out, K4P_OUTCOME_NO_RESULT);
    if (k4p_result_fields(ht, out, &e) != SUCCESS) {
        k4p_outcome_free(out);
        k4p_outcome_init(out, K4P_OUTCOME_NO_RESULT);
        return FAILURE;
    }
    return SUCCESS;
}
