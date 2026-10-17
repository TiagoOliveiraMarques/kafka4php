/*
 * malloc()ed buffers and outcomes (the memory that crosses the C ABI), UTF-8
 * validation. No PHP.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "k4p_internal.h"

k4p_buf k4p_buf_null(void)
{
    k4p_buf b = {NULL, 0, 1};
    return b;
}

k4p_buf k4p_buf_copy(const char *data, size_t len)
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

k4p_buf k4p_buf_cstr(const char *s)
{
    return k4p_buf_copy(s, strlen(s));
}

void k4p_buf_free(k4p_buf *b)
{
    free((void *)b->data);
    *b = k4p_buf_null();
}

static void k4p_headers_free(k4p_header *h, size_t n)
{
    for (size_t i = 0; i < n; i++) {
        k4p_buf_free(&h[i].key);
        k4p_buf_free(&h[i].value);
    }
    free(h);
}

void k4p_outcome_init(k4p_outcome *out, k4p_outcome_kind kind)
{
    memset(out, 0, sizeof(*out));
    out->kind = kind;
    out->text = k4p_buf_null();
    out->class_name = k4p_buf_null();
}

void k4p_outcome_with_text(k4p_outcome *out, k4p_outcome_kind kind, const char *text)
{
    k4p_outcome_init(out, kind);
    out->text = k4p_buf_cstr(text);
}

void k4p_outcome_move(k4p_outcome *to, k4p_outcome *from)
{
    *to = *from;
    k4p_outcome_init(from, K4P_OUTCOME_NO_RESULT);
}

void k4p_outcome_free(k4p_outcome *out)
{
    if (!out) {
        return;
    }
    for (size_t i = 0; i < out->messages_len; i++) {
        k4p_outbound *m = &out->messages[i];
        k4p_buf_free(&m->topic);
        k4p_buf_free(&m->key);
        k4p_buf_free(&m->value);
        k4p_headers_free(m->headers, m->headers_len);
    }
    free(out->messages);
    k4p_buf_free(&out->text);
    k4p_buf_free(&out->class_name);
    k4p_outcome_init(out, K4P_OUTCOME_NO_CONTENT);
}

int k4p_utf8_valid(const unsigned char *s, size_t len)
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

char *k4p_strdup(const char *s)
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
