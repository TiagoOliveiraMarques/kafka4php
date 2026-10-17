/*
 * Declarations shared by the kafka4php SAPI's C files. Not part of the C ABI
 * (k4p_sapi.h is); plain C, no PHP headers.
 *
 *   k4p_buf.c     malloc()ed buffers and outcomes, UTF-8 validation
 *   k4p_output.c  the log sink and script output (ub_write) -> log callback, per line
 *   k4p_result.c  Kafka4php\Native\result() arrays -> k4p_outcome (DESIGN.md §5.2)
 *   k4p_sapi.c    SAPI + native module, request execution, classification, lifecycle
 */
#ifndef K4P_INTERNAL_H
#define K4P_INTERNAL_H

#include <stddef.h>
#include <stdint.h>

#include "k4p_sapi.h"

/* --- k4p_buf.c ---------------------------------------------------------------- */

k4p_buf k4p_buf_null(void);
/* malloc()ed copy of `len` bytes; aborts on OOM. Never a NULL `data` (an empty buffer
 * still gets a 1-byte allocation) so Rust can build slices safely. */
k4p_buf k4p_buf_copy(const char *data, size_t len);
k4p_buf k4p_buf_cstr(const char *s);
void k4p_buf_free(k4p_buf *b);

/* Resets `out` (without freeing) to `kind` with no data. */
void k4p_outcome_init(k4p_outcome *out, k4p_outcome_kind kind);
/* `kind` with `text` copied from `text`. */
void k4p_outcome_with_text(k4p_outcome *out, k4p_outcome_kind kind, const char *text);
/* Moves `*from` into `*to` (which must hold nothing) and leaves `*from` as NO_RESULT. */
void k4p_outcome_move(k4p_outcome *to, k4p_outcome *from);

/* Strict UTF-8 validation (no overlongs, no surrogates, max U+10FFFF). */
int k4p_utf8_valid(const unsigned char *s, size_t len);

char *k4p_strdup(const char *s);

/* --- k4p_output.c ------------------------------------------------------------- */

void k4p_log_init(k4p_log_fn log, void *user_data);
void k4p_log_raw(k4p_log_level level, const char *source, const char *msg, size_t len);
void k4p_logf(k4p_log_level level, const char *fmt, ...);

/* SAPI ub_write: buffers script output and logs it ("stdout", INFO) line by line. */
size_t k4p_output_write(const char *str, size_t len);
/* Logs a pending partial line (end of request). */
void k4p_output_flush(void);
/* Flushes and frees this thread's line buffer (thread/runtime shutdown). */
void k4p_output_release(void);

/* --- k4p_result.c ------------------------------------------------------------- */

struct _zend_array;

/* Validates a result() array (DESIGN.md §5.2) and converts it into `*out`, which must
 * hold nothing. On failure returns non-zero, leaves `*out` holding nothing and writes
 * what was wrong into `err`. */
int k4p_result_from_array(struct _zend_array *ht, k4p_outcome *out, char *err, size_t err_cap);

#endif /* K4P_INTERNAL_H */
