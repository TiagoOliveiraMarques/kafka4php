<?php

// Stubs of the native functions registered by the kafka4php SAPI, for IDEs and static
// analysis. Never load this file at runtime: inside kafka4php the real functions exist.

namespace Kafka4php\Native;

/**
 * The message of the current request; builds a fresh array on each call. `topic` is
 * the logical topic, `delivery_topic` the one it was consumed from (a retry topic, or
 * the same); `timestamp` is unix ms; `key`, `value` and header values are binary-safe;
 * `headers` are ordered pairs and may repeat a name.
 *
 * @return array{
 *     topic: string,
 *     delivery_topic: string,
 *     partition: int,
 *     offset: int,
 *     timestamp: ?int,
 *     key: ?string,
 *     value: ?string,
 *     headers: list<array{0: string, 1: ?string}>,
 *     attempt: int,
 * }
 *
 * @throws \LogicException outside a kafka4php request.
 */
function message(): array
{
    throw new \LogicException('stub');
}

/**
 * Set the result of the current request; a later call replaces it. Accepted shapes:
 *
 * - `['type' => 'no_content']`
 * - `['type' => 'content', 'messages' => list<['topic' => string, 'value' => ?string,
 *   'key' => ?string, 'headers' => list<[string, ?string]>, 'partition' => ?int]>]`
 *   (`key`, `headers` and `partition` are optional)
 * - `['type' => 'fail', 'reason' => string, 'error_class' => ?string]`
 *   (`error_class` is optional)
 * - `['type' => 'retry_later', 'reason' => string]`
 *
 * @param array<string, mixed> $result
 *
 * @throws \ValueError if `$result` is not one of the accepted shapes.
 * @throws \LogicException outside a kafka4php request, or once the outcome was determined.
 */
function result(array $result): void
{
}
