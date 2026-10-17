<?php

declare(strict_types=1);

namespace Kafka4php;

/**
 * The boundary to the two native functions of the kafka4php SAPI
 * (`Kafka4php\Native\message()` / `result()`). The type aliases below are the array
 * shapes those functions exchange; the C side validates every result it receives.
 *
 * @phpstan-type NativeHeaders list<array{0: string, 1: ?string}>
 * @phpstan-type NativeMessage array{
 *     topic: string,
 *     delivery_topic: string,
 *     partition: int,
 *     offset: int,
 *     timestamp: ?int,
 *     key: ?string,
 *     value: ?string,
 *     headers: NativeHeaders,
 *     attempt: int,
 * }
 * @phpstan-type NativeOutbound array{
 *     topic: string,
 *     value: ?string,
 *     key: ?string,
 *     headers: NativeHeaders,
 *     partition: ?int,
 * }
 * @phpstan-type NativeResult array{type: 'no_content'}
 *     |array{type: 'content', messages: list<NativeOutbound>}
 *     |array{type: 'fail', reason: string, error_class: ?string}
 *     |array{type: 'retry_later', reason: string}
 */
interface NativeBridge
{
    /** @return NativeMessage */
    public function message(): array;

    /** @param NativeResult $result */
    public function result(array $result): void;
}
