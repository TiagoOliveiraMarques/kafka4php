<?php

declare(strict_types=1);

namespace Kafka4php\Result;

use Kafka4php\NativeBridge;

/**
 * What a handler decided: NoContent, Content, Fail or RetryLater. Not meant to be
 * implemented outside this package.
 *
 * @phpstan-import-type NativeResult from NativeBridge
 */
interface Result
{
    /**
     * @internal the array the native `result()` takes
     *
     * @return NativeResult
     */
    public function toNative(): array;
}
