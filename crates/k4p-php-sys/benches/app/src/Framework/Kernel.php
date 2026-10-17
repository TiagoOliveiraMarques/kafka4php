<?php

declare(strict_types=1);

namespace Bench\Framework;

use Bench\Framework\Result\Result;
use Bench\Framework\Result\RetryLater;

final class Kernel
{
    /** @param Handler|callable(Message): Result $handler */
    public static function run(Handler|callable $handler, ?NativeBridge $bridge = null): void
    {
        $bridge ??= new NativeFunctions();
        $message = Message::fromNative($bridge->message());
        try {
            $result = $handler instanceof Handler ? $handler->handle($message) : $handler($message);
        } catch (\Throwable $e) {
            $result = new RetryLater($e::class . ': ' . $e->getMessage());
        }
        $bridge->result($result->toNative());
    }
}
