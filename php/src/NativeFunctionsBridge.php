<?php

declare(strict_types=1);

namespace Kafka4php;

/** The real bridge: calls the native functions, so it only works inside a kafka4php request. */
final class NativeFunctionsBridge implements NativeBridge
{
    public function message(): array
    {
        return Native\message();
    }

    public function result(array $result): void
    {
        Native\result($result);
    }
}
