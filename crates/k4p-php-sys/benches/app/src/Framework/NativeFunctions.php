<?php

declare(strict_types=1);

namespace Bench\Framework;

final class NativeFunctions implements NativeBridge
{
    public function message(): array
    {
        return \Kafka4php\Native\message();
    }

    public function result(array $result): void
    {
        \Kafka4php\Native\result($result);
    }
}
