<?php

declare(strict_types=1);

namespace Bench\Framework;

interface NativeBridge
{
    /** @return array<string, mixed> */
    public function message(): array;

    /** @param array<string, mixed> $result */
    public function result(array $result): void;
}
