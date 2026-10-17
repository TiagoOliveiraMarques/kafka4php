<?php

declare(strict_types=1);

namespace Bench\Framework\Result;

interface Result
{
    /** @return array<string, mixed> */
    public function toNative(): array;
}
