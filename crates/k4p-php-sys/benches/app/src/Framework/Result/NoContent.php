<?php

declare(strict_types=1);

namespace Bench\Framework\Result;

final class NoContent implements Result
{
    public function toNative(): array
    {
        return ['type' => 'no_content'];
    }
}
