<?php

declare(strict_types=1);

namespace Bench\Framework\Result;

final class RetryLater implements Result
{
    public function __construct(public readonly string $reason) {}

    public function toNative(): array
    {
        return ['type' => 'retry_later', 'reason' => $this->reason];
    }
}
