<?php

declare(strict_types=1);

namespace Kafka4php\Result;

/**
 * Cannot be handled now: the host sends the original message to the next retry tier,
 * or to the DLQ once its attempts are exhausted.
 */
final readonly class RetryLater implements Result
{
    public function __construct(public string $reason)
    {
    }

    /** @return array{type: 'retry_later', reason: string} */
    public function toNative(): array
    {
        return ['type' => 'retry_later', 'reason' => $this->reason];
    }
}
