<?php

declare(strict_types=1);

namespace Kafka4php\Result;

/** Cannot be handled, ever: the host sends the original message to the DLQ. */
final readonly class Fail implements Result
{
    /** @param ?string $errorClass e.g. an exception class; becomes the DLQ's `k4p-failure-class` */
    public function __construct(
        public string $reason,
        public ?string $errorClass = null,
    ) {
    }

    /** @return array{type: 'fail', reason: string, error_class: ?string} */
    public function toNative(): array
    {
        return ['type' => 'fail', 'reason' => $this->reason, 'error_class' => $this->errorClass];
    }
}
