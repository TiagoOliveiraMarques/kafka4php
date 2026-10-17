<?php

declare(strict_types=1);

namespace Bench\Framework\Result;

final class Fail implements Result
{
    public function __construct(public readonly string $reason, public readonly ?string $errorClass = null) {}

    public function toNative(): array
    {
        return ['type' => 'fail', 'reason' => $this->reason, 'error_class' => $this->errorClass];
    }
}
