<?php

declare(strict_types=1);

namespace Kafka4php\Result;

/** Handled; produce nothing. The host commits the message. */
final readonly class NoContent implements Result
{
    /** @return array{type: 'no_content'} */
    public function toNative(): array
    {
        return ['type' => 'no_content'];
    }
}
