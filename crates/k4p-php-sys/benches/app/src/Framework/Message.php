<?php

declare(strict_types=1);

namespace Bench\Framework;

final class Message
{
    public function __construct(
        public readonly string $topic,
        public readonly string $deliveryTopic,
        public readonly int $partition,
        public readonly int $offset,
        public readonly ?int $timestampMs,
        public readonly ?string $key,
        public readonly ?string $value,
        public readonly Headers $headers,
        public readonly int $attempt,
    ) {}

    /** @param array<string, mixed> $m */
    public static function fromNative(array $m): self
    {
        return new self(
            $m['topic'],
            $m['delivery_topic'],
            $m['partition'],
            $m['offset'],
            $m['timestamp'],
            $m['key'],
            $m['value'],
            new Headers($m['headers']),
            $m['attempt'],
        );
    }
}
