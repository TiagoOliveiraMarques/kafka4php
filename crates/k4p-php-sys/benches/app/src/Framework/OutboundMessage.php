<?php

declare(strict_types=1);

namespace Bench\Framework;

final class OutboundMessage
{
    /** @param list<array{0: string, 1: ?string}> $headers */
    public function __construct(
        public readonly string $topic,
        public readonly ?string $value,
        public readonly ?string $key = null,
        public readonly array $headers = [],
        public readonly ?int $partition = null,
    ) {}

    /** @return array<string, mixed> */
    public function toNative(): array
    {
        return [
            'topic' => $this->topic,
            'value' => $this->value,
            'key' => $this->key,
            'headers' => $this->headers,
            'partition' => $this->partition,
        ];
    }
}
