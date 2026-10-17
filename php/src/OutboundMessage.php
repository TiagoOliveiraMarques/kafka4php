<?php

declare(strict_types=1);

namespace Kafka4php;

/**
 * A record to produce (see Result\Content).
 *
 * @phpstan-import-type NativeHeaders from NativeBridge
 * @phpstan-import-type NativeOutbound from NativeBridge
 */
final readonly class OutboundMessage
{
    /**
     * @param string $topic non-empty, valid UTF-8
     * @param NativeHeaders $headers `[name, value]` pairs, names valid UTF-8 (Headers::toList() fits)
     * @param ?int $partition null = Kafka's default partitioner (by key)
     */
    public function __construct(
        public string $topic,
        public ?string $value,
        public ?string $key = null,
        public array $headers = [],
        public ?int $partition = null,
    ) {
    }

    /**
     * @internal
     *
     * @return NativeOutbound
     */
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
