<?php

declare(strict_types=1);

namespace Kafka4php\Testing;

use Kafka4php\Message;
use Kafka4php\NativeBridge;

/**
 * Builds test messages: `MessageBuilder::forTopic('orders')->withValue('{...}')->build()`.
 * Immutable, so a builder can serve as a template. Defaults: partition 0, offset 0,
 * no timestamp, null key and value, no headers, attempt 1, delivered via `$topic`.
 *
 * @phpstan-import-type NativeHeaders from NativeBridge
 * @phpstan-import-type NativeMessage from NativeBridge
 */
final class MessageBuilder
{
    private ?string $deliveryTopic = null;
    private int $partition = 0;
    private int $offset = 0;
    private ?int $timestampMs = null;
    private ?string $key = null;
    private ?string $value = null;
    /** @var NativeHeaders */
    private array $headers = [];
    private int $attempt = 1;

    private function __construct(private readonly string $topic)
    {
    }

    /** @param string $topic the logical topic */
    public static function forTopic(string $topic): self
    {
        return new self($topic);
    }

    /** As if consumed from `$deliveryTopic`, e.g. a retry topic. */
    public function withDeliveryTopic(string $deliveryTopic): self
    {
        $builder = clone $this;
        $builder->deliveryTopic = $deliveryTopic;

        return $builder;
    }

    public function withPartition(int $partition): self
    {
        $builder = clone $this;
        $builder->partition = $partition;

        return $builder;
    }

    public function withOffset(int $offset): self
    {
        $builder = clone $this;
        $builder->offset = $offset;

        return $builder;
    }

    public function withTimestampMs(?int $timestampMs): self
    {
        $builder = clone $this;
        $builder->timestampMs = $timestampMs;

        return $builder;
    }

    public function withKey(?string $key): self
    {
        $builder = clone $this;
        $builder->key = $key;

        return $builder;
    }

    public function withValue(?string $value): self
    {
        $builder = clone $this;
        $builder->value = $value;

        return $builder;
    }

    /** Appends a header; call again with the same name for duplicates. */
    public function withHeader(string $name, ?string $value): self
    {
        $builder = clone $this;
        $builder->headers[] = [$name, $value];

        return $builder;
    }

    public function withAttempt(int $attempt): self
    {
        $builder = clone $this;
        $builder->attempt = $attempt;

        return $builder;
    }

    public function build(): Message
    {
        return Message::fromNative($this->toNative());
    }

    /**
     * @internal the array the native `message()` would return
     *
     * @return NativeMessage
     */
    public function toNative(): array
    {
        return [
            'topic' => $this->topic,
            'delivery_topic' => $this->deliveryTopic ?? $this->topic,
            'partition' => $this->partition,
            'offset' => $this->offset,
            'timestamp' => $this->timestampMs,
            'key' => $this->key,
            'value' => $this->value,
            'headers' => $this->headers,
            'attempt' => $this->attempt,
        ];
    }
}
