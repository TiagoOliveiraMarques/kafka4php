<?php

declare(strict_types=1);

namespace Kafka4php\Testing;

use Kafka4php\Message;
use Kafka4php\NativeBridge;

/**
 * Builds test messages: `MessageBuilder::forTopic('orders')->withValue('{...}')->build()`.
 * Each `with*()` sets a field and returns the builder. Defaults: partition 0, offset 0,
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
        $this->deliveryTopic = $deliveryTopic;

        return $this;
    }

    public function withPartition(int $partition): self
    {
        $this->partition = $partition;

        return $this;
    }

    public function withOffset(int $offset): self
    {
        $this->offset = $offset;

        return $this;
    }

    public function withTimestampMs(?int $timestampMs): self
    {
        $this->timestampMs = $timestampMs;

        return $this;
    }

    public function withKey(?string $key): self
    {
        $this->key = $key;

        return $this;
    }

    public function withValue(?string $value): self
    {
        $this->value = $value;

        return $this;
    }

    /** Appends a header; call again with the same name for duplicates. */
    public function withHeader(string $name, ?string $value): self
    {
        $this->headers[] = [$name, $value];

        return $this;
    }

    public function withAttempt(int $attempt): self
    {
        $this->attempt = $attempt;

        return $this;
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
