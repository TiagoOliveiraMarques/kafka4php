<?php

declare(strict_types=1);

namespace Kafka4php;

/**
 * The Kafka message a request handles.
 *
 * @phpstan-import-type NativeMessage from NativeBridge
 */
final readonly class Message
{
    /**
     * @param string $topic logical topic: the original one, also when delivered via a retry topic
     * @param string $deliveryTopic the topic it was consumed from (== $topic unless retried)
     * @param int $partition partition of $deliveryTopic
     * @param int $offset offset in $deliveryTopic
     * @param ?int $timestampMs record timestamp, unix ms
     * @param ?string $key binary-safe
     * @param ?string $value binary-safe
     * @param int $attempt 1 on first delivery, incremented on each retry
     */
    public function __construct(
        public string $topic,
        public string $deliveryTopic,
        public int $partition,
        public int $offset,
        public ?int $timestampMs,
        public ?string $key,
        public ?string $value,
        public Headers $headers,
        public int $attempt,
    ) {
    }

    /** @param NativeMessage $message */
    public static function fromNative(array $message): self
    {
        return new self(
            topic: $message['topic'],
            deliveryTopic: $message['delivery_topic'],
            partition: $message['partition'],
            offset: $message['offset'],
            timestampMs: $message['timestamp'],
            key: $message['key'],
            value: $message['value'],
            headers: new Headers($message['headers']),
            attempt: $message['attempt'],
        );
    }
}
