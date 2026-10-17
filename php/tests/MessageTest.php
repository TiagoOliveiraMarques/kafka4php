<?php

declare(strict_types=1);

namespace Kafka4php\Tests;

use Kafka4php\Headers;
use Kafka4php\Message;
use Kafka4php\Testing\MessageBuilder;
use PHPUnit\Framework\TestCase;

final class MessageTest extends TestCase
{
    public function testFromNativeMapsEveryField(): void
    {
        $message = Message::fromNative([
            'topic' => 'orders',
            'delivery_topic' => 'orders.retry.10s',
            'partition' => 3,
            'offset' => 42,
            'timestamp' => 1_760_000_000_123,
            'key' => 'order-1',
            'value' => '{"id":1}',
            'headers' => [['trace', 'abc'], ['empty', null]],
            'attempt' => 2,
        ]);

        self::assertEquals(new Message(
            topic: 'orders',
            deliveryTopic: 'orders.retry.10s',
            partition: 3,
            offset: 42,
            timestampMs: 1_760_000_000_123,
            key: 'order-1',
            value: '{"id":1}',
            headers: new Headers([['trace', 'abc'], ['empty', null]]),
            attempt: 2,
        ), $message);
    }

    public function testBinaryKeyAndValueArePreserved(): void
    {
        $binary = "\x00\xff\xfe\n\x00k4p\x00";
        $message = Message::fromNative(
            MessageBuilder::forTopic('t')->withKey($binary)->withValue($binary . 'v')->toNative(),
        );

        self::assertSame($binary, $message->key);
        self::assertSame($binary . 'v', $message->value);
    }

    public function testNullKeyValueAndTimestamp(): void
    {
        $message = Message::fromNative(MessageBuilder::forTopic('t')->toNative());

        self::assertNull($message->key);
        self::assertNull($message->value);
        self::assertNull($message->timestampMs);
    }

    public function testEmptyKeyAndValueAreNotNull(): void
    {
        $message = MessageBuilder::forTopic('t')->withKey('')->withValue('')->build();

        self::assertSame('', $message->key);
        self::assertSame('', $message->value);
    }

    public function testDuplicateHeadersKeepTheirOrder(): void
    {
        $message = Message::fromNative(MessageBuilder::forTopic('t')
            ->withHeader('h', 'first')
            ->withHeader('other', "\x00bin")
            ->withHeader('h', null)
            ->withHeader('h', 'third')
            ->toNative());

        self::assertSame('first', $message->headers->get('h'));
        self::assertSame(['first', null, 'third'], $message->headers->all('h'));
        self::assertSame(
            [['h', 'first'], ['other', "\x00bin"], ['h', null], ['h', 'third']],
            $message->headers->toList(),
        );
    }

    public function testBuilderDefaults(): void
    {
        self::assertSame([
            'topic' => 'orders',
            'delivery_topic' => 'orders',
            'partition' => 0,
            'offset' => 0,
            'timestamp' => null,
            'key' => null,
            'value' => null,
            'headers' => [],
            'attempt' => 1,
        ], MessageBuilder::forTopic('orders')->toNative());
    }

    public function testBuilderSetsEveryField(): void
    {
        $message = MessageBuilder::forTopic('orders')
            ->withDeliveryTopic('orders.retry.1m')
            ->withPartition(5)
            ->withOffset(7)
            ->withTimestampMs(99)
            ->withKey('k')
            ->withValue('v')
            ->withHeader('h', 'x')
            ->withAttempt(3)
            ->build();

        self::assertEquals(new Message(
            topic: 'orders',
            deliveryTopic: 'orders.retry.1m',
            partition: 5,
            offset: 7,
            timestampMs: 99,
            key: 'k',
            value: 'v',
            headers: new Headers([['h', 'x']]),
            attempt: 3,
        ), $message);
    }
}
