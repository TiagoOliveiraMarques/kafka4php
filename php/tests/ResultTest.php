<?php

declare(strict_types=1);

namespace Kafka4php\Tests;

use Kafka4php\OutboundMessage;
use Kafka4php\Result\Content;
use Kafka4php\Result\Fail;
use Kafka4php\Result\NoContent;
use Kafka4php\Result\Result;
use Kafka4php\Result\RetryLater;
use Kafka4php\Testing\MessageBuilder;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class ResultTest extends TestCase
{
    /** @return iterable<string, array{Result, array<string, mixed>}> */
    public static function results(): iterable
    {
        yield 'no content' => [new NoContent(), ['type' => 'no_content']];

        yield 'content without messages' => [new Content(), ['type' => 'content', 'messages' => []]];

        yield 'content with defaults' => [
            new Content(new OutboundMessage('out', 'v')),
            ['type' => 'content', 'messages' => [
                ['topic' => 'out', 'value' => 'v', 'key' => null, 'headers' => [], 'partition' => null],
            ]],
        ];

        yield 'content with every field, in order' => [
            new Content(
                new OutboundMessage('a', null, 'k', [['h', 'x'], ['h', null]], 2),
                new OutboundMessage(topic: 'b', value: "\x00\xff", key: "\x01", partition: 0),
            ),
            ['type' => 'content', 'messages' => [
                ['topic' => 'a', 'value' => null, 'key' => 'k', 'headers' => [['h', 'x'], ['h', null]], 'partition' => 2],
                ['topic' => 'b', 'value' => "\x00\xff", 'key' => "\x01", 'headers' => [], 'partition' => 0],
            ]],
        ];

        yield 'content forwarding inbound headers' => [
            new Content(new OutboundMessage(
                'out',
                'v',
                headers: MessageBuilder::forTopic('in')->withHeader('t', '1')->withHeader('t', '2')
                    ->build()->headers->toList(),
            )),
            ['type' => 'content', 'messages' => [
                ['topic' => 'out', 'value' => 'v', 'key' => null, 'headers' => [['t', '1'], ['t', '2']], 'partition' => null],
            ]],
        ];

        yield 'fail' => [new Fail('bad payload'), ['type' => 'fail', 'reason' => 'bad payload', 'error_class' => null]];

        yield 'fail with error class' => [
            new Fail('bad payload', 'App\\InvalidOrder'),
            ['type' => 'fail', 'reason' => 'bad payload', 'error_class' => 'App\\InvalidOrder'],
        ];

        yield 'retry later' => [new RetryLater('db down'), ['type' => 'retry_later', 'reason' => 'db down']];
    }

    /** @param array<string, mixed> $expected */
    #[DataProvider('results')]
    public function testToNative(Result $result, array $expected): void
    {
        self::assertSame($expected, $result->toNative());
    }

    public function testContentMessagesAreAListEvenWhenSpreadWithNames(): void
    {
        $content = new Content(...['first' => new OutboundMessage('a', null), 'second' => new OutboundMessage('b', null)]);

        // array_map() keeps keys, so this also asserts they are 0 and 1.
        self::assertSame(['a', 'b'], array_map(static fn (OutboundMessage $m): string => $m->topic, $content->messages));
    }
}
