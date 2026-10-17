<?php

declare(strict_types=1);

namespace Kafka4php\Tests;

use Kafka4php\ExceptionPolicy;
use Kafka4php\Handler;
use Kafka4php\Kernel;
use Kafka4php\Message;
use Kafka4php\NativeBridge;
use Kafka4php\OutboundMessage;
use Kafka4php\Result\Content;
use Kafka4php\Result\NoContent;
use Kafka4php\Result\Result;
use Kafka4php\Router;
use Kafka4php\Testing\FakeNativeBridge;
use Kafka4php\Testing\MessageBuilder;
use PHPUnit\Framework\TestCase;

final class KernelTest extends TestCase
{
    public function testReportsTheHandlersResultOnce(): void
    {
        $bridge = new FakeNativeBridge(MessageBuilder::forTopic('orders')->withValue('v'));
        $handler = new class () implements Handler {
            public function __invoke(Message $message): Result
            {
                return new Content(new OutboundMessage('orders.out', $message->value, 'k'));
            }
        };

        (new Kernel($bridge))->run($handler);

        self::assertSame([['type' => 'content', 'messages' => [
            ['topic' => 'orders.out', 'value' => 'v', 'key' => 'k', 'headers' => [], 'partition' => null],
        ]]], $bridge->results());
    }

    public function testHandsTheBridgesMessageToTheHandler(): void
    {
        $builder = MessageBuilder::forTopic('orders')
            ->withDeliveryTopic('orders.retry.10s')
            ->withPartition(2)
            ->withOffset(10)
            ->withKey("\x00k")
            ->withHeader('h', '1')
            ->withAttempt(3);
        $received = null;

        (new Kernel(new FakeNativeBridge($builder)))->run(static function (Message $message) use (&$received): Result {
            $received = $message;

            return new NoContent();
        });

        self::assertEquals($builder->build(), $received);
    }

    public function testRunsARouter(): void
    {
        $bridge = new FakeNativeBridge(MessageBuilder::forTopic('payments'));

        (new Kernel($bridge))->run((new Router())->on('orders', static fn (Message $m): Result => new NoContent()));

        self::assertSame([['type' => 'fail', 'reason' => 'no handler for topic "payments"', 'error_class' => null]], $bridge->results());
    }

    public function testExceptionsAreRetriedByDefault(): void
    {
        $bridge = new FakeNativeBridge(MessageBuilder::forTopic('orders'));

        (new Kernel($bridge))->run(static fn (Message $m): Result => throw new \RuntimeException('db down'));

        self::assertSame([['type' => 'retry_later', 'reason' => 'RuntimeException: db down']], $bridge->results());
    }

    public function testExceptionsCanFailWithTheirClass(): void
    {
        $bridge = new FakeNativeBridge(MessageBuilder::forTopic('orders'));

        (new Kernel($bridge, ExceptionPolicy::Fail))->run(
            static fn (Message $m): Result => throw new InvalidOrder('missing id'),
        );

        self::assertSame([[
            'type' => 'fail',
            'reason' => 'Kafka4php\Tests\InvalidOrder: missing id',
            'error_class' => InvalidOrder::class,
        ]], $bridge->results());
    }

    public function testErrorsAreCaughtToo(): void
    {
        $bridge = new FakeNativeBridge(MessageBuilder::forTopic('orders'));

        (new Kernel($bridge))->run(static fn (Message $m): Result => throw new \DivisionByZeroError('Division by zero'));

        self::assertSame([['type' => 'retry_later', 'reason' => 'DivisionByZeroError: Division by zero']], $bridge->results());
    }

    public function testCallableReturningAWrongTypeGoesThroughThePolicy(): void
    {
        $retry = new FakeNativeBridge(MessageBuilder::forTopic('orders'));
        $fail = new FakeNativeBridge(MessageBuilder::forTopic('orders'));
        $handler = static fn (Message $m): ?Result => null;

        /** @phpstan-ignore argument.type (a wrong return type is under test) */
        (new Kernel($retry))->run($handler);
        /** @phpstan-ignore argument.type (a wrong return type is under test) */
        (new Kernel($fail, ExceptionPolicy::Fail))->run($handler);

        $reason = 'TypeError: Kafka4php\Kernel::resultOf(): Return value must be of type Kafka4php\Result\Result, null returned';
        self::assertSame([['type' => 'retry_later', 'reason' => $reason]], $retry->results());
        self::assertSame([['type' => 'fail', 'reason' => $reason, 'error_class' => \TypeError::class]], $fail->results());
    }

    public function testHandlerReturningAWrongTypeGoesThroughThePolicy(): void
    {
        $bridge = new FakeNativeBridge(MessageBuilder::forTopic('orders'));
        $handler = new class () implements Handler {
            public mixed $result = 'not a result';

            public function __invoke(Message $message): Result
            {
                /** @phpstan-ignore return.type (PHP's own return type check is under test) */
                return $this->result;
            }
        };

        (new Kernel($bridge))->run($handler);

        self::assertSame([[
            'type' => 'retry_later',
            'reason' => 'TypeError: Kafka4php\Handler@anonymous::__invoke(): Return value must be of type Kafka4php\Result\Result, string returned',
        ]], $bridge->results());
    }

    public function testDoesNotCatchWhatHappensOutsideTheHandler(): void
    {
        $bridge = new class () implements NativeBridge {
            public int $results = 0;

            public function message(): array
            {
                throw new \LogicException('not in a kafka4php request');
            }

            public function result(array $result): void
            {
                ++$this->results;
            }
        };

        try {
            (new Kernel($bridge))->run(static fn (Message $m): Result => new NoContent());
            self::fail('expected the bridge exception');
        } catch (\LogicException $exception) {
            self::assertSame('not in a kafka4php request', $exception->getMessage());
        }
        self::assertSame(0, $bridge->results);
    }

    public function testDefaultsToTheNativeFunctions(): void
    {
        // Outside the SAPI the native functions don't exist, so the default bridge fails.
        $this->expectException(\Error::class);
        $this->expectExceptionMessage('Call to undefined function Kafka4php\Native\message()');
        (new Kernel())->run(static fn (Message $m): Result => new NoContent());
    }
}
