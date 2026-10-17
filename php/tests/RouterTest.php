<?php

declare(strict_types=1);

namespace Kafka4php\Tests;

use Kafka4php\Handler;
use Kafka4php\Message;
use Kafka4php\Result\Fail;
use Kafka4php\Result\NoContent;
use Kafka4php\Result\Result;
use Kafka4php\Result\RetryLater;
use Kafka4php\Router;
use Kafka4php\Testing\MessageBuilder;
use PHPUnit\Framework\TestCase;

final class RouterTest extends TestCase
{
    public function testDispatchesByTopic(): void
    {
        $router = (new Router())
            ->on('orders', self::replyWith('orders'))
            ->on('payments', self::replyWith('payments'));

        self::assertEquals(new RetryLater('orders'), $router->handle(MessageBuilder::forTopic('orders')->build()));
        self::assertEquals(new RetryLater('payments'), $router->handle(MessageBuilder::forTopic('payments')->build()));
    }

    public function testDispatchesRetriedMessagesByLogicalTopic(): void
    {
        $router = (new Router())->on('orders', self::replyWith('orders'));
        $retried = MessageBuilder::forTopic('orders')->withDeliveryTopic('orders.retry.10s')->withAttempt(2);

        self::assertEquals(new RetryLater('orders'), $router->handle($retried->build()));
    }

    public function testPassesTheMessageToTheHandler(): void
    {
        $message = MessageBuilder::forTopic('orders')->withValue('payload')->build();
        $router = (new Router())->on('orders', static fn (Message $m): Result => new RetryLater((string) $m->value));

        self::assertEquals(new RetryLater('payload'), $router->handle($message));
    }

    public function testUnknownTopicFails(): void
    {
        $router = (new Router())->on('orders', self::replyWith('orders'));

        self::assertEquals(
            new Fail('no handler for topic "refunds"'),
            $router->handle(MessageBuilder::forTopic('refunds')->build()),
        );
        self::assertEquals(
            new Fail('no handler for topic "orders.retry.10s"'),
            (new Router())->handle(MessageBuilder::forTopic('orders.retry.10s')->build()),
        );
    }

    public function testTakesCallables(): void
    {
        $router = (new Router())
            ->on('closure', static fn (Message $m): Result => new NoContent())
            ->on('invokable', new class () {
                public function __invoke(Message $message): Result
                {
                    return new RetryLater('invokable');
                }
            });

        self::assertEquals(new NoContent(), $router->handle(MessageBuilder::forTopic('closure')->build()));
        self::assertEquals(new RetryLater('invokable'), $router->handle(MessageBuilder::forTopic('invokable')->build()));
    }

    public function testCallableReturningAnythingButAResultIsATypeError(): void
    {
        /** @phpstan-ignore argument.type (a wrong return type is under test) */
        $router = (new Router())->on('orders', static fn (Message $m): string => 'done');

        $this->expectException(\TypeError::class);
        $this->expectExceptionMessage('Handler must return Kafka4php\Result\Result, string returned');
        $router->handle(MessageBuilder::forTopic('orders')->build());
    }

    public function testRejectsASecondHandlerForATopic(): void
    {
        $router = (new Router())->on('orders', self::replyWith('first'));

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('a handler for topic "orders" is already registered');
        $router->on('orders', self::replyWith('second'));
    }

    public function testNumericTopicNames(): void
    {
        $router = (new Router())->on('123', self::replyWith('numeric'));

        self::assertEquals(new RetryLater('numeric'), $router->handle(MessageBuilder::forTopic('123')->build()));
    }

    /** A handler whose result names it. */
    private static function replyWith(string $name): Handler
    {
        return new readonly class ($name) implements Handler {
            public function __construct(private string $name)
            {
            }

            public function handle(Message $message): Result
            {
                return new RetryLater($this->name);
            }
        };
    }
}
