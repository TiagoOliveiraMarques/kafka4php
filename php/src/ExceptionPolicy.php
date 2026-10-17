<?php

declare(strict_types=1);

namespace Kafka4php;

use Kafka4php\Result\Fail;
use Kafka4php\Result\Result;
use Kafka4php\Result\RetryLater;

/** What the Kernel makes of an exception thrown by the handler. */
enum ExceptionPolicy
{
    /** RetryLater("<class>: <message>"): retried, and dead-lettered after max attempts. */
    case RetryLater;

    /** Fail("<class>: <message>", errorClass: <class>): dead-lettered right away. */
    case Fail;

    public function resultFor(\Throwable $exception): Result
    {
        $reason = $exception::class . ': ' . $exception->getMessage();

        return match ($this) {
            self::RetryLater => new RetryLater($reason),
            self::Fail => new Fail($reason, $exception::class),
        };
    }
}
