<?php

declare(strict_types=1);

namespace Bench\App;

use Kafka4php\Handler;
use Kafka4php\Message;
use Kafka4php\Result\NoContent;
use Kafka4php\Result\Result;

/** A trivial handler: decode the JSON value, return NoContent. */
final class OrderPlacedHandler implements Handler
{
    public function __invoke(Message $message): Result
    {
        json_decode($message->value ?? 'null', true, 512, JSON_THROW_ON_ERROR);
        return new NoContent();
    }
}
