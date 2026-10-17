<?php

declare(strict_types=1);

namespace Bench\App;

use Bench\Framework\Handler;
use Bench\Framework\Message;
use Bench\Framework\Result\NoContent;
use Bench\Framework\Result\Result;

/** A trivial handler: decode the JSON value, return NoContent. */
final class OrderPlacedHandler implements Handler
{
    public function handle(Message $message): Result
    {
        json_decode($message->value ?? 'null', true, 512, JSON_THROW_ON_ERROR);
        return new NoContent();
    }
}
