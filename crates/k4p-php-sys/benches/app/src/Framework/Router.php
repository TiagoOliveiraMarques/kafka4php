<?php

declare(strict_types=1);

namespace Bench\Framework;

use Bench\Framework\Result\Fail;
use Bench\Framework\Result\Result;

final class Router implements Handler
{
    /** @var array<string, Handler> */
    private array $handlers = [];

    public function on(string $topic, Handler $handler): self
    {
        $this->handlers[$topic] = $handler;
        return $this;
    }

    public function handle(Message $message): Result
    {
        $handler = $this->handlers[$message->topic] ?? null;
        return $handler === null
            ? new Fail("no handler for topic {$message->topic}")
            : $handler->handle($message);
    }
}
