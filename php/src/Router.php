<?php

declare(strict_types=1);

namespace Kafka4php;

use Kafka4php\Result\Fail;
use Kafka4php\Result\Result;

/**
 * Dispatches by logical topic (Message::$topic, so retried messages reach the same
 * handler). A message for a topic without a handler fails: it goes to the DLQ.
 */
final class Router implements Handler
{
    /** @var array<string, Handler> */
    private array $handlers = [];

    /**
     * @param Handler|callable(Message): Result $handler
     *
     * @throws \InvalidArgumentException if `$topic` already has a handler
     */
    public function on(string $topic, Handler|callable $handler): self
    {
        if (isset($this->handlers[$topic])) {
            throw new \InvalidArgumentException("a handler for topic \"$topic\" is already registered");
        }
        $this->handlers[$topic] = CallableHandler::wrap($handler);

        return $this;
    }

    public function handle(Message $message): Result
    {
        $handler = $this->handlers[$message->topic] ?? null;
        if ($handler === null) {
            return new Fail("no handler for topic \"$message->topic\"");
        }

        return $handler->handle($message);
    }
}
