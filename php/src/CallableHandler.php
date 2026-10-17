<?php

declare(strict_types=1);

namespace Kafka4php;

use Kafka4php\Result\Result;

/**
 * @internal adapts a `callable(Message): Result` to Handler, so the Kernel and the
 * Router take both
 */
final readonly class CallableHandler implements Handler
{
    private \Closure $handler;

    private function __construct(callable $handler)
    {
        $this->handler = $handler(...);
    }

    public static function wrap(Handler|callable $handler): Handler
    {
        return $handler instanceof Handler ? $handler : new self($handler);
    }

    /**
     * @throws \TypeError if the callable does not return a Result (what PHP itself throws
     *   for a Handler class returning the wrong type)
     */
    public function handle(Message $message): Result
    {
        $result = ($this->handler)($message);
        if (!$result instanceof Result) {
            throw new \TypeError(sprintf(
                'Handler must return %s, %s returned',
                Result::class,
                get_debug_type($result),
            ));
        }

        return $result;
    }
}
