<?php

declare(strict_types=1);

namespace Kafka4php;

use Kafka4php\Result\Result;

/** Runs one request: the entrypoint calls `(new Kernel())->run($handler)` once per message. */
final readonly class Kernel
{
    /**
     * @param NativeBridge $bridge the native functions by default; Testing\FakeNativeBridge in tests
     * @param ExceptionPolicy $onException what an exception thrown by the handler becomes
     */
    public function __construct(
        private NativeBridge $bridge = new NativeFunctionsBridge(),
        private ExceptionPolicy $onException = ExceptionPolicy::RetryLater,
    ) {
    }

    /**
     * Read the current message, hand it to `$handler` and report its result, exactly
     * once. An exception from the handler (including the \TypeError of returning
     * something other than a Result) becomes a result through the ExceptionPolicy.
     *
     * @param callable(Message): Result $handler a Handler, a Router or any callable
     */
    public function run(callable $handler): void
    {
        $message = Message::fromNative($this->bridge->message());
        $this->bridge->result($this->resultOf($handler, $message)->toNative());
    }

    /** @param callable(Message): Result $handler */
    private function resultOf(callable $handler, Message $message): Result
    {
        try {
            return $handler($message);
        } catch (\Throwable $exception) {
            return $this->onException->resultFor($exception);
        }
    }
}
