<?php

declare(strict_types=1);

namespace Kafka4php;

use Kafka4php\Result\Result;

/** Runs one request: the entrypoint calls Kernel::run() once per message. */
final class Kernel
{
    private function __construct()
    {
    }

    /**
     * Read the current message, hand it to `$handler` and report its result. An
     * exception from the handler (including a callable not returning a Result) becomes
     * a result through `$onException`; the result is always reported.
     *
     * @param Handler|callable(Message): Result $handler
     * @param ?NativeBridge $bridge null = the native functions (Testing\FakeNativeBridge in tests)
     */
    public static function run(
        Handler|callable $handler,
        ?NativeBridge $bridge = null,
        ExceptionPolicy $onException = ExceptionPolicy::RetryLater,
    ): void {
        $bridge ??= new NativeFunctionsBridge();
        $message = Message::fromNative($bridge->message());
        try {
            $result = CallableHandler::wrap($handler)->handle($message);
        } catch (\Throwable $exception) {
            $result = $onException->resultFor($exception);
        }
        $bridge->result($result->toNative());
    }
}
