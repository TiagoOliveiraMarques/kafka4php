<?php

declare(strict_types=1);

namespace Kafka4php\Testing;

use Kafka4php\NativeBridge;

/**
 * Stands in for the native functions when testing an entrypoint's Kernel::run() call:
 * serves one message (as built when the bridge was created) and records every result
 * reported, as the arrays the native `result()` receives. It does not validate them
 * like the C side does.
 *
 * @phpstan-import-type NativeMessage from NativeBridge
 * @phpstan-import-type NativeResult from NativeBridge
 */
final class FakeNativeBridge implements NativeBridge
{
    /** @var NativeMessage */
    private readonly array $message;
    /** @var list<NativeResult> */
    private array $results = [];

    public function __construct(MessageBuilder $message)
    {
        $this->message = $message->toNative();
    }

    public function message(): array
    {
        return $this->message;
    }

    public function result(array $result): void
    {
        $this->results[] = $result;
    }

    /**
     * Every result reported so far, in order (the native side keeps the last one).
     *
     * @return list<NativeResult>
     */
    public function results(): array
    {
        return $this->results;
    }
}
