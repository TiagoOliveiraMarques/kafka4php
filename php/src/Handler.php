<?php

declare(strict_types=1);

namespace Kafka4php;

use Kafka4php\Result\Result;

/**
 * A message handler as a class. Any `callable(Message): Result` (e.g. a closure) is a
 * handler too: the Kernel and the Router just call it.
 */
interface Handler
{
    /**
     * Handle one message. A thrown exception becomes a result through the Kernel's
     * ExceptionPolicy.
     */
    public function __invoke(Message $message): Result;
}
