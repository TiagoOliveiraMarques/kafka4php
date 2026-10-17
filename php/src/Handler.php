<?php

declare(strict_types=1);

namespace Kafka4php;

use Kafka4php\Result\Result;

interface Handler
{
    /**
     * Handle one message. A thrown exception becomes a result through the Kernel's
     * ExceptionPolicy.
     */
    public function handle(Message $message): Result;
}
