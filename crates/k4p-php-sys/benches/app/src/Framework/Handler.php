<?php

declare(strict_types=1);

namespace Bench\Framework;

use Bench\Framework\Result\Result;

interface Handler
{
    public function handle(Message $message): Result;
}
