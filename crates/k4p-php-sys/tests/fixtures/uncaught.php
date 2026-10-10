<?php
namespace App;

class OrderFailed extends \RuntimeException {}

function handle(): void
{
    throw new OrderFailed("db down: caf\u{e9}");
}

\Kafka4php\Native\result(['type' => 'no_content']); // overridden by the exception
handle();
