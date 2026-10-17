<?php
// Returns the message's key, value and headers unchanged, plus its coordinates, so
// the test can check every concurrent request got its own message back intact.
use function Kafka4php\Native\{message, result};

$m = message();
result([
    'type' => 'content',
    'messages' => [[
        'topic' => 'echo',
        'key' => $m['key'],
        'value' => $m['value'],
        'headers' => [...$m['headers'], ['coordinates', "{$m['partition']}:{$m['offset']}:{$m['attempt']}"]],
    ]],
]);
