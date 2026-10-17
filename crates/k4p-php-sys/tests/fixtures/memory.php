<?php
// Entrypoint of the `memory` test binary. Reports memory_get_usage() at script start,
// then builds ~1 MB of userland data (10,000 strings of ~100 bytes in an array, plus
// objects) that the request's end must free.
$start = memory_get_usage();

final class K4pMemoryItem
{
    public function __construct(public readonly int $id, public readonly string $payload) {}
}

$data = [];
for ($i = 0; $i < 10000; $i++) {
    $data[] = str_repeat(chr(65 + $i % 26), 100) . $i;
}
$objects = [];
for ($i = 0; $i < 1000; $i++) {
    $objects[] = new K4pMemoryItem($i, $data[$i]);
}
$peak = memory_get_usage();

\Kafka4php\Native\result([
    'type' => 'content',
    'messages' => [['topic' => 'memory', 'value' => "$start:$peak"]],
]);
