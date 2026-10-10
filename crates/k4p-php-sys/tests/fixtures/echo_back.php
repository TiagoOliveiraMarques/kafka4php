<?php
// Returns the message's key/value/headers unchanged (binary round trip), plus a
// "meta" message describing what PHP saw, hex-encoded, so the test can check the
// bytes arrived intact independently of the way back.
use function Kafka4php\Native\{message, result};

$m = message();
$hex = static fn (?string $s): ?string => $s === null ? null : bin2hex($s);

$meta = [
    'keys' => array_keys($m),
    'topic' => $m['topic'],
    'delivery_topic' => $m['delivery_topic'],
    'partition' => $m['partition'],
    'offset' => $m['offset'],
    'timestamp' => $m['timestamp'],
    'attempt' => $m['attempt'],
    'key_hex' => $hex($m['key']),
    'value_hex' => $hex($m['value']),
    'headers_hex' => array_map(static fn (array $h): array => [bin2hex($h[0]), $hex($h[1])], $m['headers']),
    'headers_is_list' => array_is_list($m['headers']),
    'same_on_second_call' => message() === $m,
];

result([
    'type' => 'content',
    'messages' => [
        ['topic' => 'echo', 'key' => $m['key'], 'value' => $m['value'], 'headers' => $m['headers']],
        ['topic' => 'meta', 'value' => json_encode($meta, JSON_THROW_ON_ERROR)],
    ],
]);
