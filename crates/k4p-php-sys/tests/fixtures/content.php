<?php
// Several messages, exercising every optional field.
\Kafka4php\Native\result([
    'type' => 'content',
    'messages' => [
        [
            'topic' => 'out-a',
            'value' => 'v1',
            'key' => 'k1',
            'headers' => [['h1', 'x'], ['h1', 'y'], ['h2', null], ['h3', '']],
            'partition' => 3,
        ],
        ['topic' => 'out-b', 'value' => null],
        ['topic' => 'out-a', 'value' => '', 'key' => null, 'headers' => [], 'partition' => null],
    ],
]);
