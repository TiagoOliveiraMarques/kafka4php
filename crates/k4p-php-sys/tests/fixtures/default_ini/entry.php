<?php
// Entrypoint of the `default_ini` test binary: reports the effective INI settings the
// host's default INI touches.
$keys = [
    'display_errors', 'log_errors', 'html_errors', 'register_argc_argv', 'output_buffering',
    'memory_limit', 'max_execution_time', 'disable_functions', 'opcache.enable',
];
\Kafka4php\Native\result([
    'type' => 'content',
    'messages' => [[
        'topic' => 'default_ini',
        'value' => json_encode([
            'ini' => array_combine($keys, array_map('ini_get', $keys)),
            'sodium_loaded' => extension_loaded('sodium'),
        ], JSON_THROW_ON_ERROR),
    ]],
]);
