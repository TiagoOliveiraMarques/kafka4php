<?php
// register_shutdown_function() callbacks are part of the script: their result()
// counts, and message() works inside them.
register_shutdown_function(static function (): void {
    \Kafka4php\Native\result([
        'type' => 'retry_later',
        'reason' => 'from shutdown function for ' . \Kafka4php\Native\message()['topic'],
    ]);
});
\Kafka4php\Native\result(['type' => 'no_content']);
