<?php
// Sleeps for `value` milliseconds, then returns the value.
use function Kafka4php\Native\{message, result};

$m = message();
usleep((int) $m['value'] * 1000);
result(['type' => 'content', 'messages' => [['topic' => 'slept', 'value' => $m['value']]]]);
