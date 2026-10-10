<?php
// Exhausts memory_limit: an E_ERROR fatal error, not an exception.
ini_set('memory_limit', '16M');
\Kafka4php\Native\result(['type' => 'no_content']); // overridden by the fatal error
$a = [];
while (true) {
    $a[] = str_repeat('x', 4096);
}
