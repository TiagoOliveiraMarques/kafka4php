<?php
// Busy loop past max_execution_time (the sapi test runtime sets it to 1 second).
\Kafka4php\Native\result(['type' => 'no_content']); // overridden by the timeout
$i = 0;
while (true) {
    $i++;
}
