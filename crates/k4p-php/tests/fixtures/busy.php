<?php
// Busy loop with a soft timeout (max_execution_time) of `value` seconds.
set_time_limit((int) \Kafka4php\Native\message()['value']);
\Kafka4php\Native\result(['type' => 'no_content']); // overridden by the timeout
while (true) {
}
