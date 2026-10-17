<?php
// Blocks in C for `value` milliseconds with no soft timeout: only the hard timeout
// (the pool's watchdog) can end the wait for it.
use function Kafka4php\Native\{message, result};

set_time_limit(0);
usleep((int) message()['value'] * 1000);
result(['type' => 'no_content']);
