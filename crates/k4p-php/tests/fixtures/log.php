<?php
// One stdout line and one warning, to check the log context.
echo "hello from offset ", \Kafka4php\Native\message()['offset'], "\n";
trigger_error('careful', E_USER_WARNING);
\Kafka4php\Native\result(['type' => 'no_content']);
