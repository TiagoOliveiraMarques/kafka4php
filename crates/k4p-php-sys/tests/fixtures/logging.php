<?php
// Output and errors must reach the host log, never the result.
echo "hello from php\n";
echo "two\nlines";
print " joined\n";
echo "\n"; // empty line: dropped
echo "no trailing newline";
// error_reporting is E_ALL by default: these are logged.
echo @$silenced; // suppressed: not logged
$x = $k4p_undefined_variable; // Warning: Undefined variable
trigger_error('a user notice', E_USER_NOTICE);
trigger_error('a user warning', E_USER_WARNING);
error_log('an error_log() line');
\Kafka4php\Native\result(['type' => 'no_content']);
