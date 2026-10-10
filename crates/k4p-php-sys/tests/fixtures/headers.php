<?php
// HTTP-ish functions are harmless no-ops: no warnings, no state.
header('X-Before: 1');
http_response_code(500);
echo "some output\n";
header('X-After: 2'); // after output: still no "headers already sent" warning
setcookie('k', 'v');
\Kafka4php\Native\result([
    'type' => 'content',
    'messages' => [['topic' => 'headers', 'value' => json_encode(headers_list())]],
]);
