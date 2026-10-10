<?php
// Calls result() with the malformed array named by the message value. Modes (key):
//   (none)       the \ValueError is left uncaught
//   "caught"     the \ValueError is caught and nothing else happens
//   "then_valid" the \ValueError is caught, then a valid result is set
use function Kafka4php\Native\{message, result};

$cases = [
    'missing_type' => ['reason' => 'x'],
    'unknown_type' => ['type' => 'commit'],
    'type_not_string' => ['type' => 1],
    'extra_key' => ['type' => 'no_content', 'oops' => 1],
    'int_key' => ['type' => 'no_content', 0 => 'x'],
    'fail_no_reason' => ['type' => 'fail'],
    'fail_reason_int' => ['type' => 'fail', 'reason' => 42],
    'fail_class_int' => ['type' => 'fail', 'reason' => 'r', 'error_class' => 1],
    'retry_no_reason' => ['type' => 'retry_later'],
    'messages_missing' => ['type' => 'content'],
    'messages_not_list' => ['type' => 'content', 'messages' => ['a' => ['topic' => 't', 'value' => null]]],
    'message_not_array' => ['type' => 'content', 'messages' => ['nope']],
    'topic_missing' => ['type' => 'content', 'messages' => [['value' => 'v']]],
    'topic_empty' => ['type' => 'content', 'messages' => [['topic' => '', 'value' => 'v']]],
    'topic_not_utf8' => ['type' => 'content', 'messages' => [['topic' => "t\xff", 'value' => 'v']]],
    'value_missing' => ['type' => 'content', 'messages' => [['topic' => 't']]],
    'value_int' => ['type' => 'content', 'messages' => [['topic' => 't', 'value' => 1]]],
    'key_int' => ['type' => 'content', 'messages' => [['topic' => 't', 'value' => 'v', 'key' => 1]]],
    'partition_negative' => ['type' => 'content', 'messages' => [['topic' => 't', 'value' => 'v', 'partition' => -1]]],
    'partition_too_big' => ['type' => 'content', 'messages' => [['topic' => 't', 'value' => 'v', 'partition' => 2 ** 31]]],
    'partition_string' => ['type' => 'content', 'messages' => [['topic' => 't', 'value' => 'v', 'partition' => '1']]],
    'headers_not_list' => ['type' => 'content', 'messages' => [['topic' => 't', 'value' => 'v', 'headers' => ['a' => 'b']]]],
    'header_not_pair' => ['type' => 'content', 'messages' => [['topic' => 't', 'value' => 'v', 'headers' => [['a']]]]],
    'header_name_int' => ['type' => 'content', 'messages' => [['topic' => 't', 'value' => 'v', 'headers' => [[1, 'b']]]]],
    'header_name_not_utf8' => ['type' => 'content', 'messages' => [['topic' => 't', 'value' => 'v', 'headers' => [["a\xc3\x28", 'b']]]]],
    'header_value_int' => ['type' => 'content', 'messages' => [['topic' => 't', 'value' => 'v', 'headers' => [['a', 1]]]]],
    'message_extra_key' => ['type' => 'content', 'messages' => [['topic' => 't', 'value' => 'v', 'timestamp' => 1]]],
];

[$name, $mode] = array_pad(explode(':', message()['value'], 2), 2, '');

if ($mode === 'caught' || $mode === 'then_valid') {
    try {
        result($cases[$name]);
    } catch (\ValueError $e) {
        echo 'caught: ', $e->getMessage(), "\n";
    }
    if ($mode === 'then_valid') {
        result(['type' => 'no_content']);
    }
} else {
    result($cases[$name]);
}
