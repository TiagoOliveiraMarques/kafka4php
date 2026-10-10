<?php
// value "with_class": Fail with an error class; otherwise without one.
use function Kafka4php\Native\{message, result};

if (message()['value'] === 'with_class') {
    result(['type' => 'fail', 'reason' => 'bad order', 'error_class' => 'App\\InvalidOrder']);
} elseif (message()['value'] === 'null_class') {
    result(['type' => 'fail', 'reason' => 'bad order', 'error_class' => null]);
} else {
    result(['type' => 'fail', 'reason' => 'bad order']);
}
