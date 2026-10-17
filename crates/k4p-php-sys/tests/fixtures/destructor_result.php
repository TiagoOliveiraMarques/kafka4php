<?php
// Destructors of objects still alive at request shutdown run after the outcome is
// determined: result() there throws \LogicException (logged), the outcome stands.
$GLOBALS['k4p_late'] = new class {
    public function __destruct()
    {
        \Kafka4php\Native\result(['type' => 'no_content']);
    }
};
\Kafka4php\Native\result(['type' => 'retry_later', 'reason' => 'before destructors']);
