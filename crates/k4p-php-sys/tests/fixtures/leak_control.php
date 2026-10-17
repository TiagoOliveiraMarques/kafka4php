<?php
// Positive control for the debug-build leak check: zend_leak_bytes() (zend_test
// extension, debug image only) deliberately leaks request memory.
zend_leak_bytes(64);
\Kafka4php\Native\result(['type' => 'no_content']);
