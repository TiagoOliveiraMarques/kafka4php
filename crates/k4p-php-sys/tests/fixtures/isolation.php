<?php
// Reports which state from a previous request is visible, then sets all of it.
// Every request must report a clean slate.
function k4p_isolation_counter(): int
{
    static $n = 0;
    return ++$n;
}

class K4pIsolation
{
    public static int $count = 0;
    public static array $cache = [];
}

$seen = [
    'global' => isset($GLOBALS['k4p_isolation_global']),
    'const' => defined('K4P_ISOLATION_CONST'),
    'static_var' => k4p_isolation_counter(),
    'static_prop' => ++K4pIsolation::$count,
    'static_cache' => count(K4pIsolation::$cache),
    'ini' => ini_get('precision'),
    'included' => count(get_included_files()),
    'output_level' => ob_get_level(),
    'error_handler_set' => set_error_handler(null) !== null,
];
restore_error_handler();

$GLOBALS['k4p_isolation_global'] = true;
define('K4P_ISOLATION_CONST', 1);
K4pIsolation::$cache = range(1, 1000);
ini_set('precision', '5');
ob_start();
set_error_handler(static fn () => true);

\Kafka4php\Native\result([
    'type' => 'content',
    'messages' => [['topic' => 'isolation', 'value' => json_encode($seen)]],
]);
