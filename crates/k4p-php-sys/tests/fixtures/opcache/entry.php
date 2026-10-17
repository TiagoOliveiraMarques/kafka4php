<?php
// Entrypoint of the `opcache` test binary. The message key selects what to do:
//   status: report OPcache / JIT / preload state as JSON
//   jit:    a hot loop (traced by the tracing JIT) whose result depends on the offset
use function Kafka4php\Native\{message, result};

// Proves preloaded classes need no autoloading.
spl_autoload_register(static function (string $class): never {
    throw new LogicException("unexpected autoload of $class");
});

function k4p_mix(int $acc, int $i, int $n): int
{
    return ($acc * 31 + $i * $n) % 1000003;
}

$m = message();
switch ($m['key']) {
    case 'status':
        $status = opcache_get_status(false);
        $value = json_encode([
            'php_version_id' => PHP_VERSION_ID,
            'opcache_loaded' => extension_loaded('Zend OPcache'),
            'status' => $status === false ? false : [
                'enabled' => $status['opcache_enabled'],
                'hits' => $status['opcache_statistics']['hits'],
                'entry_cached' => opcache_is_script_cached(__FILE__),
                'preload' => isset($status['preload_statistics']),
                'jit_enabled' => $status['jit']['enabled'] ?? false,
                'jit_on' => $status['jit']['on'] ?? false,
                'jit_buffer_used' => isset($status['jit'])
                    ? $status['jit']['buffer_size'] - $status['jit']['buffer_free'] : 0,
            ],
            'preloaded_class' => class_exists('K4pPreloaded', false)
                ? K4pPreloaded::greet('kafka4php') : null,
            'preloaded_function' => function_exists('k4p_preloaded_function')
                ? k4p_preloaded_function() : null,
        ], JSON_THROW_ON_ERROR);
        break;
    case 'jit':
        $acc = 0;
        for ($i = 0; $i < 2000; $i++) {
            $acc = k4p_mix($acc, $i, $m['offset']);
        }
        $value = (string) $acc;
        break;
    default:
        throw new LogicException("unknown key {$m['key']}");
}

result(['type' => 'content', 'messages' => [['topic' => 'opcache', 'value' => $value]]]);
