<?php
// A user exception handler handles the exception: it is not "uncaught".
set_exception_handler(static function (\Throwable $e): void {
    \Kafka4php\Native\result([
        'type' => 'fail',
        'reason' => 'handled: ' . $e->getMessage(),
        'error_class' => $e::class,
    ]);
});
throw new \DomainException('boom');
