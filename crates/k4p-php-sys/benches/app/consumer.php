<?php
// Benchmark scenarios "kernel" and "preload": a typical entrypoint (DESIGN.md §5.4).
require __DIR__ . '/vendor/autoload.php';

use Bench\App\OrderPlacedHandler;
use Kafka4php\Kernel;
use Kafka4php\Router;

(new Kernel())->run((new Router())->on('orders', new OrderPlacedHandler()));
