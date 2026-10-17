<?php
// Benchmark scenarios "kernel" and "preload": a typical entrypoint (DESIGN.md §5.4).
require __DIR__ . '/vendor/autoload.php';

use Bench\App\OrderPlacedHandler;
use Bench\Framework\Kernel;
use Bench\Framework\Router;

Kernel::run((new Router())->on('orders', new OrderPlacedHandler()));
