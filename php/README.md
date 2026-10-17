# kafka4php/framework

The PHP side of [kafka4php](../docs/DESIGN.md): kafka4php consumes Kafka and runs your
entrypoint script once per message, as one isolated PHP request (like PHP-FPM does for
HTTP). This package turns that request into a typed `Message` for your handler and its
`Result` into what kafka4php does next. Pure PHP ≥ 8.4, no runtime dependencies.

```sh
composer require kafka4php/framework
```

## Entrypoint

The script configured as `php.entrypoint` runs for **every** message, so keep its
bootstrap small (`opcache.preload` is the lever for big apps, on PHP 8.5):

```php
<?php
require __DIR__ . '/vendor/autoload.php';

use Kafka4php\Kernel;
use Kafka4php\Router;

(new Kernel())->run((new Router())
    ->on('orders', new App\OrderPlacedHandler(new App\StockService()))
    ->on('payments', new App\PaymentHandler()));
```

A handler is any `callable(Message): Result`: a class implementing `Handler`
(`__invoke`), a closure, or a `Router` (which is a `Handler` itself, so routers nest).
The `Router` dispatches on the message's logical topic — retried messages arrive with
their original topic, so they reach the same handler. A message for a topic without a
handler fails (it goes to the DLQ).

## Handlers

```php
use Kafka4php\Handler;
use Kafka4php\Message;
use Kafka4php\OutboundMessage;
use Kafka4php\Result\{Content, Fail, NoContent, Result, RetryLater};

final class OrderPlacedHandler implements Handler
{
    public function __construct(private StockService $stock)
    {
    }

    public function __invoke(Message $message): Result
    {
        $order = json_decode($message->value ?? '', true);
        if (!is_array($order)) {
            return new Fail('not a JSON object', 'InvalidPayload'); // straight to the DLQ
        }
        if (!$this->stock->isAvailable()) {
            return new RetryLater('stock service unavailable');      // next retry tier
        }
        // ...
        return new Content(new OutboundMessage(                      // produce, then commit
            topic: 'orders.confirmed',
            value: json_encode(['id' => $order['id']]),
            key: $message->key,
            headers: $message->headers->toList(),
        ));
        // or: return new NoContent();                               // just commit
    }
}
```

| Result | kafka4php then |
|---|---|
| `NoContent` | commits the message |
| `Content(OutboundMessage ...$messages)` | produces all messages, waits for the broker's acks, commits (no messages = `NoContent`) |
| `Fail($reason, $errorClass = null)` | sends the original message to the DLQ, commits |
| `RetryLater($reason)` | sends the original message to the next retry tier (or the DLQ once `max_attempts` is reached), commits |

Delivery is **at least once**: a handler may see the same message twice (after a
crash, a rebalance or a timeout), so make it idempotent.

`Message` has `topic` (logical), `deliveryTopic` (the topic it was consumed from — a
retry topic or the same), `partition`, `offset`, `timestampMs`, `key`, `value` (both
binary-safe, nullable), `headers` and `attempt` (1 on first delivery). `Headers` keeps
Kafka's semantics — ordered, names may repeat, exact-case names: `get($name)` is the
first value, `all($name)` every value, `has($name)`, `toList()` the `[name, value]` pairs.
kafka4php's own `k4p-*` headers are never shown to handlers.

## Exceptions

`Kernel::run()` catches anything the handler throws (also the `TypeError` of one
returning something other than a `Result`) and reports a result for it:

- by default `RetryLater("<class>: <message>")`: retried, and dead-lettered once the
  attempts are exhausted;
- with `(new Kernel(onException: ExceptionPolicy::Fail))->run($router)`,
  `Fail("<class>: <message>", errorClass: <class>)`: dead-lettered right away.

Return `Fail` / `RetryLater` yourself where you know better. A fatal error, `exit()`
without a result, or a timeout is handled by kafka4php itself (as a retry).

## Runtime notes

- `PHP_SAPI` is `kafka4php`, not `cli`.
- Output (`echo`, `var_dump`, …) goes to kafka4php's log, never into a result.
- Nothing survives a request: statics, singletons and connections start fresh per
  message. Process-wide state does leak between threads: `setlocale()`, `putenv()`,
  `umask()`; `pcntl_*` is disabled.

## Testing handlers

Handlers are plain PHP: test them with `Testing\MessageBuilder`. To test the
entrypoint's `Kernel`, construct it with a `Testing\FakeNativeBridge`:

```php
use Kafka4php\Kernel;
use Kafka4php\Testing\{FakeNativeBridge, MessageBuilder};

$message = MessageBuilder::forTopic('orders')
    ->withKey('order-1')
    ->withValue('{"id": 1}')
    ->withHeader('trace-id', 'abc');

$result = (new OrderPlacedHandler($stock))($message->build());

$bridge = new FakeNativeBridge($message->withAttempt(2)); // the builder is mutable
(new Kernel($bridge))->run($router);
$bridge->results(); // [['type' => 'no_content']]: the arrays kafka4php would receive
```

For IDEs and static analysis outside the package, `stubs/native.php` declares the native
functions `Kafka4php\Native\message()` / `result()` that the framework wraps.

## Developing this package

```sh
scripts/php-test.sh                    # composer test + composer stan on php:8.4-cli
PHP_VERSION=8.5 scripts/php-test.sh
```
