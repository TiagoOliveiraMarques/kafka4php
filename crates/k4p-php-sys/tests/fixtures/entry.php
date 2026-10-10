<?php
// Entrypoint of the `sapi` and `leaks` test binaries: runs the fixture named by the
// message's topic, so one runtime (one entrypoint) can exercise every fixture.
declare(strict_types=1);

$k4pFixture = \Kafka4php\Native\message()['topic'];
if (!preg_match('/^[a-z0-9_]+$/', $k4pFixture) || !is_file(__DIR__ . "/$k4pFixture.php")) {
    throw new \RuntimeException("unknown fixture '$k4pFixture'");
}
require __DIR__ . "/$k4pFixture.php";
