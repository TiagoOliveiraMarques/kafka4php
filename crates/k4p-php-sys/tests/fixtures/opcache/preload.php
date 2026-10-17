<?php
// opcache.preload script of the `opcache` test binary: runs once, during runtime init,
// outside any request. What it declares must be visible in every request.
require __DIR__ . '/preloaded.php';
