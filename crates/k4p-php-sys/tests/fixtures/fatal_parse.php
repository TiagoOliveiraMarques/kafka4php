<?php
// A ParseError from require is reported as E_PARSE (a fatal error), not as an
// uncaught exception, exactly as PHP itself reports it.
require __DIR__ . '/broken/parse_error.inc';
