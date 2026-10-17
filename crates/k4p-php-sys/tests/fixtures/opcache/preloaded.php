<?php
// Declared by preload.php only: never included by a request.
final class K4pPreloaded
{
    public static function greet(string $name): string
    {
        return "hello $name";
    }
}

function k4p_preloaded_function(): string
{
    return 'preloaded';
}
