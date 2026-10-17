<?php

declare(strict_types=1);

namespace Bench\Framework;

final class Headers
{
    /** @param list<array{0: string, 1: ?string}> $pairs */
    public function __construct(private readonly array $pairs = []) {}

    public function get(string $name): ?string
    {
        foreach ($this->pairs as [$k, $v]) {
            if ($k === $name) {
                return $v;
            }
        }
        return null;
    }

    /** @return list<?string> */
    public function all(string $name): array
    {
        $values = [];
        foreach ($this->pairs as [$k, $v]) {
            if ($k === $name) {
                $values[] = $v;
            }
        }
        return $values;
    }

    /** @return list<array{0: string, 1: ?string}> */
    public function toList(): array
    {
        return $this->pairs;
    }
}
