<?php

declare(strict_types=1);

namespace Kafka4php;

/**
 * Kafka record headers: ordered, names may repeat, values are binary-safe strings or
 * null. Name lookups are exact (case-sensitive), like Kafka's.
 *
 * @phpstan-import-type NativeHeaders from NativeBridge
 */
final readonly class Headers
{
    /** @param NativeHeaders $headers */
    public function __construct(private array $headers = [])
    {
    }

    /** The value of the first header named `$name`; null if there is none (see has()) or it is null. */
    public function get(string $name): ?string
    {
        return $this->all($name)[0] ?? null;
    }

    public function has(string $name): bool
    {
        return $this->all($name) !== [];
    }

    /**
     * The values of every header named `$name`, in order.
     *
     * @return list<?string>
     */
    public function all(string $name): array
    {
        $values = [];
        foreach ($this->headers as [$headerName, $value]) {
            if ($headerName === $name) {
                $values[] = $value;
            }
        }

        return $values;
    }

    /**
     * Every header as a `[name, value]` pair, in order (the shape OutboundMessage takes).
     *
     * @return NativeHeaders
     */
    public function toList(): array
    {
        return $this->headers;
    }
}
