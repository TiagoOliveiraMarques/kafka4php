<?php

declare(strict_types=1);

namespace Kafka4php\Tests;

use Kafka4php\Headers;
use PHPUnit\Framework\TestCase;

final class HeadersTest extends TestCase
{
    private Headers $headers;

    protected function setUp(): void
    {
        $this->headers = new Headers([
            ['trace', 'a'],
            ['nullable', null],
            ['trace', 'b'],
            ['Trace', 'upper'],
        ]);
    }

    public function testGetReturnsTheFirstMatch(): void
    {
        self::assertSame('a', $this->headers->get('trace'));
    }

    public function testAllReturnsEveryMatchInOrder(): void
    {
        self::assertSame(['a', 'b'], $this->headers->all('trace'));
        self::assertSame([], $this->headers->all('missing'));
    }

    public function testNamesAreCaseSensitive(): void
    {
        self::assertSame('upper', $this->headers->get('Trace'));
        self::assertSame(['upper'], $this->headers->all('Trace'));
        self::assertFalse($this->headers->has('TRACE'));
    }

    public function testHasTellsANullValueFromAMissingHeader(): void
    {
        self::assertNull($this->headers->get('nullable'));
        self::assertTrue($this->headers->has('nullable'));
        self::assertSame([null], $this->headers->all('nullable'));

        self::assertNull($this->headers->get('missing'));
        self::assertFalse($this->headers->has('missing'));
    }

    public function testToListKeepsEveryPairInOrder(): void
    {
        self::assertSame(
            [['trace', 'a'], ['nullable', null], ['trace', 'b'], ['Trace', 'upper']],
            $this->headers->toList(),
        );
        self::assertSame([], (new Headers())->toList());
    }
}
