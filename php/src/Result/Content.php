<?php

declare(strict_types=1);

namespace Kafka4php\Result;

use Kafka4php\NativeBridge;
use Kafka4php\OutboundMessage;

/**
 * Handled; produce these messages. The host commits the message once the broker
 * acknowledged all of them. No messages is the same as NoContent.
 *
 * @phpstan-import-type NativeOutbound from NativeBridge
 */
final readonly class Content implements Result
{
    /** @var list<OutboundMessage> */
    public array $messages;

    public function __construct(OutboundMessage ...$messages)
    {
        $this->messages = array_values($messages);
    }

    /** @return array{type: 'content', messages: list<NativeOutbound>} */
    public function toNative(): array
    {
        return [
            'type' => 'content',
            'messages' => array_map(
                static fn (OutboundMessage $message): array => $message->toNative(),
                $this->messages,
            ),
        ];
    }
}
