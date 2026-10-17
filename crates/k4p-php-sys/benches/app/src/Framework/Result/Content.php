<?php

declare(strict_types=1);

namespace Bench\Framework\Result;

use Bench\Framework\OutboundMessage;

final class Content implements Result
{
    /** @var list<OutboundMessage> */
    public readonly array $messages;

    public function __construct(OutboundMessage ...$messages)
    {
        $this->messages = array_values($messages);
    }

    public function toNative(): array
    {
        return [
            'type' => 'content',
            'messages' => array_map(static fn (OutboundMessage $m): array => $m->toNative(), $this->messages),
        ];
    }
}
