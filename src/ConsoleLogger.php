<?php

namespace Solcloud\PubSub;

use Psr\Log\AbstractLogger;

class ConsoleLogger extends AbstractLogger {

    public function log($level, \Stringable|string $message, array $context = []): void { // @phpstan-ignore-line
        if (!is_string($level)) {
            $level = 'unknown';
        }
        if ($message instanceof \Throwable) {
            $message = $message->getMessage() . " '{$message->getFile()}:{$message->getLine()}'";
        }

        printf("[%s] %s [%s]\n", date('Y-m-d H:i:s'), $message, $level);
    }

}
