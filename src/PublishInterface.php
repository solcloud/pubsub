<?php

namespace Solcloud\PubSub;

use Google\Cloud\PubSub\Message;

interface PublishInterface {

    /**
     * @param array<mixed> $options
     */
    public function publish(Message $msg, string $topicName, array $options = []): void;

}
