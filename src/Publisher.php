<?php
declare(strict_types=1);

namespace Solcloud\PubSub;

use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\PubSubClient;

class Publisher implements PublishInterface {

    public function __construct(
        private readonly PubSubClient $pubSub,
    ) {
    }

    public function publish(Message $msg, string $topicName, array $options = []): void {
        $this->pubSub->topic($topicName)->publish($msg, $options);
    }

}
