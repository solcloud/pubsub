<?php
declare(strict_types=1);

namespace Solcloud\PubSub;

use Google\Cloud\PubSub\PubSubClient;
use Psr\Log\LoggerInterface;
use Symfony\Component\Serializer\SerializerInterface;

class ConsumerFactory {

    public function __construct(
        private PubSubClient        $pubSub,
        private SerializerInterface $serializer,
        private LoggerInterface     $logger,
    ) {
    }

    public function create(string $subscriptionName, string $inputMapperFormat = 'json'): Consumer {
        $consumer = new Consumer($this->pubSub, $this->serializer, $subscriptionName, $inputMapperFormat);
        $consumer->setLogger($this->logger);

        return $consumer;
    }

}
