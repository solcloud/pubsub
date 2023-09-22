<?php
declare(strict_types=1);

namespace Solcloud\PubSub;

use Closure;
use Google\Cloud\Core\Exception\ServiceException;
use Google\Cloud\PubSub\Message;
use Google\Cloud\PubSub\PubSubClient;
use Google\Cloud\PubSub\Subscription;
use Solcloud\PubSub\Exception\MessageParseException;
use Solcloud\PubSub\Exception\PubsubException;
use Solcloud\PubSub\Exception\TimeoutException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use Symfony\Component\Serializer\SerializerInterface;
use Throwable;

class Consumer implements PublishInterface {

    private Message $message;
    private Subscription $subscription;
    private LoggerInterface $logger;
    private ?Closure $callback = null;
    private bool $ackSend = false;
    private int $numberOfProcessedMessages = 0;
    private int $maximumNumberOfProcessedMessages = -1;
    private ?string $failedTopicName = null;
    protected string $inputMapperClass = EmptyPayload::class;

    public function __construct(
        private readonly PubSubClient        $pubSub,
        private readonly SerializerInterface $serializer,
        private readonly string              $subscriptionName,
        private readonly string              $inputMapperFormat = 'json',
    ) {
        $this->logger = new NullLogger();
        $this->subscription = $pubSub->subscription($subscriptionName);
        $this->setup();
    }

    protected function setup(): void {
        // empty hook
    }

    /**
     * @throws TimeoutException
     * @throws ServiceException
     */
    public function consumeOneMsg(bool $shouldBlock = true): void {
        $msg = ($this->subscription->pull(['returnImmediately' => !$shouldBlock, 'maxMessages' => 1])[0] ?? null);
        if (!$msg) {
            if ($shouldBlock) {
                throw new TimeoutException("No message received while blocking");
            }
            return;
        }

        $this->ackSend = false;
        $this->message = $msg;

        try {
            $this->process();
        } catch (MessageParseException $ex) {
            $this->processUnparsed($ex, $msg);
        } catch (Throwable $ex) {
            $this->processFailed($ex, $msg);
        } finally {
            $this->sendAck();
            $this->numberOfProcessedMessages++;
        }
    }

    protected function process(): void {
        $this->beforeRun();
        $this->run();
        $this->afterRun();
    }

    protected function beforeRun(): void {
        // empty hook
    }

    protected function run(): void {
        if ($this->callback === null) {
            throw new PubsubException("Consumer do not have callback. Use setCallback() method or overwrite run().");
        }

        $msg = $this->getMessage();
        call_user_func($this->callback, $this->parseMessage($msg), $msg);
    }

    protected function afterRun(): void {
        // empty hook
    }

    protected function parseMessage(Message $msg, ?string $inputMapperClass = null): mixed {
        $inputMapperClass = ($inputMapperClass ?? $this->inputMapperClass);
        if ($inputMapperClass === EmptyPayload::class) {
            return new EmptyPayload();
        }

        try {
            $object = $this->serializer->deserialize($msg->data(), $inputMapperClass, $this->inputMapperFormat);
            return $object;
        } catch (Throwable $ex) {
            throw new MessageParseException("Message '{$msg->id()}' cannot be parsed.", $ex->getCode(), $ex);
        }
    }

    protected function processUnparsed(Throwable $ex, Message $msg): void {
        $this->logger->error($ex);
        $this->sendToFailed($msg);
    }

    protected function processFailed(Throwable $ex, Message $msg): void {
        $this->logger->error($ex);
        $this->sendToFailed($msg);
    }

    private function sendToFailed(Message $msg): void {
        if ($this->failedTopicName === null) {
            $this->sendReject();
            return;
        }

        $this->publish($msg, $this->failedTopicName);
    }

    protected function getMessage(): Message {
        return $this->message;
    }

    public function sendAck(): void {
        if ($this->ackSend) {
            return;
        }
        $this->subscription->acknowledge($this->getMessage());
        $this->ackSend = true;
    }

    public function sendReject(): void {
        if ($this->ackSend) {
            return;
        }
        $this->subscription->modifyAckDeadline($this->getMessage(), 0);
        $this->ackSend = true;
    }

    /**
     * Format from https://cloud.google.com/pubsub/docs/reference/rest/v1/PubsubMessage
     * @param array<mixed> $message
     * @param array<mixed> $metadata
     */
    public function createMessage(array $message, array $metadata = []): Message {
        return new Message($message, $metadata);
    }

    public function createMessageHelper(string $body): Message {
        return $this->createMessage(['data' => $body]);
    }

    public function publish(Message $msg, string $topicName, array $options = []): void {
        $this->pubSub->topic($topicName)->publish($msg, $options);
    }

    public function canConsume(): bool {
        return ($this->maximumNumberOfProcessedMessages === -1 || $this->numberOfProcessedMessages < $this->maximumNumberOfProcessedMessages);
    }

    public function getSubscriptionName(): string {
        return $this->subscriptionName;
    }

    public function getNumberOfProcessedMessages(): int {
        return $this->numberOfProcessedMessages;
    }

    public function getLogger(): LoggerInterface {
        return $this->logger;
    }

    /**
     * @param Closure $callback function (InputPayload $input, Message $msg): void {}
     */
    public function setCallback(Closure $callback, string $inputMapperClass = EmptyPayload::class): self {
        $this->callback = $callback;
        $this->inputMapperClass = $inputMapperClass;
        return $this;
    }

    public function setLogger(LoggerInterface $logger): void {
        $this->logger = $logger;
    }

    public function setFailedTopicName(?string $failedTopicName): void {
        $this->failedTopicName = $failedTopicName;
    }

    public function setMaximumNumberOfProcessedMessages(int $maximumNumberOfProcessedMessages): void {
        $this->maximumNumberOfProcessedMessages = $maximumNumberOfProcessedMessages;
    }

}
