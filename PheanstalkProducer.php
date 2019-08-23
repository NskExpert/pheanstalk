<?php

declare(strict_types=1);

namespace Enqueue\Pheanstalk;

use Interop\Queue\Destination;
use Interop\Queue\Exception\InvalidDestinationException;
use Interop\Queue\Exception\InvalidMessageException;
use Interop\Queue\Exception\PriorityNotSupportedException;
use Interop\Queue\Message;
use Interop\Queue\Producer;
use InvalidArgumentException;
use LogicException;
use Pheanstalk\Pheanstalk;

/**
 * Class PheanstalkProducer
 * @package Enqueue\Pheanstalk
 */
class PheanstalkProducer implements Producer
{
    /**
     * @var Pheanstalk
     */
    private $pheanstalk;

    /**
     * PheanstalkProducer constructor.
     * @param Pheanstalk $pheanstalk
     */
    public function __construct(Pheanstalk $pheanstalk)
    {
        $this->pheanstalk = $pheanstalk;
    }

    /**
     * @param Destination $destination
     * @param Message $message
     * @throws InvalidDestinationException
     * @throws InvalidMessageException
     */
    public function send(Destination $destination, Message $message): void
    {
        InvalidDestinationException::assertDestinationInstanceOf($destination, PheanstalkDestination::class);
        InvalidMessageException::assertMessageInstanceOf($message, PheanstalkMessage::class);

        $rawMessage = json_encode($message);
        if (JSON_ERROR_NONE !== json_last_error()) {
            throw new InvalidArgumentException(sprintf(
                'Could not encode value into json. Error %s and message %s',
                json_last_error(),
                json_last_error_msg()
            ));
        }

        /** @noinspection PhpUndefinedMethodInspection */
        $this->pheanstalk->useTube($destination->getName())->put(
            $rawMessage,
            $message->getPriority(),
            $message->getDelay(),
            $message->getTimeToRun()
        );
    }

    /**
     * @param int|null $deliveryDelay
     * @return PheanstalkProducer
     */
    public function setDeliveryDelay(int $deliveryDelay = null): Producer
    {
        if (null === $deliveryDelay) {
            return $this;
        }

        throw new LogicException('Not implemented');
    }

    /**
     * @return int|null
     */
    public function getDeliveryDelay(): ?int
    {
        return null;
    }

    /**
     * @param int|null $priority
     * @return PheanstalkProducer
     * @throws PriorityNotSupportedException
     */
    public function setPriority(int $priority = null): Producer
    {
        if (null === $priority) {
            return $this;
        }

        throw PriorityNotSupportedException::providerDoestNotSupportIt();
    }

    /**
     * @return int|null
     */
    public function getPriority(): ?int
    {
        return null;
    }

    /**
     * @param int|null $timeToLive
     * @return Producer
     */
    public function setTimeToLive(int $timeToLive = null): Producer
    {
        if (null === $timeToLive) {
            return $this;
        }

        throw new LogicException('Not implemented');
    }

    /**
     * @return int|null
     */
    public function getTimeToLive(): ?int
    {
        return null;
    }
}
