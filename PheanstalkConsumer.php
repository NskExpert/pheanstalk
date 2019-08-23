<?php

declare(strict_types=1);

namespace Enqueue\Pheanstalk;

use Interop\Queue\Consumer;
use Interop\Queue\Exception\InvalidMessageException;
use Interop\Queue\Message;
use Interop\Queue\Queue;
use LogicException;
use Pheanstalk\Exception\DeadlineSoonException;
use Pheanstalk\Job;
use Pheanstalk\Pheanstalk;

class PheanstalkConsumer implements Consumer
{
    /**
     * @var PheanstalkDestination
     */
    private $destination;

    /**
     * @var Pheanstalk
     */
    private $pheanstalk;

    /**
     * PheanstalkConsumer constructor.
     * @param PheanstalkDestination $destination
     * @param Pheanstalk $pheanstalk
     */
    public function __construct(PheanstalkDestination $destination, Pheanstalk $pheanstalk)
    {
        $this->destination = $destination;
        $this->pheanstalk = $pheanstalk;
    }

    /**
     * @return PheanstalkDestination
     */
    public function getQueue(): Queue
    {
        return $this->destination;
    }

    /**
     * @param int $timeout
     * @return PheanstalkMessage
     * @throws DeadlineSoonException
     */
    public function receive(int $timeout = 0): ?Message
    {
        if (0 === $timeout) {
            while (true) {
                if ($job = $this->reserveFromTube($this->destination->getName(), 5)) {
                    return $this->convertJobToMessage($job);
                }
            }
        } else {
            if ($job = $this->reserveFromTube($this->destination->getName(), $timeout / 1000)) {
                return $this->convertJobToMessage($job);
            }
        }

        return null;
    }

    /**
     * @return PheanstalkMessage
     * @throws DeadlineSoonException
     */
    public function receiveNoWait(): ?Message
    {
        if ($job = $this->reserveFromTube($this->destination->getName(), 0)) {
            return $this->convertJobToMessage($job);
        }

        return null;
    }

    /**
     * @param Message $message
     * @throws InvalidMessageException
     */
    public function acknowledge(Message $message): void
    {
        InvalidMessageException::assertMessageInstanceOf($message, PheanstalkMessage::class);

        /** @noinspection PhpUndefinedMethodInspection */
        if (false == $message->getJob()) {
            throw new LogicException('The message could not be acknowledged because it does not have job set.');
        }

        /** @noinspection PhpUndefinedMethodInspection */
        $this->pheanstalk->delete($message->getJob());
    }

    /**
     * @param Message $message
     * @param bool $requeue
     * @throws InvalidMessageException
     */
    public function reject(Message $message, bool $requeue = false): void
    {
        InvalidMessageException::assertMessageInstanceOf($message, PheanstalkMessage::class);

        /** @noinspection PhpUndefinedMethodInspection */
        if (false == $message->getJob()) {
            throw new LogicException(sprintf(
                'The message could not be %s because it does not have job set.',
                $requeue ? 'requeued' : 'rejected'
            ));
        }

        if ($requeue) {
            /** @noinspection PhpUndefinedMethodInspection */
            $this->pheanstalk->release($message->getJob(), $message->getPriority(), $message->getDelay());

            return;
        }

        /** @noinspection PhpUndefinedMethodInspection */
        $this->pheanstalk->bury($message->getJob());
    }

    /**
     * @param Job $job
     * @return PheanstalkMessage
     */
    private function convertJobToMessage(Job $job): PheanstalkMessage
    {
        $stats = $this->pheanstalk->statsJob($job);

        $message = PheanstalkMessage::jsonUnserialize($job->getData());
        $message->setRedelivered($stats['reserves'] > 1);
        $message->setJob($job);

        return $message;
    }

    /**
     * @param string $tubeName
     * @param int $timeout
     * @return Job|null
     * @throws DeadlineSoonException
     */
    private function reserveFromTube(string $tubeName, int $timeout): ?Job
    {
        $this->pheanstalk->watchOnly($tubeName);
        return $this->pheanstalk->reserveWithTimeout($timeout);
    }
}
