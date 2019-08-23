<?php

declare(strict_types=1);

namespace Enqueue\Pheanstalk;

use Interop\Queue\Queue;
use Interop\Queue\Topic;

class PheanstalkDestination implements Queue, Topic
{
    /**
     * @var string
     */
    private $destinationName;

    /**
     * PheanstalkDestination constructor.
     * @param string $destinationName
     */
    public function __construct(string $destinationName)
    {
        $this->destinationName = $destinationName;
    }

    /**
     * @return string
     */
    public function getName(): string
    {
        return $this->destinationName;
    }

    /**
     * @return string
     */
    public function getQueueName(): string
    {
        return $this->destinationName;
    }

    /**
     * @return string
     */
    public function getTopicName(): string
    {
        return $this->destinationName;
    }
}
