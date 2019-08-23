<?php

namespace Enqueue\Pheanstalk\Tests;

use Enqueue\Pheanstalk\PheanstalkMessage;
use Enqueue\Test\ClassExtensionTrait;
use Pheanstalk\Job;
use PHPUnit\Framework\TestCase;

class PheanstalkMessageTest extends TestCase
{
    use ClassExtensionTrait;

    const THE_JOB_ID = 123;

    public function testShouldAllowGetJobPreviouslySet()
    {
        $job = new Job(self::THE_JOB_ID, 'aData');

        $message = new PheanstalkMessage();
        $message->setJob($job);

        $this->assertSame($job, $message->getJob());
    }
}
