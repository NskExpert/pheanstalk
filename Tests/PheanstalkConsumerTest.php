<?php

namespace Enqueue\Pheanstalk\Tests;

use Enqueue\Pheanstalk\PheanstalkConsumer;
use Enqueue\Pheanstalk\PheanstalkDestination;
use Enqueue\Pheanstalk\PheanstalkMessage;
use Enqueue\Test\ClassExtensionTrait;
use Interop\Queue\Exception\InvalidMessageException;
use LogicException;
use Pheanstalk\Exception\DeadlineSoonException;
use Pheanstalk\Job;
use Pheanstalk\Pheanstalk;
use PHPUnit\Framework\TestCase;
use PHPUnit_Framework_MockObject_MockObject;

class PheanstalkConsumerTest extends TestCase
{
    use ClassExtensionTrait;

    const THE_JOB_ID = 123;

    public function testCouldBeConstructedWithDestinationAndPheanstalkAsArguments()
    {
        new PheanstalkConsumer(
            new PheanstalkDestination('aQueueName'),
            $this->createPheanstalkMock()
        );
    }

    public function testShouldReturnQueueSetInConstructor()
    {
        $destination = new PheanstalkDestination('aQueueName');

        $consumer = new PheanstalkConsumer(
            $destination,
            $this->createPheanstalkMock()
        );

        $this->assertSame($destination, $consumer->getQueue());
    }

    /**
     * @throws DeadlineSoonException
     */
    public function testShouldReceiveFromQueueAndReturnNullIfNoMessageInQueue()
    {
        $destination = new PheanstalkDestination('theQueueName');

        $pheanstalk = $this->createPheanstalkMock();
        $pheanstalk
            ->expects($this->once())
            ->method('reserveWithTimeout')
            ->with(1)
            ->willReturn(null)
        ;

        $consumer = new PheanstalkConsumer($destination, $pheanstalk);

        $this->assertNull($consumer->receive(1000));
    }

    /**
     * @throws DeadlineSoonException
     */
    public function testShouldReceiveFromQueueAndReturnMessageIfMessageInQueue()
    {
        $destination = new PheanstalkDestination('theQueueName');
        $message = new  PheanstalkMessage('theBody', ['foo' => 'fooVal'], ['bar' => 'barVal']);

        $job = new Job(self::THE_JOB_ID, json_encode($message));

        $pheanstalk = $this->createPheanstalkMock();
        $pheanstalk
            ->expects($this->once())
            ->method('reserveWithTimeout')
            ->with(1)
            ->willReturn($job)
        ;

        $consumer = new PheanstalkConsumer($destination, $pheanstalk);

        $actualMessage = $consumer->receive(1000);

        $this->assertSame('theBody', $actualMessage->getBody());
        $this->assertSame(['foo' => 'fooVal'], $actualMessage->getProperties());
        $this->assertSame(['bar' => 'barVal'], $actualMessage->getHeaders());
        $this->assertSame($job, $actualMessage->getJob());
    }

    /**
     * @throws DeadlineSoonException
     */
    public function testShouldReceiveNoWaitFromQueueAndReturnNullIfNoMessageInQueue()
    {
        $destination = new PheanstalkDestination('theQueueName');

        $pheanstalk = $this->createPheanstalkMock();
        $pheanstalk
            ->expects($this->once())
            ->method('reserveWithTimeout')
            ->with(0)
            ->willReturn(null)
        ;

        $consumer = new PheanstalkConsumer($destination, $pheanstalk);

        $this->assertNull($consumer->receiveNoWait());
    }

    /**
     * @throws DeadlineSoonException
     */
    public function testShouldReceiveNoWaitFromQueueAndReturnMessageIfMessageInQueue()
    {
        $destination = new PheanstalkDestination('theQueueName');
        $message = new PheanstalkMessage('theBody', ['foo' => 'fooVal'], ['bar' => 'barVal']);

        $job = new Job(self::THE_JOB_ID, json_encode($message));

        $pheanstalk = $this->createPheanstalkMock();
        $pheanstalk
            ->expects($this->once())
            ->method('reserveWithTimeout')
            ->with(0)
            ->willReturn($job)
        ;

        $consumer = new PheanstalkConsumer($destination, $pheanstalk);

        $actualMessage = $consumer->receiveNoWait();

        $this->assertSame('theBody', $actualMessage->getBody());
        $this->assertSame(['foo' => 'fooVal'], $actualMessage->getProperties());
        $this->assertSame(['bar' => 'barVal'], $actualMessage->getHeaders());
        $this->assertSame($job, $actualMessage->getJob());
    }

    /**
     * @throws InvalidMessageException
     */
    public function testShouldAcknowledgeMessage()
    {
        $destination = new PheanstalkDestination('theQueueName');
        $message = new PheanstalkMessage();

        $job = new Job(self::THE_JOB_ID, json_encode($message));
        $message->setJob($job);

        $pheanstalk = $this->createPheanstalkMock();
        $pheanstalk
            ->expects($this->once())
            ->method('delete')
            ->with($job)
        ;

        $consumer = new PheanstalkConsumer($destination, $pheanstalk);

        $consumer->acknowledge($message);
    }

    /**
     * @throws InvalidMessageException
     */
    public function testAcknowledgeShouldThrowExceptionIfMessageHasNoJob()
    {
        $destination = new PheanstalkDestination('theQueueName');
        $pheanstalk = $this->createPheanstalkMock();

        $consumer = new PheanstalkConsumer($destination, $pheanstalk);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('The message could not be acknowledged because it does not have job set.');

        $consumer->acknowledge(new PheanstalkMessage());
    }

    /**
     * @throws InvalidMessageException
     */
    public function testShouldRejectMessage()
    {
        $destination = new PheanstalkDestination('theQueueName');
        $message = new PheanstalkMessage();

        $job = new Job(self::THE_JOB_ID, json_encode($message));
        $message->setJob($job);

        $pheanstalk = $this->createPheanstalkMock();
        $pheanstalk
            ->expects($this->once())
            ->method('bury')
            ->with($job)
        ;

        $consumer = new PheanstalkConsumer($destination, $pheanstalk);

        $consumer->reject($message);
    }

    /**
     * @throws InvalidMessageException
     */
    public function testRejectShouldThrowExceptionIfMessageHasNoJob()
    {
        $destination = new PheanstalkDestination('theQueueName');
        $pheanstalk = $this->createPheanstalkMock();

        $consumer = new PheanstalkConsumer($destination, $pheanstalk);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('The message could not be rejected because it does not have job set.');

        $consumer->reject(new PheanstalkMessage());
    }

    /**
     * @throws InvalidMessageException
     */
    public function testShouldRequeueMessage()
    {
        $destination = new PheanstalkDestination('theQueueName');
        $message = new PheanstalkMessage();

        $job = new Job(self::THE_JOB_ID, json_encode($message));
        $message->setJob($job);

        $pheanstalk = $this->createPheanstalkMock();
        $pheanstalk
            ->expects($this->once())
            ->method('release')
            ->with($job, Pheanstalk::DEFAULT_PRIORITY, Pheanstalk::DEFAULT_DELAY)
        ;
        $pheanstalk
            ->expects($this->never())
            ->method('delete')
        ;

        $consumer = new PheanstalkConsumer($destination, $pheanstalk);

        $consumer->reject($message, true);
    }

    /**
     * @throws InvalidMessageException
     */
    public function testRequeueShouldThrowExceptionIfMessageHasNoJob()
    {
        $destination = new PheanstalkDestination('theQueueName');
        $pheanstalk = $this->createPheanstalkMock();

        $consumer = new PheanstalkConsumer($destination, $pheanstalk);

        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('The message could not be requeued because it does not have job set.');

        $consumer->reject(new PheanstalkMessage(), true);
    }

    /**
     * @return PHPUnit_Framework_MockObject_MockObject|Pheanstalk
     */
    private function createPheanstalkMock()
    {
        return $this->createMock(Pheanstalk::class);
    }
}
