<?php

namespace Enqueue\Pheanstalk\Tests;

use Enqueue\Null\NullQueue;
use Enqueue\Pheanstalk\PheanstalkContext;
use Enqueue\Test\ClassExtensionTrait;
use Interop\Queue\Context;
use Interop\Queue\Exception\InvalidDestinationException;
use Interop\Queue\Exception\TemporaryQueueNotSupportedException;
use Pheanstalk\Connection;
use Pheanstalk\Pheanstalk;
use PHPUnit\Framework\TestCase;
use PHPUnit_Framework_MockObject_MockObject;

class PheanstalkContextTest extends TestCase
{
    use ClassExtensionTrait;

    public function testShouldImplementContextInterface()
    {
        $this->assertClassImplements(Context::class, PheanstalkContext::class);
    }

    public function testCouldBeConstructedWithPheanstalkAsFirstArgument()
    {
        new PheanstalkContext($this->createPheanstalkMock());
    }

    /**
     * @throws TemporaryQueueNotSupportedException
     */
    public function testThrowNotImplementedOnCreateTemporaryQueue()
    {
        $context = new PheanstalkContext($this->createPheanstalkMock());

        $this->expectException(TemporaryQueueNotSupportedException::class);

        $context->createTemporaryQueue();
    }

    /**
     * @throws InvalidDestinationException
     */
    public function testThrowInvalidDestinationIfInvalidDestinationGivenOnCreateConsumer()
    {
        $context = new PheanstalkContext($this->createPheanstalkMock());

        $this->expectException(InvalidDestinationException::class);
        $context->createConsumer(new NullQueue('aQueue'));
    }

    public function testShouldAllowGetPheanstalkSetInConstructor()
    {
        $pheanstalk = $this->createPheanstalkMock();

        $context = new PheanstalkContext($pheanstalk);

        $this->assertSame($pheanstalk, $context->getPheanstalk());
    }

    public function testShouldDoConnectionDisconnectOnContextClose()
    {
        $connection = $this->createMock(Connection::class);
        $connection
            ->expects($this->never())
            ->method('disconnect');
    }

    /**
     * @return PHPUnit_Framework_MockObject_MockObject|Pheanstalk
     */
    private function createPheanstalkMock()
    {
        return $this->createMock(Pheanstalk::class);
    }
}
