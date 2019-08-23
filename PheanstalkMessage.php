<?php

declare(strict_types=1);

namespace Enqueue\Pheanstalk;

use Interop\Queue\Message;
use InvalidArgumentException;
use JsonSerializable;
use Pheanstalk\Job;
use Pheanstalk\Pheanstalk;

/**
 * Class PheanstalkMessage
 * @package Enqueue\Pheanstalk
 */
class PheanstalkMessage implements Message, JsonSerializable
{
    /**
     * @var string
     */
    private $body;

    /**
     * @var array
     */
    private $properties;

    /**
     * @var array
     */
    private $headers;

    /**
     * @var bool
     */
    private $redelivered;

    /**
     * @var Job
     */
    private $job;

    /**
     * PheanstalkMessage constructor.
     * @param string $body
     * @param array $properties
     * @param array $headers
     */
    public function __construct(string $body = '', array $properties = [], array $headers = [])
    {
        $this->body = $body;
        $this->properties = $properties;
        $this->headers = $headers;
        $this->redelivered = false;
    }

    /**
     * @param string $body
     */
    public function setBody(string $body): void
    {
        $this->body = $body;
    }

    /**
     * @return string
     */
    public function getBody(): string
    {
        return $this->body;
    }

    /**
     * @param array $properties
     */
    public function setProperties(array $properties): void
    {
        $this->properties = $properties;
    }

    /**
     * @return array
     */
    public function getProperties(): array
    {
        return $this->properties;
    }

    /**
     * @param string $name
     * @param $value
     */
    public function setProperty(string $name, $value): void
    {
        $this->properties[$name] = $value;
    }

    /**
     * @param string $name
     * @param null $default
     * @return mixed|null
     */
    public function getProperty(string $name, $default = null)
    {
        return array_key_exists($name, $this->properties) ? $this->properties[$name] : $default;
    }

    /**
     * @param array $headers
     */
    public function setHeaders(array $headers): void
    {
        $this->headers = $headers;
    }

    /**
     * @return array
     */
    public function getHeaders(): array
    {
        return $this->headers;
    }

    /**
     * @param string $name
     * @param $value
     */
    public function setHeader(string $name, $value): void
    {
        $this->headers[$name] = $value;
    }

    /**
     * @param string $name
     * @param null $default
     * @return mixed|null
     */
    public function getHeader(string $name, $default = null)
    {
        return array_key_exists($name, $this->headers) ? $this->headers[$name] : $default;
    }

    /**
     * @return bool
     */
    public function isRedelivered(): bool
    {
        return $this->redelivered;
    }

    /**
     * @param bool $redelivered
     */
    public function setRedelivered(bool $redelivered): void
    {
        $this->redelivered = $redelivered;
    }

    /**
     * @param string|null $correlationId
     */
    public function setCorrelationId(string $correlationId = null): void
    {
        $this->setHeader('correlation_id', (string) $correlationId);
    }

    /**
     * @return string|null
     */
    public function getCorrelationId(): ?string
    {
        return $this->getHeader('correlation_id');
    }

    /**
     * @param string|null $messageId
     */
    public function setMessageId(string $messageId = null): void
    {
        $this->setHeader('message_id', (string) $messageId);
    }

    /**
     * @return string|null
     */
    public function getMessageId(): ?string
    {
        return $this->getHeader('message_id');
    }

    /**
     * @return int|null
     */
    public function getTimestamp(): ?int
    {
        $value = $this->getHeader('timestamp');

        return null === $value ? null : (int) $value;
    }

    /**
     * @param int|null $timestamp
     */
    public function setTimestamp(int $timestamp = null): void
    {
        $this->setHeader('timestamp', $timestamp);
    }

    /**
     * @param string|null $replyTo
     */
    public function setReplyTo(string $replyTo = null): void
    {
        $this->setHeader('reply_to', $replyTo);
    }

    /**
     * @return string|null
     */
    public function getReplyTo(): ?string
    {
        return $this->getHeader('reply_to');
    }

    /**
     * @param int $time
     */
    public function setTimeToRun(int $time)
    {
        $this->setHeader('ttr', $time);
    }

    /**
     * @return int
     */
    public function getTimeToRun(): int
    {
        /** @noinspection PhpStrictTypeCheckingInspection */
        return $this->getHeader('ttr', Pheanstalk::DEFAULT_TTR);
    }

    /**
     * @param int $priority
     */
    public function setPriority(int $priority): void
    {
        $this->setHeader('priority', $priority);
    }

    /**
     * @return int
     */
    public function getPriority(): int
    {
        /** @noinspection PhpStrictTypeCheckingInspection */
        return $this->getHeader('priority', Pheanstalk::DEFAULT_PRIORITY);
    }

    /**
     * @param int $delay
     */
    public function setDelay(int $delay): void
    {
        $this->setHeader('delay', $delay);
    }

    /**
     * @return int
     */
    public function getDelay(): int
    {
        /** @noinspection PhpStrictTypeCheckingInspection */
        return $this->getHeader('delay', Pheanstalk::DEFAULT_DELAY);
    }

    /**
     * @return array
     */
    public function jsonSerialize(): array
    {
        return [
            'body' => $this->getBody(),
            'properties' => $this->getProperties(),
            'headers' => $this->getHeaders(),
        ];
    }

    /**
     * @param string $json
     * @return PheanstalkMessage
     */
    public static function jsonUnserialize(string $json): self
    {
        $data = json_decode($json, true);
        if (JSON_ERROR_NONE !== json_last_error()) {
            throw new InvalidArgumentException(sprintf(
                'The malformed json given. Error %s and message %s',
                json_last_error(),
                json_last_error_msg()
            ));
        }

        return new self($data['body'], $data['properties'], $data['headers']);
    }

    /**
     * @return Job|null
     */
    public function getJob(): ?Job
    {
        return $this->job;
    }

    /**
     * @param Job|null $job
     */
    public function setJob(Job $job = null): void
    {
        $this->job = $job;
    }
}
