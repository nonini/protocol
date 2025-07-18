<?php

namespace nonini\Protocol\Generic;

use Evenement\EventEmitterTrait;
use Exception;
use React\Stream\DuplexStreamInterface;
use React\Stream\ReadableStreamInterface;
use React\Stream\Util;
use React\Stream\WritableStreamInterface;
use RuntimeException;

abstract class AbstractStreamWrapper implements DuplexStreamInterface
{
    use EventEmitterTrait;

    protected ReadableStreamInterface $input;
    protected WritableStreamInterface $output;

    private bool $closed = false;

    public function __construct(ReadableStreamInterface $in, ?WritableStreamInterface $out = null)
    {
        $this->readFrom($in);
        if ($out === null && $in instanceof WritableStreamInterface) {
            $this->writeTo($in);
        } elseif ($out !== null) {
            $this->writeTo($out);
        } else {
            throw new \InvalidArgumentException('Either $in must be a writable stream, or $out must be given');
        }
    }

    abstract public function handleData(string $data): void;

    protected function readFrom(ReadableStreamInterface $input): void
    {
        $this->input = $input;
        if (! $input->isReadable()) {
            $this->close();
            return;
        }
        $input->on('data', function ($data) {
            $this->handleData($data);
        });
        $input->on('end', function () {
            $this->handleEnd();
        });
        $input->on('close', function () {
            $this->close();
        });
        $input->on('error', function (Exception $error) {
            $this->handleError($error);
        });
    }

    protected function writeTo(WritableStreamInterface $output): void
    {
        $this->output = $output;
        if (! $this->output->isWritable()) {
            $this->close();
            throw new RuntimeException('Cannot write to output');
        }

        $output->on('drain', function () {
            $this->handleDrain();
        });
        $output->on('close', function () {
            $this->close();
        });
        $output->on('error', function (Exception $error) {
            $this->handleError($error);
        });
    }

    protected function handleDrain(): void
    {
        $this->emit('drain');
    }

    protected function handleEnd(): void
    {
        if (! $this->closed) {
            $this->emit('end');
            $this->close();
        }
    }

    public function isReadable(): bool
    {
        return !$this->closed && $this->input->isReadable();
    }

    public function isWritable(): bool
    {
        return !$this->closed && $this->output->isWritable();
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        $this->closed = true;
        $this->input->close();
        $this->output->close();

        $this->emit('close');
        $this->removeAllListeners();
    }

    public function pause(): void
    {
        $this->input->pause();
    }

    public function resume(): void
    {
        $this->input->resume();
    }

    /**
     * Hint: I've seen nothing but end => true/false for $options
     *
     * @param array<string, scalar> $options
     */
    public function pipe(WritableStreamInterface $dest, array $options = []): WritableStreamInterface
    {
        Util::pipe($this, $dest, $options);

        return $dest;
    }

    /**
     * @param Exception $error
     */
    protected function handleError(Exception $error): void
    {
        $this->emit('error', [$error]);
        $this->close();
    }
}
