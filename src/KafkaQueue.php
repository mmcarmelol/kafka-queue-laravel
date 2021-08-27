<?php

namespace Kafka;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;

class KafkaQueue extends Queue implements QueueContract
{
    protected $consumer, $producer;

    public function __construct($producer, $consumer)
    {
        $this->producer = $producer;
        $this->consumer = $consumer;

    }

    public function size($queue = null)
    {
        // Todo: Implement size() method.
    }

    public function push($job, $data = '', $queue = null)
    {
        // Todo: Implement size() method.
        $topic = $this->producer->newTopic($queue ?? env('KAFKA_QUEUE'));
        $topic->produce(RD_KAFKA_PARTITION_UA, 0, "Hello from the other app");
        $this->producer->flush(1000);

    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
        // Todo: Implement pushRaw() method.
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        // Todo: Implement later() method.
    }

    public function pop($queue = null)
    {
        // Todo: Implement pop() method.
        // var_dump('queue is running');

        $this->consumer->subscribe([$queue]);
        
        try {
            $message = $this->consumer->consume(120 * 1000);

            switch ($message->err){
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $job = unserialize($message->payload);
                    $job->handle();

                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    var_dump("No more messages; will wait for more \n");
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    var_dump("Timme Out\n");
                    break;
                dafault:
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }
        } catch (\Exception $e) {
            var_dump($e->getMessage());
        }       
    }
}
