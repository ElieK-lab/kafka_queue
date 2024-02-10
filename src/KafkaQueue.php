<?php

namespace Kafka;

use Exception;
use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;

class KafkaQueue extends Queue implements QueueContract
{

    protected $consumer, $producer;


    public function __construct($producer, $consumer)
    {
        $this->consumer = $consumer;
        $this->producer = $producer;
    }

    public function size($queue = null)
    {
    }

    public function push($job, $data = '', $queue = null)
    {
        // try {
        $topic = $this->producer->newTopic($queue ?? env('KAFKA_QUEUE'));

        $topic->produce(RD_KAFKA_PARTITION_UA, 0, serialize($job));

        $this->producer->flush(10000);
        // } catch (Exception $e) {
        //     var_dump($e->getMessage());
        // }
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
    }
    public function later($delay, $job, $data = '', $queue = null)
    {
    }
    public function pop($queue = null)
    {
        $this->consumer->subscribe([$queue]);
        try {
            $message = $this->consumer->consume(120 * 1000);

            switch ($message->err) {
                case \RD_KAFKA_RESP_ERR_NO_ERROR:

                    var_dump($message->payload);
                    $job = unserialize($message->payload);

                    if ($job instanceof OrderCompleted) {
                        var_dump('the job is instance of OrderCompleted');
                        $job->handle();
                    }

                    break;
                case \RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    var_dump("no more messages : will await for more\n");
                    break;
                case \RD_KAFKA_RESP_ERR__TIMED_OUT:
                    var_dump("Timed out\n");
                default:
                    throw new Exception($message->errstr(), $message->err);
                    break;
            }
        } catch (Exception $e) {
            var_dump('error ' . $e->getMessage());
        }
    }
}
