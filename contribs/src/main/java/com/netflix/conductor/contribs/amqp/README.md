This is a simple copy of the Kafka Publish Task that works with the AMQP implementation of RabbitMQ.

The task definition will look something like this:


'''{
            "name": "amqp_task_name",
            "taskReferenceName": "amqp_task_name",
            "inputParameters": {
                "amqp_request": {
                    "queue": "rabbit_queue_name",
                    "value": "some kind of string value here",
                    "hosts": "ip address of rabbit server"
                }

            },
            "type": "AMQP_PUBLISH",
            "startDelay": 0,
            "optional": False
        }'''
