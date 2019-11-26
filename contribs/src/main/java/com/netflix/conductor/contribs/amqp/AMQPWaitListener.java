package com.netflix.conductor.contribs.amqp;

import com.google.inject.Inject;
import com.netflix.conductor.grpc.server.service.WorkflowServiceImpl;
import com.netflix.conductor.service.TaskServiceImpl;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class AMQPWaitListener {

	@Inject
	TaskServiceImpl taskService;

	@Inject
	WorkflowServiceImpl workflowService;

	public AMQPWaitListener() {

		Thread listener = new Thread(new Listener());
		listener.start();

	}

	class Listener implements Runnable {

		@Override
		public void run() {

			try {

				workflowService.hashCode();
				taskService.hashCode();

				ConnectionFactory factory = new ConnectionFactory();
				factory.setHost("rabbitmq-headless");
				factory.setUsername("conductor");
				factory.setPassword("conductor");
				Connection connection = factory.newConnection();
				Channel channel = connection.createChannel();

				channel.queueDeclare("conductor", false, false, false, null);
				System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

				DeliverCallback deliverCallback = (consumerTag, delivery) -> {
					String message = new String(delivery.getBody(), "UTF-8");
					System.out.println(" [x] Received '" + message + "'");
				};
				channel.basicConsume("conductor", true, deliverCallback, consumerTag -> {
				});

			} catch (Exception e) {
				e.printStackTrace();
			}

		}

	}

}
