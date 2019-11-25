package com.netflix.conductor.contribs.amqp;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.netflix.conductor.core.execution.WorkflowExecutorModule;
import com.netflix.conductor.service.TaskService;
import com.netflix.conductor.service.TaskServiceImpl;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

public class AMQPWaitListener {

	public AMQPWaitListener() {

		Injector injector = Guice.createInjector(new WorkflowExecutorModule());
		TaskService taskService = injector.getInstance(TaskServiceImpl.class);
		Thread listener = new Thread(new Listener());
		listener.start();

	}

	class Listener implements Runnable {

		@Override
		public void run() {

			try {

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
