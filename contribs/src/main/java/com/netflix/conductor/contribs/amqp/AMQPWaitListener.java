package com.netflix.conductor.contribs.amqp;

import com.google.inject.Inject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.netflix.conductor.client.http.TaskClient;

public class AMQPWaitListener {

	

	public AMQPWaitListener() {

		Thread listener = new Thread(new Listener());
		listener.start();

	}

	class Listener implements Runnable {

		@Override
		public void run() {

			try {

				TaskClient taskClient = new TaskClient();
				taskClient.setRootURI("http://localhost:8080/api/");

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
