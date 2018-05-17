package com.sky.rabbit.roundrobindispatching;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * 消息生产者
 */
public class Worker {
    private final static Random random = new Random();
    //队列名称
    private final static String QUEUE_NAME = "round-robin";

    public static void main(String[] argv) throws java.io.IOException {
        try {
            //创建连接连接到MabbitMQ
            ConnectionFactory factory = new ConnectionFactory();
            //设置MabbitMQ所在主机ip或者主机名
            factory.setHost("localhost");
            //创建一个连接
            Connection connection = factory.newConnection();
            //创建一个频道
            Channel channel = connection.createChannel();
            //boolean durable = true;
            //指定一个队列ss
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            //发送的消息
            System.out.println("waiting for task....");
            Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope,
                                           AMQP.BasicProperties properties, byte[] body)
                        throws IOException {
                    String message = new String(body, "UTF-8");
                    System.out.println("worker start to handler the task:" + message);
                    //模拟处理任务花费时间
                    handlerTask();
                    System.out.println("worker end to handler the task:" + message);
                }
            };
            boolean autoAck = true; // acknowledgment is covered below
            channel.basicConsume(QUEUE_NAME, autoAck, consumer);
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }

    private static void handlerTask() {
        try {
            TimeUnit.SECONDS.sleep(random.nextInt(5) + 1);
        } catch (InterruptedException e) {
            //ignore
        }
    }
}