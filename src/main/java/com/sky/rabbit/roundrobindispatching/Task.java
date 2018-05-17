package com.sky.rabbit.roundrobindispatching;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.TimeoutException;

/**
 * 消息消费者
 */
public class Task {
    //队列名称
    private final static String QUEUE_NAME = "round-robin";

    public static void main(String[] argv) throws IOException {
        try {
            //创建连接连接到MabbitMQ
            ConnectionFactory factory = new ConnectionFactory();
            //设置MabbitMQ所在主机ip或者主机名
            factory.setHost("localhost");
            //创建一个连接
            Connection connection = factory.newConnection();
            //创建一个频道
            Channel channel = connection.createChannel();
            //指定一个队列
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);
            //定义8个任务
            String[] tasks = {"task1", "task2", "task3", "task4", "task5", "task6", "task7", "task8"};
            System.out.println("start to dispatch tasks...");
            //分发任务
            for (String task : tasks) {
                //消息持久化
                channel.basicPublish("", QUEUE_NAME, MessageProperties.PERSISTENT_TEXT_PLAIN, task.getBytes());
            }
            System.out.println("end to dispatch tasks...");
            //关闭频道和连接
            channel.close();
            connection.close();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}
