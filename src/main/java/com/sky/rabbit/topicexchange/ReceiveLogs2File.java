package com.sky.rabbit.topicexchange;

import com.rabbitmq.client.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeoutException;

public class ReceiveLogs2File {
    private static final String EXCHANGE_NAME = "topic_logs";

    public static void main(String[] argv) throws IOException, InterruptedException {
        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost("127.0.0.1");
            // 打开连接和创建频道，与发送端一样
            Connection connection = null;
            try {
                connection = factory.newConnection();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
            final Channel channel;
            channel = connection.createChannel();
            channel.exchangeDeclare(EXCHANGE_NAME, "topic");
            // 声明一个随机队列
            String queueName = channel.queueDeclare().getQueue();
            channel.queueBind(queueName, EXCHANGE_NAME, "");

            String severity = "kern.error";//只关注核心错误级别的日志，然后记录到文件中去。
            channel.queueBind(queueName, EXCHANGE_NAME, severity);

            System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

            // 创建队列消费者
            final Consumer consumer = new DefaultConsumer(channel) {
                @Override
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    String message = new String(body, "UTF-8");
                    //记录日志到文件：
                    print2File("[" + envelope.getRoutingKey() + "] " + message);
                }
            };
            channel.basicConsume(queueName, true, consumer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void print2File(String msg) {
        try {
            String dir = ReceiveLogs2File.class.getClassLoader().getResource("").getPath();
            String logFileName = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            File file = new File(dir, logFileName + ".log");
            FileOutputStream fos = new FileOutputStream(file, true);
            fos.write((new SimpleDateFormat("HH:mm:ss").format(new Date()) + " - " + msg + "\r\n").getBytes());
            fos.flush();
            fos.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
