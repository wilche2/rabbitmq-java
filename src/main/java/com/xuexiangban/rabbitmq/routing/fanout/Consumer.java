package com.xuexiangban.rabbitmq.routing.fanout;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 消费者
 *
 * @author caoweiquan
 * @date 2021/3/18
 */
public class Consumer {

    private static Runnable runnable = () -> {
        // 所有的中间件技术都是基于 TCP/IP 协议构建新型的协议规范
        // 只不过 rabbitmq 使用的是 amqp
        // ip port

        // 1. 创建连接工程
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("159.75.4.236");
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("admin");
        connectionFactory.setVirtualHost("/");

        final String queueName = Thread.currentThread().getName();

        Connection connection = null;
        Channel channel = null;
        try {
            // 2. 创建连接 Connection
            connection = connectionFactory.newConnection("生成者");
            // 3. 通过连接获取通道 Channel
            channel = connection.createChannel();

            // 接收消息
            // 4. 定义接受消息回调的主体
            Channel finalChannel = channel;
            finalChannel.basicConsume(queueName, true, new DeliverCallback() {
                @Override
                public void handle(String consumerTag, Delivery message) throws IOException {
                    System.out.println(queueName + "：收到消息是：" + new String(message.getBody(), "UTF-8"));
                }
            }, new CancelCallback() {
                @Override
                public void handle(String consumerTag) throws IOException {
                    System.out.println(queueName + "：接收失败了");
                }
            });

            System.out.println(queueName + "：开始接收消息");
            System.in.read();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 7. 关闭通道 Channel
            if (channel != null && channel.isOpen()) {
                try {
                    channel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
            }

            // 8. 关闭连接
            if (connection != null && connection.isOpen()) {
                try {
                    connection.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    };

    public static void main(String[] args) {
        new Thread(runnable, "queue1").start();
        new Thread(runnable, "queue2").start();
        new Thread(runnable, "queue3").start();
    }

}
