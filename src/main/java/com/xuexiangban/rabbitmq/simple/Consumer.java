package com.xuexiangban.rabbitmq.simple;

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

    public static void main(String[] args) {
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

        Connection connection = null;
        Channel channel = null;
        try {
            // 2. 创建连接 Connection
            connection = connectionFactory.newConnection("生产者");
            // 3. 通过连接获取通道 Channel
            channel = connection.createChannel();

            String queueName = "queue1";
            // 接收消息
            channel.basicConsume(queueName, true, new DeliverCallback() {
                @Override
                public void handle(String consumerTag, Delivery message) throws IOException {
                    System.out.println("收到消息是：" + new String(message.getBody(), "UTF-8"));
                }
            }, new CancelCallback() {
                @Override
                public void handle(String consumerTag) throws IOException {
                    System.out.println("接收失败了");
                }
            });

            System.out.println("开始接收消息");
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
    }

}
