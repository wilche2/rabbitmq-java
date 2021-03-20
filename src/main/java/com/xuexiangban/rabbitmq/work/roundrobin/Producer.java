package com.xuexiangban.rabbitmq.work.roundrobin;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 简单队列生产者
 * <p>
 * work_轮询策略，自动应答：
 * 1. 当存在多个消费者接入时，消息的分配模式是一个消费者分配一条，直到消费完毕，按均分配
 *
 * @author caoweiquan
 * @date 2021/3/20 18:55
 */
public class Producer {

    public static void main(String[] args) {
        // 1. 创建连接工厂
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("159.75.4.236");
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("admin");
        connectionFactory.setPassword("admin");
        connectionFactory.setVirtualHost("/");

        Connection connection = null;
        Channel channel = null;
        try {
            // 2. 创建连接 Connection，rabbitmq为什么不基于连接去处理通道？长连接-信道channel
            connection = connectionFactory.newConnection("生产者");

            // 3. 通过连接获取通道 Channel
            channel = connection.createChannel();

            // 4. 通过创建交换机，声明队列，绑定关系，路由key，发送消息，和接收消息
            String queueName = "queue1";

            // 5. 发送消息对队列queue
            // 模拟环境生产者环境，发送多条记录
            for (int i = 0; i < 20; i++) {
                String message = "Hello wilche!!! = " + i;
                // 默认交换机的情况下，routeKey 添加队列名即可
                channel.basicPublish("", queueName, null, message.getBytes());
            }

            System.out.println("消息发送成功");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 6. 关闭通道 Channel
            if (channel != null && channel.isOpen()) {
                try {
                    channel.close();
                } catch (IOException | TimeoutException e) {
                    e.printStackTrace();
                }
            }

            // 7. 关闭连接
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
