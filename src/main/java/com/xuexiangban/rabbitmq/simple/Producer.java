package com.xuexiangban.rabbitmq.simple;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 生产者
 * 简单模式
 * 1. Producer -> 消息队列 -> Consumer
 * 官方7种模式：
 * <a href="https://www.rabbitmq.com/getstarted.html"></a>
 * 官方简单模式 "Hello World!"图解：
 * <a href="https://www.rabbitmq.com/tutorials/tutorial-one-python.html"></a>
 *
 * @author caoweiquan
 * @date 2021/3/18
 */
public class Producer {

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
            // 2. 创建连接 Connection，rabbitmq为什么不基于连接去处理通道？长连接-信道channel
            connection = connectionFactory.newConnection("生成者");
            // 3. 通过连接获取通道 Channel
            channel = connection.createChannel();
            // 4. 通过创建交换机，声明队列，绑定关系，路由key，发送消息，和接收消息
            String queueName = "queue1";

            // @params1 队列的名称
            // @params2 是否要持久化 durable，持久化就是是否存盘，如果false非持久化，true持久化？非持久化是否存盘，会存盘，但是随着服务重启丢失
            // @params3 排他性，是否是一个独占队列
            // @params4 是否自动删除，随着最后一个消费者消费完毕之后，是否删除
            // @params5 携带一些附加参数
            channel.queueDeclare(queueName, false, false, false, null);
            // 5. 准备消息内容
            String message = "Hello xuexiangban!!!";
            // 6. 发送消息对队列queue
            // @params1 交换机 @params2 队列、路由key @params3 消息控制状态 @params4 消息内容
            // 面试题：可以存在没有交换机的队列吗？不可能，会存在一个默认的direct交换机
            channel.basicPublish("", queueName, null, message.getBytes());

            System.out.println("消息发送成功");
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
