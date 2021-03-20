package com.xuexiangban.rabbitmq.routing.all;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * 生产者
 * <p>
 * 完整流程：
 * 1. 创建连接工程
 * 2. 创建连接
 * 3. 通过连接获取通道 Channel
 * 4. 使用 Channel 声明交换机、队列
 * 5. 使用 Channel 绑定交换机与队列的关系
 * 6. 向指定 routeKey、queue、arguments 发送消息
 * 7. 关闭通道
 * 8. 关闭连接
 * <p>
 * 官方7种模式：
 * <a href="https://www.rabbitmq.com/getstarted.html"></a>
 * 官方fanout图解：
 * <a href="https://www.rabbitmq.com/tutorials/tutorial-five-python.html"></a>
 * <p>
 * 目前的状态是已经在web端让exchange绑定了queue，所以代码中没有体现
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
            connection = connectionFactory.newConnection("生产者");

            // 3. 通过连接获取通道 Channel
            channel = connection.createChannel();

            // 通过创建交换机，声明队列，绑定关系，路由key，发送消息，和接收消息
            // 4. 使用 Channel 声明交换机、队列
            String exchangeName = "direct_order_test1";
            String exchangeType = "direct";
            channel.exchangeDeclare(exchangeName, exchangeType, true);
            // @params1 队列的名称
            // @params2 是否要持久化 durable，持久化就是是否存盘，如果false非持久化，true持久化？非持久化是否存盘，会存盘，但是随着服务重启丢失
            // @params3 排他性，是否是一个独占队列
            // @params4 是否自动删除，随着最后一个消费者消费完毕之后，是否删除
            // @params5 携带一些附加参数
            String queueName1 = "direct_order_1";
            String queueName2 = "direct_order_2";
            String queueName3 = "direct_order_3";
            channel.queueDeclare(queueName1, true, false, false, null);
            channel.queueDeclare(queueName2, true, false, false, null);
            channel.queueDeclare(queueName3, true, false, false, null);

            // 5. 使用 Channel 绑定交换机与队列的关系
            channel.queueBind(queueName1, exchangeName, "com");
            channel.queueBind(queueName2, exchangeName, "order");
            channel.queueBind(queueName3, exchangeName, "sister");

            // 6. 向指定 routeKey、queue、arguments 发送消息
            String routeKey = "order";
            String message = "曹威权是一个大帅逼!!!";
            channel.basicPublish(exchangeName, routeKey, null, message.getBytes());
            System.out.println("消息发送成功");
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("消息发送异常");
        } finally {
            // 8. 关闭通道 Channel
            if (channel != null && channel.isOpen()) {
                try {
                    channel.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (TimeoutException e) {
                    e.printStackTrace();
                }
            }

            // 9. 关闭连接
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
