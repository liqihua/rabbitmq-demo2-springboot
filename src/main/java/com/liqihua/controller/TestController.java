package com.liqihua.controller;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.ChannelAwareMessageListener;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/loginController")
public class TestController {
    @Resource
    private AmqpTemplate amqpTemplate;
    @Resource
    private RabbitTemplate rabbitTemplate;//RabbitTemplate继承AmqpTemplate，拥有更多方法
    @Resource
    private RabbitAdmin rabbitAdmin;
    @Resource
    private ConnectionFactory connectionFactory;


    /**
     * 使用RabbitAdmin可以动态创建、队列，不需要提现@Bean注入
     * @param aa
     * @return
     */
    @RequestMapping("/testRabbitAdmin")
    public String testRabbitAdmin(String aa){
        Queue queue = new Queue("queue-liqihua");
        rabbitAdmin.declareQueue(queue);
        DirectExchange ex = new DirectExchange("ex-liqihua");
        rabbitAdmin.declareExchange(ex);
        rabbitAdmin.declareBinding(BindingBuilder.bind(queue).to(ex).with("key-liqihua"));
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);//设置确认模式手工确认
        container.setMessageListener(new ChannelAwareMessageListener(){
            @Override
            public void onMessage(Message message, Channel channel) throws Exception {
                System.out.println(" [x] Received '" + new String(message.getBody()) + "' start id: "+Thread.currentThread().getId());
                System.out.println(" [x] Received '" + new String(message.getBody()) + "' end id: "+Thread.currentThread().getId());
                channel.basicAck(message.getMessageProperties().getDeliveryTag(), false); //确认消息成功消
            }
        });
        container.setQueueNames("queue-liqihua");
        container.start();
        amqpTemplate.convertSendAndReceive("ex-liqihua","key-liqihua","testRabbitAdmin -- "+aa);
        return "testRabbitAdmin ok";
    }


    /**
     * 一条消息发送给一个队列，一个队列可以有多个消费者，但每条消息只能被一个消费者消费一次
     * @return
     */
    @RequestMapping(value = "/testA")
    public String testA(){
        //把消息指定发送给队列queueA
        amqpTemplate.convertAndSend("queueA","msgA-"+System.currentTimeMillis());
        return "testA ok";
    }


    /**
     * faout交换机，routingKey不起作用，消息发给交换机，交换机可以绑定多条队列，每条队列可以有多个消费者，但每个队列的一条消息只能被一个消费者消费一次
     * @return
     */
    @RequestMapping(value = "/testB")
    public String testB(){
        //把消息指定发送给交换机fanoutExchange-queueB，参数（交换机名称，路由键值，消息），fanout类型交换机路由键值填空
        amqpTemplate.convertAndSend("fanoutExchange-queueB", "", "msgB-"+System.currentTimeMillis());
        return "testB ok";
    }


    /**
     * faout交换机，routingKey起作用，消息发给交换机，交换机可以绑定多条队列，每条队列可以有多个消费者，但每个队列的一条消息只能被一个消费者消费一次
     * @return
     */
    @RequestMapping(value = "/testDirect")
    public String testDirect(){
        amqpTemplate.convertAndSend("directExchange-queueDirect", "direct-liqihua", "msgDirect-"+System.currentTimeMillis());
        return "testDirect ok";
    }

    /**
     * topic类型交换机
     * @return
     */
    @RequestMapping(value = "/testC")
    public String testC(){
        //把消息指定发送给交换机topicExchange-queueC，参数（交换机名称，路由键值，消息）
        amqpTemplate.convertAndSend("topicExchange-queueC", "aa.ERROR", "msgC-aabbcc-"+System.currentTimeMillis());//消息匹配给路由键值aa.ERROR
        amqpTemplate.convertAndSend("topicExchange-queueC", "aa.INFO", "msgC-qqwwee-"+System.currentTimeMillis());//消息匹配给路由键值aa.INFO
        amqpTemplate.convertAndSend("topicExchange-queueC", "aa.abcd", "msgC-abcd-"+System.currentTimeMillis());//消息匹配给路由键值aa.abcd
        return "testC ok";
    }


    /**
     * 发送消息同时接收返回对象
     * @return
     */
    @RequestMapping(value = "/testD")
    public String testD(){
        String msg = "msgA-"+System.currentTimeMillis();
        Object obj = rabbitTemplate.convertSendAndReceive("queueD", msg);
        System.out.println("--- testD obj : "+obj.toString());
        return "testD ok";
    }

    /**
     * 发送消息同时接收返回对象-发送对象消息
     * @return
     */
    @RequestMapping(value = "/testE")
    public String testE(){
        Map<String,Object> map = new HashMap<String,Object>();
        map.put("abc", "value-abc");
        Object obj = rabbitTemplate.convertSendAndReceive("queueE", map);
        System.out.println("--- testE finished. obj : "+obj.toString());
        return "testE ok";
    }


}
