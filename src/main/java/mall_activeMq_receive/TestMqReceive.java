package mall_activeMq_receive;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * //创建一个session  
//第一个参数:是否支持事务，如果为true，则会忽略第二个参数，被jms服务器设置为SESSION_TRANSACTED  
//第二个参数为false时，paramB的值可为Session.AUTO_ACKNOWLEDGE，Session.CLIENT_ACKNOWLEDGE，DUPS_OK_ACKNOWLEDGE其中一个。  
//Session.AUTO_ACKNOWLEDGE为自动确认，客户端发送和接收消息不需要做额外的工作。哪怕是接收端发生异常，也会被当作正常发送成功。  
//Session.CLIENT_ACKNOWLEDGE为客户端确认。客户端接收到消息后，必须调用javax.jms.Message的acknowledge方法。jms服务器才会当作发送成功，并删除消息。  
//DUPS_OK_ACKNOWLEDGE允许副本的确认模式。一旦接收方应用程序的方法调用从处理消息处返回，会话对象就会确认消息的接收；而且允许重复确认。  
 * @author Administrator
 *
 */
public class TestMqReceive {

	public static void main(String[] args) throws Exception {
		//MqReceiveQueue("tcp://localhost:61616","TestQueues");
		MqReceiveTopics("tcp://localhost:61616","TestTopics");
	}

	public static void MqReceiveQueue(String url, String application) throws Exception {

		// 創建連接对象
		ActiveMQConnectionFactory mqConnectionFactory = new ActiveMQConnectionFactory(url);
		Connection createConnection = mqConnectionFactory.createConnection();
		createConnection.start();

		// 根据连接对象床架会话对象
		Session createSession = createConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Queue createQueue = createSession.createQueue(application);

		MessageConsumer createConsumer = createSession.createConsumer(createQueue);

		// 设置监听器获取传递的信息
		createConsumer.setMessageListener(new MessageListener() {

			@Override
			public void onMessage(Message message) {
				// 使用TextMessage包装获取的消息
				TextMessage textMessage = (TextMessage) message;
				try {
					String text = textMessage.getText();
					System.out.println(text);
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
		});
		System.out.println("消费者1启动");
		System.in.read();
	}
	
	//Topic订阅方式的的消息队列
	public static void MqReceiveTopics(String url,String application) throws Exception {
		//创建连接对象
		ActiveMQConnectionFactory mqConnectionFactory = new ActiveMQConnectionFactory(url);
		Connection createConnection = mqConnectionFactory.createConnection();
		//手动实现持久化
		createConnection.setClientID("clientID");
		createConnection.start();
		
		//根据连接对象创建会话对象
		Session createSession = createConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Topic createTopic = createSession.createTopic(application);
		//通过客户端的id实现对客户端的消息队列的持久化
		TopicSubscriber createDurableSubscriber = createSession.createDurableSubscriber(createTopic, "clientID");
		
		//通过会话对象创建消费者
		//MessageConsumer createConsumer = createSession.createConsumer(createTopic);
		
		//设置监听器获取消息队列
		createDurableSubscriber.setMessageListener(new MessageListener() {
			
			@Override
			public void onMessage(Message message) {
				TextMessage textMassage = (TextMessage) message;
				try {
					String text = textMassage.getText();
					System.out.println(text);
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
		});
		System.out.println("订阅者1启动");
		System.in.read();
	}
}
