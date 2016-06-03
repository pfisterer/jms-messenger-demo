package messenger.version_1;

import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import util.CommonStuff;

public class SVA16WhatsAppClone {
	public static void main(String[] args) throws Exception {
		// Create a connection factory
		ActiveMQConnectionFactory connectionFactory = CommonStuff.setupAndGetConnectionFactory(args);
		Logger log = LoggerFactory.getLogger(SVA16WhatsAppClone.class);

		// Create a connection
		Connection connection = connectionFactory.createConnection();
		connection.start();

		// Create a session
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

		// Compute a topic name based on the members of the chat
		SortedSet<String> chatMembers = new TreeSet<>();
		chatMembers.add("Dennis");
		chatMembers.add("Bjarne");
		chatMembers.add("Kaspar");
		String topic = "chat-" + DigestUtils.md5Hex(chatMembers.stream().collect(Collectors.joining(", ")));
		log.info("Using topic name: {}", topic);
		Topic demoTopic = session.createTopic(topic);

		// Register some consumers
		List<MessageConsumer> consumers = new LinkedList<>();
		{
			MessageConsumer consumer = session.createConsumer(demoTopic);
			consumers.add(consumer);
			consumer.setMessageListener(message -> {
				try {
					TextMessage textMessage = (TextMessage) message;
					log.info("{}: {}", textMessage.getJMSMessageID(), textMessage.getText());
				} catch (Exception e) {
					log.warn("Exception: {}", e);
				}
			});
		}

		MessageProducer producer = session.createProducer(demoTopic);

		Scanner sysIn = new Scanner(System.in);
		for (String nextMessage = sysIn.nextLine(); !nextMessage.startsWith("exit"); nextMessage = sysIn.nextLine()) {
			log.info("Sending next message : {}", nextMessage);
			TextMessage message = session.createTextMessage(nextMessage);
			producer.send(message);
		}

		// Cleanup resources
		consumers.forEach(consumer -> {
			try {
				consumer.close();
			} catch (Exception e) {
			}
		});

		sysIn.close();
		session.close();
		connection.close();

	}
}
