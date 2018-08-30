package eu.unicorn.kafka;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;

import eu.unicorn.kafka.avro.User;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
public class KafkaProofOfConcept implements CommandLineRunner {

	private static final String KAFKA_TOPIC = "kafkaTest.normal";
	private static final String KAFKA_TOPIC_USER = "kafkaTest.user";
	private static final String KAFKA_TOPIC_LARGE = "kafkaTest.large";
	private static final int NORMAL_MESSAGES = 3;
	private static final int LARGE_MESSAGES = 1;
	private static final long WAIT_TIMEOUT_SECONDS = 10;

//	private static final int LARGE_MESSAGE_SIZE = 1024 * 1024 * 16;
	private static final int LARGE_MESSAGE_SIZE = 1024 * 16;

	@Autowired
	private KafkaTemplate<String, String> template;

	@Autowired
	private KafkaTemplate<String, User> userTemplate;

	private final CountDownLatch latch = new CountDownLatch(NORMAL_MESSAGES);
	private final CountDownLatch largeLatch = new CountDownLatch(LARGE_MESSAGES);

	@Override
	public void run(String... args) throws Exception {
		log.info("Sending.");

		sendUser();
		sendShortMessages();
		sendLongMessages();
        waitForReceivers();

		log.info("All received");
	}

	private void waitForReceivers() throws InterruptedException {
		this.latch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		this.largeLatch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
	}

	private void sendLongMessages() {
		Random r = new Random();

		for (int i = 0; i < LARGE_MESSAGES; i++) {
			log.info("Sending large message #{}", Integer.valueOf(i));
			try (IntStream is = r.ints(LARGE_MESSAGE_SIZE)) {
				StringBuilder sb = new StringBuilder(LARGE_MESSAGE_SIZE);
				sb.append(i);
				sb.append(": ");
				is.forEach(random -> sb.append((char) ((random & 63) + ' ')));

				this.template.send(KAFKA_TOPIC_LARGE, sb.toString());
			}
		}
	}

	private void sendShortMessages() {
		for (int i = 0; i < NORMAL_MESSAGES; i++) {
			log.info("Sending normal message #{}", Integer.valueOf(i));
			this.template.send(KAFKA_TOPIC, "normal message " + i);
		}
	}

	private void sendUser() {
		log.info("Sending user.");
		User user = new User("Ja Nevim", Integer.valueOf(13), "Modra");
		this.userTemplate.send(KAFKA_TOPIC_USER, user);
	}

	@KafkaListener(topics = KAFKA_TOPIC)
	public void listen(ConsumerRecord<String, String> cr) {
		log.info(cr.toString());
		this.latch.countDown();
		log.info("Remaining " + this.latch.getCount() + " normal messages.");
	}

	@SuppressWarnings("static-method")
	@KafkaListener(topics = KAFKA_TOPIC_USER, containerFactory = "userFactory")
	public void listenUser(ConsumerRecord<String, User> cr) {
		log.info("Received user: {}", cr.toString());
		User user = cr.value();
		log.info("User: {}, {}", user.getName(), user.getFavoriteNumber());

	}

	@KafkaListener(topics = KAFKA_TOPIC_LARGE)
	public void listenLarge(ConsumerRecord<String, String> cr) {
		log.info(cr.toString());
		this.largeLatch.countDown();
		log.info("Remaining " + this.largeLatch.getCount() + " large messages.");
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaProofOfConcept.class, args).close();
	}

	// https://www.codenotfound.com/spring-kafka-json-serializer-deserializer-example.html
	// https://stackoverflow.com/questions/41533391/how-to-create-separate-kafka-listener-for-each-topic-dynamically-in-springboot
	// https://github.com/spring-projects/spring-kafka/issues/132
	// https://docs.spring.io/spring-kafka/reference/htmlsingle/

}
