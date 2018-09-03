package eu.unicorn.kafka;

import eu.unicorn.kafka.avro.Event;
import eu.unicorn.kafka.avro.User;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.StopWatch;

@Slf4j
@SpringBootApplication
public class KafkaProofOfConcept implements CommandLineRunner {

	private static final String KAFKA_TOPIC = "kafkaTest.normal";
	private static final String KAFKA_TOPIC_USER = "kafkaTest.user";
	private static final String KAFKA_TOPIC_LARGE = "kafkaTest.large";
	private static final String KAFKA_TOPIC_INPUT_XML = "kafkaTest.inputXml";
	private static final String KAFKA_TOPIC_EVENT = "kafkaTest.event";
	private static final String KAFKA_TOPIC_FAILED_EVENT = "kafkaTest.failedEvent";

	private static final int NORMAL_MESSAGES = 1000;
	private static final int LARGE_MESSAGES = 10;
	private static final int INPUT_XMLS = 1000;

	private static final long WAIT_TIMEOUT_SECONDS = 30;

	private static final int LARGE_MESSAGE_SIZE = 1024 * 1024 * 16;

	private int receivedEvents = 0;
	private int receivedFailedEvents = 0;
	private long lastEventReceived = System.currentTimeMillis();

	private StopWatch appWatch = new StopWatch();
	private StopWatch normalMessagesWatch = new StopWatch();
	private StopWatch largeMessagesWatch = new StopWatch();
	private StopWatch userWatch = new StopWatch();
	private StopWatch xmlFilesWatch = new StopWatch();

	@Autowired
	private KafkaTemplate<String, String> template;

	@Autowired
	private KafkaTemplate<String, User> userTemplate;

	@Autowired
	private KafkaTemplate<String, byte[]> xmlTemplate;

	@Autowired
	private KafkaTemplate<String, Event> eventTemplate;

	@Autowired
	private Consumer<String, Event> failureConsumer;

	private final CountDownLatch normalMessagesLatch = new CountDownLatch(NORMAL_MESSAGES);
	private final CountDownLatch largeMessagesLatch = new CountDownLatch(LARGE_MESSAGES);
	private final CountDownLatch xmlFilesLatch = new CountDownLatch(INPUT_XMLS);
	private final CountDownLatch eventsLatch = new CountDownLatch(INPUT_XMLS);

	@Override
	public void run(String... args) throws Exception {
		appWatch.start();
		log.info("Sending.");

		//string message test
		normalMessagesWatch.start();
		sendShortMessages();
		//long string message test
		largeMessagesWatch.start();
		sendLongMessages();
		//avro usage test
		userWatch.start();
		sendUser();
		//xml sending test with
		xmlFilesWatch.start();
		sendInputXmls();
		waitForReceivers();

		//wait to finished events
		while (System.currentTimeMillis() - lastEventReceived < 5000)
		{
			Thread.sleep(1000);
			log.info("waiting for events..");
		}
		appWatch.stop();

		log.info("All received");
		log.info("{} normal messages sent and received in {}ms", NORMAL_MESSAGES, normalMessagesWatch.getTotalTimeMillis());
		log.info("{} large messages ({}) sent and received in {}ms", LARGE_MESSAGES, humanReadableByteCount(LARGE_MESSAGE_SIZE, true), largeMessagesWatch.getTotalTimeMillis());
		log.info("{} xml files sent and received in {}ms", INPUT_XMLS, receivedEvents, xmlFilesWatch.getTotalTimeMillis());
		log.info("{} events received", receivedEvents);
		log.info("{} failed events received", receivedFailedEvents);
		log.info("total run {}ms", appWatch.getTotalTimeMillis());
	}

	public static String humanReadableByteCount(long bytes, boolean si) {
		int unit = si ? 1000 : 1024;
		if (bytes < unit) return bytes + " B";
		int exp = (int) (Math.log(bytes) / Math.log(unit));
		String pre = (si ? "kMGTPE" : "KMGTPE").charAt(exp-1) + (si ? "" : "i");
		return String.format("%.1f %sB", bytes / Math.pow(unit, exp), pre);
	}

	private void waitForReceivers() throws InterruptedException {
		this.normalMessagesLatch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		this.largeMessagesLatch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		this.xmlFilesLatch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		this.eventsLatch.await(WAIT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
	}

	private void sendLongMessages() {
		Random r = new Random();
		for (int i = 0; i < LARGE_MESSAGES; i++) {
			log.info("Sending large message #{}", i);
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
			log.info("Sending normal message #{}", i);
			this.template.send(KAFKA_TOPIC, "normal message " + i);
		}
	}

	private void sendUser() {
		log.info("Sending user.");
		User user = new User("John Doe", 13, "Blue");
		this.userTemplate.send(KAFKA_TOPIC_USER, user);
	}

	private void sendInputXmls() {
		for (int i = 0; i < INPUT_XMLS; i++) {
			log.info("Sending xml #{}", i+1);
			String filePath = String.format("input-xml/%d.xml", (i % 10)+1);
			try {
				Path path = Paths.get(Objects.requireNonNull(getClass().getClassLoader().getResource(filePath)).toURI());
				byte[] inputXmlBytes = Files.readAllBytes(path);
				this.xmlTemplate.send(KAFKA_TOPIC_INPUT_XML, inputXmlBytes);
			}
			catch (IOException | URISyntaxException ex) {
				log.error("file reading failed", ex);
			}
		}
	}

	@KafkaListener(topics = KAFKA_TOPIC)
	public void listen(ConsumerRecord<String, String> cr) {
		this.normalMessagesLatch.countDown();
		if (normalMessagesLatch.getCount() == 0 && normalMessagesWatch.isRunning()) {
			normalMessagesWatch.stop();
		}
		log.info("Received normal message '{}'. Remaining {} normal messages.", cr.value(), this.normalMessagesLatch.getCount());
	}

	@SuppressWarnings("static-method")
	@KafkaListener(topics = KAFKA_TOPIC_USER, containerFactory = "userFactory")
	public void listenUser(ConsumerRecord<String, User> cr) {
		User user = cr.value();
		log.info("Received user: name: {}, favorite number: {}, favorite color: {}", user.getName(), user.getFavoriteNumber(), user.getFavoriteColor());
	}

	@KafkaListener(topics = KAFKA_TOPIC_LARGE)
	public void listenLarge(ConsumerRecord<String, String> cr) {
		this.largeMessagesLatch.countDown();
		if (largeMessagesLatch.getCount() == 0 && largeMessagesWatch.isRunning()) {
			largeMessagesWatch.stop();
		}
		log.info("Received large message. Remaining {} large messages.", this.largeMessagesLatch.getCount());
	}

	@KafkaListener(topics = KAFKA_TOPIC_INPUT_XML, containerFactory = "inputXmlFactory")
	public void listenInputXml(ConsumerRecord<String, String> cr) {
		this.xmlFilesLatch.countDown();
		if (xmlFilesLatch.getCount() == 0 && xmlFilesWatch.isRunning()) {
			xmlFilesWatch.stop();
		}
		int modulo = (int)cr.offset() % 3;
		String eventType = modulo == 0 ? EventType.NEW_MESSAGE.name() : modulo == 1 ? EventType.DUPLICATED_MESSAGE.name() : EventType.READY_FOR_PROCESSING.name();
		Event event = new Event(eventType, String.format("%s.%s.%s", cr.topic(), cr.partition(), cr.offset()), cr.offset());
		this.eventTemplate.executeInTransaction(t -> t.send(KAFKA_TOPIC_EVENT, event));
	}

	@KafkaListener(topics = KAFKA_TOPIC_EVENT, containerFactory = "eventFactory")
	public void listenEvent(ConsumerRecord<String, Event> cr) {
		eventsLatch.countDown();
		receivedEvents++;
		lastEventReceived = System.currentTimeMillis();
		Event event = cr.value();
		log.info("Received event: type: {}, file: {}, xml offset: {}, event offset: {}", event.getType(), event.getReceivedFile(), event.getTopicOffset(), cr.offset());
		if (EventType.DUPLICATED_MESSAGE.name().contentEquals(event.getType())) {
		 	//re-consume last message
		 	//failureConsumer.poll(100L); //dummy poll to initialize state of topic
		 	failureConsumer.seek(new TopicPartition(KAFKA_TOPIC_EVENT, 0), cr.offset());
		 	ConsumerRecords<String, Event> records = failureConsumer.poll(1000L);
		 	if (!records.isEmpty()) {
				ConsumerRecord<String, Event> failedRecord = records.iterator().next();
				Event failedEvent = failedRecord.value();
				lastEventReceived = System.currentTimeMillis();
				log.info("  Re-consumed event: type: {}, file: {}, xml offset: {}, event offset {}", failedEvent.getType(), failedEvent.getReceivedFile(), failedEvent.getTopicOffset(), failedRecord.offset());
				eventTemplate.executeInTransaction(t -> t.send(KAFKA_TOPIC_FAILED_EVENT, failedEvent));
			}
		}
	}

	@KafkaListener(topics = KAFKA_TOPIC_FAILED_EVENT, containerFactory = "eventFactory")
	public void listenFailedEvent(ConsumerRecord<String, Event> cr) {
		Event event = cr.value();
		receivedFailedEvents++;
		lastEventReceived = System.currentTimeMillis();
		log.info("  Received failed event: type: {}, file: {}, xml offset: {}, event offset {}", event.getType(), event.getReceivedFile(), event.getTopicOffset(), cr.offset());
	}

	public static void main(String[] args) {
		SpringApplication.run(KafkaProofOfConcept.class, args).close();
	}

	// https://www.codenotfound.com/spring-kafka-json-serializer-deserializer-example.html
	// https://stackoverflow.com/questions/41533391/how-to-create-separate-kafka-listener-for-each-topic-dynamically-in-springboot
	// https://github.com/spring-projects/spring-kafka/issues/132
	// https://docs.spring.io/spring-kafka/reference/htmlsingle/
}
