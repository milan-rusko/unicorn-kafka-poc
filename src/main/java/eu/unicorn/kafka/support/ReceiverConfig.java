package eu.unicorn.kafka.support;

import eu.unicorn.kafka.avro.Event;
import eu.unicorn.kafka.avro.User;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

@Configuration
@EnableKafka
public class ReceiverConfig {

	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Bean
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "default");
		props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, 16793600);

		ConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
				new StringDeserializer());

		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory);

		return factory;
	}

	@Bean(name = "userFactory")
	public ConcurrentKafkaListenerContainerFactory<String, User> kafkaListenerUserContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, User> factory = new ConcurrentKafkaListenerContainerFactory<>();
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "avro");

		ConsumerFactory<String, User> consumerFactory = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
				new AvroDeserializer<>(User.class));

		factory.setConsumerFactory(consumerFactory);

		return factory;
	}

	@Bean(name = "inputXmlFactory")
	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerInputXmlContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "inputXml");

		ConsumerFactory<String, String> consumerFactory = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
        new StringDeserializer());

		factory.setConsumerFactory(consumerFactory);

		return factory;
	}

	@Bean(name = "eventFactory")
	public ConcurrentKafkaListenerContainerFactory<String, Event> kafkaListenerEventContainerFactory() {
		ConcurrentKafkaListenerContainerFactory<String, Event> factory = new ConcurrentKafkaListenerContainerFactory<>();
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "event");
		props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

		ConsumerFactory<String, Event> consumerFactory = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
				new AvroDeserializer<>(Event.class));

		factory.setConsumerFactory(consumerFactory);

		return factory;
	}

	@Bean
	public Consumer<String, Event> getEventConsumer() {
		Map<String, Object> props = new HashMap<>();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, AvroDeserializer.class);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "reConsumeEvent");
		props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

		ConsumerFactory<String, Event> cf = new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(),
				new AvroDeserializer<>(Event.class));
		Consumer<String, Event> consumer = cf.createConsumer();
		consumer.assign(Collections.singletonList(new TopicPartition("kafkaTest.event", 0)));

		return consumer;
	}
}
