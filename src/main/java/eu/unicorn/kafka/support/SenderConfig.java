package eu.unicorn.kafka.support;

import eu.unicorn.kafka.avro.Event;
import eu.unicorn.kafka.avro.User;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class SenderConfig {

	@Value("${kafka.bootstrap-servers}")
	private String bootstrapServers;

	@Bean
	public KafkaTemplate<String, String> kafkaTemplate() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 16793600);
		ProducerFactory<String, String> factory = new DefaultKafkaProducerFactory<>(props);

		return new KafkaTemplate<>(factory);
	}

	@Bean
	public KafkaTemplate<String, User> kafkaUserTemplate() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);
		ProducerFactory<String, User> factory = new DefaultKafkaProducerFactory<>(props);

		return new KafkaTemplate<>(factory);
	}

	@Bean
	public KafkaTemplate<String, byte[]> kafkaXmlTemplate() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
		ProducerFactory<String, byte[]> factory = new DefaultKafkaProducerFactory<>(props);

		return new KafkaTemplate<>(factory);
	}

	@Bean
	public KafkaTemplate<String, Event> kafkaEventTemplate() {
		Map<String, Object> props = new HashMap<>();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, AvroSerializer.class);
		props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "kafkaTest-transactional-id");
		DefaultKafkaProducerFactory<String, Event> factory = new DefaultKafkaProducerFactory<>(props);
		factory.setTransactionIdPrefix("kafkaTest-transactional-id");
		return new KafkaTemplate<>(factory);
	}
}
