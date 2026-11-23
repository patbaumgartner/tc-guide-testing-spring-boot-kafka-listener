package com.testcontainers.demo;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

@SpringBootTest
@TestPropertySource(
  properties = {
    "spring.kafka.consumer.auto-offset-reset=earliest",
    "spring.datasource.url=jdbc:tc:mysql:26.10.0:///db",
  }
)
@Testcontainers
class ProductPriceChangedEventHandlerTest {

  @Container
  static final ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(
    DockerImageName.parse("confluentinc/cp-kafka:8.4.1")
  );

  @DynamicPropertySource
  static void overrideProperties(DynamicPropertyRegistry registry) {
    registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
  }

  @Autowired
  private KafkaTemplate<String, Object> kafkaTemplate;

  @Autowired
  private ProductRepository productRepository;

  @BeforeEach
  void setUp() {
    productRepository.deleteAll();
    Product product = new Product(null, "P100", "Product One", BigDecimal.TEN);
    productRepository.save(product);
  }

  @Test
  void shouldHandleProductPriceChangedEvent() {
    ProductPriceChangedEvent event = new ProductPriceChangedEvent(
      "P100",
      new BigDecimal("14.50")
    );

    kafkaTemplate.send("product-price-changes", event.productCode(), event);

    awaitProductPrice("P100", new BigDecimal("14.50"));
  }

  @Test
  void shouldHandleMessageFromLegacyJsonSerializer()
    throws InterruptedException, ExecutionException, TimeoutException
  {
    Map<String, Object> producerConfig = Map.of(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
      kafka.getBootstrapServers(),
      ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
      StringSerializer.class,
      ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
      StringSerializer.class
    );
    ProducerRecord<String, String> record = new ProducerRecord<>(
      "product-price-changes",
      "P100",
      "{\"productCode\":\"P100\",\"price\":14.50}"
    );
    record
      .headers()
      .add(
        "__TypeId__",
        ProductPriceChangedEvent.class.getName().getBytes(UTF_8)
      );

    try (
      KafkaProducer<String, String> producer = new KafkaProducer<>(
        producerConfig
      )
    ) {
      producer.send(record).get(10, SECONDS);
    }

    awaitProductPrice("P100", new BigDecimal("14.50"));
  }

  private void awaitProductPrice(String productCode, BigDecimal price) {
    await()
      .pollInterval(Duration.ofSeconds(3))
      .atMost(10, SECONDS)
      .untilAsserted(() -> {
        Optional<Product> optionalProduct = productRepository.findByCode(
          productCode
        );
        assertThat(optionalProduct).isPresent();
        assertThat(optionalProduct.get().getCode()).isEqualTo(productCode);
        assertThat(optionalProduct.get().getPrice()).isEqualTo(price);
      });
  }
}
