package com.backendguru.orderservice.outbox;

import com.kholodilin.outbox.autoconfigure.OutboxChannelSink;
import com.kholodilin.outbox.model.OutboxPublishResult;
import com.kholodilin.outbox.model.OutboxRecord;
import com.kholodilin.outbox.spi.OutboxSink;
import java.util.List;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;

/**
 * Starter poller dequeues rows from {@code outbox_records}; this sink publishes the payload to
 * Kafka {@code order.confirmed}. A thrown send error becomes {@link OutboxPublishResult.AllFailed}
 * so the starter retries the row.
 */
@Slf4j
@OutboxChannelSink(OutboxAppender.CHANNEL)
public class KafkaOrderOutboxSink implements OutboxSink {

  private static final long SEND_TIMEOUT_SECONDS = 5;

  private final KafkaTemplate<String, String> kafkaTemplate;
  private final String orderConfirmedTopic;

  public KafkaOrderOutboxSink(
      KafkaTemplate<String, String> kafkaTemplate,
      @Value("${app.kafka.topics.order-confirmed:order.confirmed}") String orderConfirmedTopic) {
    this.kafkaTemplate = kafkaTemplate;
    this.orderConfirmedTopic = orderConfirmedTopic;
  }

  @Override
  public OutboxPublishResult publish(List<OutboxRecord> records) {
    try {
      for (OutboxRecord rec : records) {
        kafkaTemplate
            .send(orderConfirmedTopic, rec.aggregateId(), rec.payloadJson())
            .get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        log.info(
            "Outbox event {} published to {} (eventType={})",
            rec.eventId(),
            orderConfirmedTopic,
            rec.eventType());
      }
      return new OutboxPublishResult.AllSucceeded();
    } catch (Exception ex) {
      log.error("Outbox Kafka publish failed for {} records: {}", records.size(), ex.toString());
      return new OutboxPublishResult.AllFailed(ex);
    }
  }
}
