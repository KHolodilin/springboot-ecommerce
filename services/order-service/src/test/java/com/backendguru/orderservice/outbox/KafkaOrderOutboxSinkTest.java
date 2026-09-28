package com.backendguru.orderservice.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

import com.kholodilin.outbox.model.OutboxPublishResult;
import com.kholodilin.outbox.model.OutboxRecord;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

@ExtendWith(MockitoExtension.class)
class KafkaOrderOutboxSinkTest {

  @Mock KafkaTemplate<String, String> kafkaTemplate;

  private OutboxRecord pendingRow() {
    return new OutboxRecord(
        "order",
        1L,
        "ORDER_CONFIRMED",
        "100",
        "100",
        "{\"orderId\":100}",
        Map.of(),
        null,
        0,
        Instant.now());
  }

  @Test
  void successReturnsAllSucceeded() throws Exception {
    KafkaOrderOutboxSink sink = new KafkaOrderOutboxSink(kafkaTemplate, "order.confirmed");
    OutboxRecord row = pendingRow();
    var record = new ProducerRecord<String, String>("order.confirmed", "100", row.payloadJson());
    var meta = new RecordMetadata(new TopicPartition("order.confirmed", 0), 0, 0, 0L, 0, 0);
    var sendResult = new SendResult<>(record, meta);
    when(kafkaTemplate.send(anyString(), anyString(), anyString()))
        .thenReturn(CompletableFuture.completedFuture(sendResult));

    OutboxPublishResult result = sink.publish(List.of(row));

    assertThat(result).isInstanceOf(OutboxPublishResult.AllSucceeded.class);
  }

  @Test
  void kafkaSendFailureReturnsAllFailed() {
    KafkaOrderOutboxSink sink = new KafkaOrderOutboxSink(kafkaTemplate, "order.confirmed");
    OutboxRecord row = pendingRow();
    var failed = new CompletableFuture<SendResult<String, String>>();
    failed.completeExceptionally(new ExecutionException(new RuntimeException("broker down")));
    when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(failed);

    OutboxPublishResult result = sink.publish(List.of(row));

    assertThat(result).isInstanceOf(OutboxPublishResult.AllFailed.class);
    assertThat(((OutboxPublishResult.AllFailed) result).cause()).hasMessageContaining("broker down");
  }
}
