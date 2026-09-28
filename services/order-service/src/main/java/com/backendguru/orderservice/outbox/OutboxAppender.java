package com.backendguru.orderservice.outbox;

import com.backendguru.common.event.OrderConfirmedEvent;
import com.backendguru.orderservice.order.Order;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kholodilin.outbox.OutboxService;
import java.time.Instant;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * Enqueues {@code ORDER_CONFIRMED} into the starter outbox in the same DB transaction as {@code
 * OrderStatus.CONFIRMED}.
 *
 * <p>{@code channel("order")} is an isolated table for this service's order stream — not V2 {@code
 * aggregate_type}. {@code eventType} is the operation ({@code ORDER_CONFIRMED}). {@code
 * aggregateId} is the order id.
 */
@Component
@RequiredArgsConstructor
public class OutboxAppender {

  static final String CHANNEL = "order";
  static final String EVENT_TYPE = "ORDER_CONFIRMED";

  private final OutboxService outboxService;
  private final ObjectMapper objectMapper;

  public void appendOrderConfirmed(Order order) {
    String eventId = UUID.randomUUID().toString();
    OrderConfirmedEvent event =
        new OrderConfirmedEvent(
            eventId,
            order.getId(),
            order.getUserId(),
            order.getTotalAmount(),
            order.getCurrency(),
            Instant.now());
    String payload;
    try {
      payload = objectMapper.writeValueAsString(event);
    } catch (JsonProcessingException ex) {
      throw new IllegalStateException(
          "Failed to serialize OrderConfirmedEvent for order " + order.getId(), ex);
    }
    String orderId = String.valueOf(order.getId());
    outboxService
        .channel(CHANNEL)
        .eventType(EVENT_TYPE)
        .aggregateId(orderId)
        .partitionKey(orderId)
        .payload(payload)
        .append();
  }
}
