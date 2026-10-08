package com.mricotta.kafkalab.streams;

import com.mricotta.kafkalab.config.KafkaTopicConfig;
import com.mricotta.kafkalab.event.CustomerTotal;
import com.mricotta.kafkalab.event.OrderEvent;
import java.math.BigDecimal;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OrderTopology {

    private static final Logger log = LoggerFactory.getLogger(OrderTopology.class);

    public static final String CUSTOMER_TOTALS_STORE = "customer-totals-store";

    static final BigDecimal HIGH_VALUE_THRESHOLD = new BigDecimal("1000");

    private final Serde<OrderEvent> orderEventSerde;
    private final Serde<CustomerTotal> customerTotalSerde;

    public OrderTopology(Serde<OrderEvent> orderEventSerde, Serde<CustomerTotal> customerTotalSerde) {
        this.orderEventSerde = orderEventSerde;
        this.customerTotalSerde = customerTotalSerde;
    }

    // Runs once at startup: it only declares the processing graph, records flow through it later
    @Autowired
    public void build(StreamsBuilder builder) {
        // source: one record per order, key = customerId
        KStream<String, OrderEvent> orders =
                builder.stream(KafkaTopicConfig.ORDERS, Consumed.with(Serdes.String(), orderEventSerde));

        highValueOrders(orders);
        customerTotals(orders);
    }

    private void highValueOrders(KStream<String, OrderEvent> orders) {
        orders
                .peek((customerId, order) -> log.info("STREAM IN customerId={} orderId={} amount={}",
                        customerId, order.orderId(), order.amount()))
                // stateless: each record is evaluated on its own, nothing is remembered between records
                .filter((customerId, order) -> order.amount().compareTo(HIGH_VALUE_THRESHOLD) > 0)
                .peek((customerId, order) -> log.info("STREAM HIGH-VALUE orderId={}", order.orderId()))
                // sink: write the surviving records to another topic
                .to(KafkaTopicConfig.ORDERS_HIGH_VALUE, Produced.with(Serdes.String(), orderEventSerde));
    }

    private void customerTotals(KStream<String, OrderEvent> orders) {
        // stateful: the running total of each customer is remembered in a local state store
        KTable<String, CustomerTotal> totals = orders
                // the key is already customerId, so grouping does not need to move records between partitions
                .groupByKey(Grouped.with(Serdes.String(), orderEventSerde))
                .aggregate(
                        // initial value, used the first time a key is seen
                        CustomerTotal::empty,
                        // called per record with the current aggregate of that key; returns the new aggregate
                        (customerId, order, total) -> total.add(order),
                        Materialized.<String, CustomerTotal, KeyValueStore<Bytes, byte[]>>as(CUSTOMER_TOTALS_STORE)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(customerTotalSerde));

        // a KTable is the latest value per key; toStream() exposes each update as a record
        totals.toStream()
                .peek((customerId, total) -> log.info("STREAM TOTAL customerId={} orders={} total={}",
                        customerId, total.orderCount(), total.totalAmount()))
                .to(KafkaTopicConfig.CUSTOMER_TOTALS, Produced.with(Serdes.String(), customerTotalSerde));
    }
}
