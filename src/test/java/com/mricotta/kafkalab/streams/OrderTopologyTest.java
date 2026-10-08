package com.mricotta.kafkalab.streams;

import static org.assertj.core.api.Assertions.assertThat;

import com.mricotta.kafkalab.config.KafkaStreamsConfig;
import com.mricotta.kafkalab.config.KafkaTopicConfig;
import com.mricotta.kafkalab.event.CustomerTotal;
import com.mricotta.kafkalab.event.OrderEvent;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class OrderTopologyTest {

    @TempDir
    Path stateDir;

    private TopologyTestDriver driver;
    private TestInputTopic<String, OrderEvent> orders;
    private TestOutputTopic<String, OrderEvent> highValueOrders;
    private TestOutputTopic<String, CustomerTotal> customerTotals;

    @BeforeEach
    void setUp() {
        var config = new KafkaStreamsConfig();
        var orderEventSerde = config.orderEventSerde();
        var customerTotalSerde = config.customerTotalSerde();

        // build the real topology, without Spring and without a broker
        var builder = new StreamsBuilder();
        new OrderTopology(orderEventSerde, customerTotalSerde).build(builder);

        var props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-topology-test");
        // required by the config but never contacted: the driver runs the topology in memory
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());

        driver = new TopologyTestDriver(builder.build(), props);

        var stringSerde = Serdes.String();
        orders = driver.createInputTopic(
                KafkaTopicConfig.ORDERS, stringSerde.serializer(), orderEventSerde.serializer());
        highValueOrders = driver.createOutputTopic(
                KafkaTopicConfig.ORDERS_HIGH_VALUE, stringSerde.deserializer(), orderEventSerde.deserializer());
        customerTotals = driver.createOutputTopic(
                KafkaTopicConfig.CUSTOMER_TOTALS, stringSerde.deserializer(), customerTotalSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
    }

    @Test
    void forwardsOnlyOrdersAboveTheThreshold() {
        var small = order("o1", "c1", "25");
        var atThreshold = order("o2", "c1", "1000");
        var large = order("o3", "c2", "1500");

        orders.pipeInput(small.customerId(), small);
        orders.pipeInput(atThreshold.customerId(), atThreshold);
        orders.pipeInput(large.customerId(), large);

        assertThat(highValueOrders.readKeyValuesToList()).containsExactly(KeyValue.pair("c2", large));
    }

    @Test
    void emitsRunningTotalPerCustomer() {
        pipe(order("o1", "c1", "100"));
        pipe(order("o2", "c2", "50"));
        pipe(order("o3", "c1", "200"));

        // one update per input record, each carrying the accumulated value for its key
        assertThat(customerTotals.readKeyValuesToList()).containsExactly(
                KeyValue.pair("c1", new CustomerTotal("c1", 1, new BigDecimal("100"))),
                KeyValue.pair("c2", new CustomerTotal("c2", 1, new BigDecimal("50"))),
                KeyValue.pair("c1", new CustomerTotal("c1", 2, new BigDecimal("300"))));
    }

    @Test
    void keepsLatestTotalPerCustomerInTheStateStore() {
        pipe(order("o1", "c1", "100"));
        pipe(order("o2", "c1", "200"));

        KeyValueStore<String, CustomerTotal> store = driver.getKeyValueStore(OrderTopology.CUSTOMER_TOTALS_STORE);

        assertThat(store.get("c1")).isEqualTo(new CustomerTotal("c1", 2, new BigDecimal("300")));
        assertThat(store.get("unknown")).isNull();
    }

    private void pipe(OrderEvent order) {
        orders.pipeInput(order.customerId(), order);
    }

    private static OrderEvent order(String orderId, String customerId, String amount) {
        return new OrderEvent(orderId, customerId, "product", new BigDecimal(amount), Instant.parse("2026-01-01T10:00:00Z"));
    }
}
