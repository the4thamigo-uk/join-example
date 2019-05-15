package the4thamigouk.kafka.streams.joinexample;

import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

class JoinerStreamProcessor {

    private static final Logger log = LogManager.getLogger(JoinerStreamProcessor.class.getName());

    public static void main(final String[] args) {

        final Duration joinWindowBeforeSize = Duration.parse(System.getenv("JOIN_WINDOW_BEFORE_SIZE"));
        final Duration joinWindowAfterSize = Duration.parse(System.getenv("JOIN_WINDOW_AFTER_SIZE"));
        final Duration joinWindowGrace = Duration.parse(System.getenv("JOIN_WINDOW_GRACE"));

        final Properties props = new Properties();
        props.put("application.id", "some-application-id");
        props.put("bootstrap.servers", "broker:9092");
        props.put("auto.offset.reset", "earliest");
        props.put("schema.registry.url", "http://schema-registry:8081");
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class.getName());

        final StreamsBuilder builder = new StreamsBuilder();

        final TransformerSupplier streamLogger = () -> new Transformer<Object, GenericRecord, KeyValue<Object, GenericRecord>>() {
            private ProcessorContext context;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
            }

            @Override
            public KeyValue<Object, GenericRecord> transform(Object key, GenericRecord value) {
                log.info(String.format("reading : key=%s, timestamp=%d, value=%s", String.valueOf(key), context.timestamp(), String.valueOf(value)));
                return new KeyValue<>(key, value);
            }

            @Override
            public void close() {

            }
        };

        final TimestampExtractor tsExtractor = (record, previousTimestamp) -> {
            if (record == null) {
                return -1;
            }
            final GenericRecord r = (GenericRecord) record.value();
            final Number ts = (Number) r.get("event_time");
            return ts.longValue();
        };


        final Duration punctuateInterval = Duration.parse("PT0.05S");

        final Consumed<Object, GenericRecord> leftConsumed = Consumed.with(tsExtractor);
        final KStream<Object, GenericRecord> leftStream = builder.stream("x", leftConsumed)
                .transform(SuppressorTransformer.getSupplier("y", punctuateInterval))
                .transform(streamLogger);
        final Consumed<Object, GenericRecord> rightConsumed = Consumed.with(tsExtractor);
        final KStream<Object, GenericRecord> rightStream = builder.stream("y", rightConsumed)
                .transform(SuppressorTransformer.getSupplier("x", punctuateInterval))
                .transform(streamLogger);

        // setup the join
        final JoinWindows joinWindow = JoinWindows
                .of(Duration.ZERO)
                .before(joinWindowBeforeSize)
                .after(joinWindowAfterSize)
                .grace(joinWindowGrace);

        final KStream<Object, GenericRecord> joinStream = leftStream.join(rightStream,
                (l, r) -> {
                    log.info("joining: " + l + ", " + r);
                    return null;
                }, joinWindow);

        final Topology topology = builder.build();

        final KafkaStreams streams = new KafkaStreams(topology, props);


        try {
            // attach shutdown handler to catch control-c
            final CountDownLatch latch = new CountDownLatch(1);
            Runtime.getRuntime().addShutdownHook(new Thread("streams-joiner-shutdown-hook") {
                @Override
                public void run() {
                    streams.close();
                    latch.countDown();
                }
            });

            log.info("Starting topology: " + topology.describe().toString());
            streams.start();
            log.info("Topology started");
            latch.await();
        } catch (final Throwable e) {
            log.fatal("Error encountered while running topology", e);
            System.exit(1);
        }
        System.exit(0);
    }
}
