package the4thamigouk.kafka.streams.joinexample;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static org.apache.kafka.common.utils.Utils.max;

class JoinerStreamProcessor {

    private static final Logger log = LogManager.getLogger(JoinerStreamProcessor.class.getName());
    private static final String envPrefix = "APP_";
    private static final String envPropertiesFile = envPrefix + "PROPERTIES_FILE";

    public static void main(final String[] args) throws IOException, RestClientException {

       class Suppressor implements Transformer<Object,GenericRecord, KeyValue<Object,GenericRecord>> {
            private ProcessorContext context;
            private long streamTime = 0;
            private Suppressor other;


            class Record {
                final private long timestamp;
                final private Object key;
                final private GenericRecord value;

                public Record(final long timestamp, final Object key, final GenericRecord value) {
                    this.timestamp = timestamp;
                    this.key = key;
                    this.value = value;
                }
                public long getTimestamp() {
                    return timestamp;
                }

                public Object getKey() {
                    return key;
                }

                public GenericRecord getValue() {
                    return value;
                }
            }

            private ArrayDeque<Record> cache = new ArrayDeque<>();

            public void join(final Suppressor other) {
               this.other = other;
               other.other = this;
            }

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.context.schedule(Duration.ofMillis(100), PunctuationType.WALL_CLOCK_TIME, new Punctuator() {

                    @Override
                    public void punctuate(long timestamp) {
                        while(!cache.isEmpty()) {
                            if(streamTime == 0 || streamTime <= other.streamTime()) {
                                popRecord();
                            } else {
                                break;
                            }
                        }
                    }
                });
            }

            public long streamTime() {
                return streamTime;
            }


            private void popRecord() {
                if(!cache.isEmpty()) {
                    final Record record = cache.poll();
                    streamTime = max(record.getTimestamp(), streamTime);
                    context.forward(record.getKey(), record.getValue(), To.all().withTimestamp(record.getTimestamp()));
                    log.info(String.format("popped: %d, %d, %d, %s", record.getTimestamp(), streamTime, other.streamTime(), record.value.toString() ));
                }
            }

            @Override
            public KeyValue<Object, GenericRecord> transform(Object key, GenericRecord value) {
                log.info(String.format("suppressing: %d, %d, %d, %s", context.timestamp(), streamTime, other.streamTime(), value.toString() ));
                cache.offer(new Record(context.timestamp(), key, value));
                context.commit();

                log.info(String.format("finished suppressing: %d, %d, %d", context.timestamp(), streamTime, other.streamTime() ));
                return null;
            }

            @Override
            public void close() {

            }
        }

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

        final TransformerSupplier streamLogger = () -> new Transformer<Object,GenericRecord, KeyValue<Object,GenericRecord>>() {
            private ProcessorContext context;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
            }

            @Override
            public KeyValue<Object, GenericRecord> transform(Object key, GenericRecord value) {
                log.info(String.format("reading : topic=%s, partition=%d, timestamp=%d, offset=%d, key=%s", context.topic(), context.partition(), context.timestamp(), context.offset(), key));
                return new KeyValue<>(key,value);
            }

            @Override
            public void close() {

            }
        };

        final Suppressor leftSuppressor  = new Suppressor();
        final Suppressor rightSuppressor  = new Suppressor();
        leftSuppressor.join(rightSuppressor);

        final TransformerSupplier leftSupplier = () -> leftSuppressor;
        final TransformerSupplier rightSupplier = () -> rightSuppressor;

        final TimestampExtractor tsExtractor = new TimestampExtractor() {

            @Override
            public long extract(ConsumerRecord<Object, Object> record, long previousTimestamp) {
                if (record == null) {
                    return -1;
                }
                final GenericRecord r = (GenericRecord) record.value();
                final Number ts = (Number) r.get("event_time");
                return ts.longValue();
            }
        };


        final Consumed<Object, GenericRecord> leftConsumed = Consumed.with(tsExtractor);
        final KStream<Object, GenericRecord> leftStream = builder.stream("x", leftConsumed).transform(leftSupplier).transform(streamLogger);
        final Consumed<Object, GenericRecord> rightConsumed = Consumed.with(tsExtractor);
        final KStream<Object, GenericRecord> rightStream = builder.stream("y",rightConsumed).transform(rightSupplier).transform(streamLogger);

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
