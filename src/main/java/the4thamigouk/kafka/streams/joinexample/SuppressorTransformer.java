package the4thamigouk.kafka.streams.joinexample;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.kstream.TransformerSupplier;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.To;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.time.Duration;
import java.util.Deque;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

class SuppressorTransformer implements Transformer<Object, GenericRecord, KeyValue<Object, GenericRecord>>, Punctuator {

    private static final Logger log = LogManager.getLogger(SuppressorTransformer.class.getName());

    class Record {
        final private long timestamp;
        final private Object key;
        final private GenericRecord value;

        Record(final long timestamp, final Object key, final GenericRecord value) {
            this.timestamp = timestamp;
            this.key = key;
            this.value = value;
        }

        long getTimestamp() {
            return timestamp;
        }

        Object getKey() {
            return key;
        }

        GenericRecord getValue() {
            return value;
        }
    }

    final private static Map<String, SuppressorTransformer> suppressors = new ConcurrentHashMap<>();
    final private AtomicLong streamTime = new AtomicLong();
    final private Deque<Record> cache = new ConcurrentLinkedDeque<>();
    final private String otherTopic;
    private SuppressorTransformer other;
    private ProcessorContext context;
    private boolean isRegistered = false;
    private Duration punctuateInterval;

    public static TransformerSupplier<Object, GenericRecord, KeyValue<Object, GenericRecord>> getSupplier(final String otherTopic, final Duration punctuateInterval) {
        return () -> new SuppressorTransformer(otherTopic, punctuateInterval);
    }

    private SuppressorTransformer(final String otherTopic, final Duration punctuateInterval) {
        this.otherTopic = otherTopic;
        this.punctuateInterval = punctuateInterval;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.context.schedule(punctuateInterval, PunctuationType.WALL_CLOCK_TIME, this);
    }

    private long streamTime() {
        return streamTime.get();
    }

    @Override
    public KeyValue<Object, GenericRecord> transform(Object key, GenericRecord value) {
        maybeRegister(context.topic(), context.partition());
        pushRecord(context.timestamp(), key, value);
        return null;
    }

    @Override
    public void punctuate(long timestamp) {
        if(other == null) {
            return;
        }
        while (!cache.isEmpty()) {
            final long streamTime = streamTime();
            if (streamTime == 0 || streamTime <= other.streamTime()) {
                popRecord();
            } else {
                break;
            }
        }
    }

    @Override
    public void close() {

    }

    private void pushRecord(final long ts, final Object key, final GenericRecord value) {
        log.debug(String.format("pushing : key=%s, timestamp=%d, value=%s", String.valueOf(key), context.timestamp(), String.valueOf(value)));
        cache.offer(new Record(context.timestamp(), key, value));
        log.debug(String.format("pushed : key=%s, timestamp=%d, value=%s", String.valueOf(key), context.timestamp(), String.valueOf(value)));
    }

    private void popRecord() {
        if (!cache.isEmpty()) {
            final Record record = cache.poll();
            log.debug(String.format("popping : key=%s, timestamp=%d, value=%s, streamTime=%d", String.valueOf(record.getKey()), context.timestamp(), String.valueOf(record.getValue()), streamTime.get()));
            context.forward(record.getKey(), record.getValue(), To.all().withTimestamp(record.getTimestamp()));
            if (record.getTimestamp() > streamTime.get()) {
                streamTime.set(record.getTimestamp());
            }
            log.debug(String.format("popped : key=%s, timestamp=%d, value=%s, streamTime=%d", String.valueOf(record.getKey()), context.timestamp(), String.valueOf(record.getValue()), streamTime.get()));
        }
    }

    private String id(final String topic, final int partition) {
        return String.format("%s.%d", topic, partition );
    }

    private void maybeRegister(final String topic, final int partition) {
        if(isRegistered) {
            return;
        }

        final String id = id(topic, partition);
        log.info(String.format("Registering : %s", id));
        suppressors.putIfAbsent(id, this);

        if(other == null) {

            final String otherId = id(otherTopic, partition);
            log.info(String.format("Connecting : %s <-> %s", id, otherId));
            other = suppressors.get(otherId);
            if (other != null) {
                other.other = this;
                log.info(String.format("Connected : %s <-> %s", id, otherId));
            }
            isRegistered = true;
            log.info(String.format("Registered : %s", id));
        }
    }
}

