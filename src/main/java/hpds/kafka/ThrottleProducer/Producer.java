package hpds.kafka.ThrottleProducer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.SplittableRandom;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class Producer {

    public static void main(String[] args) throws Exception {
        ArgumentParser parser = argParser();

        try {
            Namespace res = parser.parseArgs(args);

            /* parse args */
            String topicName = res.getString("topic");
            long numRecords = res.getLong("numRecords");
            int throughput = res.getInt("throughput");
            List<String> producerProps = res.getList("producerConfig");

            if (producerProps == null) {
                throw new ArgumentParserException("Either --producer-props or --producer.config must be specified.", parser);
            }

            Properties props = readProps(producerProps);
            debugProperties(props);
            KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

            /* setup perf test */
            List<Float> payload = null;

            // not threadsafe, do not share with other threads
            SplittableRandom random = new SplittableRandom(0);
            ProducerRecord<String, byte[]> record;
            long startMs = System.currentTimeMillis();

            ThroughputThrottler throttler = new ThroughputThrottler(throughput, startMs);

            for (long i = 0; i < numRecords; i++) {

                payload = generateFloatList(); // Generate list of floats
                byte[] serializedData = serialize(payload);

                record = new ProducerRecord<>(topicName, serializedData);

                long sendStartMs = System.currentTimeMillis();
                producer.send(record);

                if (throttler.shouldThrottle(i, sendStartMs)) {
                    throttler.throttle();
                }
            }
            producer.close();

        } catch (ArgumentParserException e) {
            if (args.length == 0) {
                parser.printHelp();
            } else {
                parser.handleError(e);
            }
        }
    }

    static void debugProperties(Properties props) {
        System.out.println("Printing properties:");
        for (String key : props.stringPropertyNames()) {
            System.out.println(key + " = " + props.getProperty(key));
        }
    }

    static Properties readProps(List<String> producerProps) {
        Properties props = new Properties();
        if (producerProps != null) {
            for (String prop : producerProps) {
                String[] pieces = prop.split("=");
                if (pieces.length != 2) {
                    throw new IllegalArgumentException("Invalid property: " + prop);
                }
                props.put(pieces[0], pieces[1]);
            }
        }

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        if (props.getProperty(ProducerConfig.CLIENT_ID_CONFIG) == null) {
            props.put(ProducerConfig.CLIENT_ID_CONFIG, "perf-producer-client");
        }
        return props;
    }

    private static byte[] serialize(List<Float> data) {
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos)) {
            oos.writeObject(data);
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
        return bos.toByteArray();
    }

    static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("producer-performance")
                .defaultHelp(true)
                .description("This tool is used to verify the producer performance.");

        parser.addArgument("--topic")
                .required(true)
                .type(String.class)
                .metavar("TOPIC")
                .help("produce messages to this topic");

        parser.addArgument("--num-records")
                .required(true)
                .type(Long.class)
                .metavar("NUM-RECORDS")
                .dest("numRecords")
                .help("number of messages to produce");

        parser.addArgument("--throughput")
                .required(true)
                .type(Integer.class)
                .metavar("THROUGHPUT")
                .help("throttle maximum message throughput to *approximately* THROUGHPUT messages/sec. Set this to -1 to disable throttling.");

        parser.addArgument("--producer-props")
                .nargs("+")
                .required(false)
                .metavar("PROP-NAME=PROP-VALUE")
                .type(String.class)
                .dest("producerConfig")
                .help("kafka producer related configuration properties like bootstrap.servers,client.id etc. " +
                        "These configs take precedence over those passed via --producer.config.");

        return parser;
    }

    static List<Float> generateFloatList() {
        List<Float> floatList = new ArrayList<>();
        // Generate random floats
        for (int i = 0; i < 20; i++) {
            floatList.add((float) Math.random());
        }
        return floatList;
    }
}
