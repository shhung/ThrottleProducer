package example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Produce {

    public static void main(String[] args) {
        // Kafka producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap.kafka:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // Create Kafka producer
        KafkaProducer<String, byte[]> producer = new KafkaProducer<>(props);

        // Topic to send records to
        String topic = "java";

        // Example data (List of Floats)
        List<Float> inputData = new ArrayList<>();
        inputData.add(1.2f);
        inputData.add(3.4f);
        inputData.add(5.6f);

        // Serialize the list of floats
        byte[] serializedData = serialize(inputData);

        // Send record with List<Float> data
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, "key", serializedData);
        producer.send(record, (metadata, exception) -> {
            if (metadata != null) {
                System.out.println("Record sent successfully to partition " + metadata.partition()
                        + " with offset " + metadata.offset());
            } else {
                System.err.println("Error sending record: " + exception);
            }
        });

        // Close producer
        producer.close();
    }

    // Serialize List<Float> to byte[]
    private static byte[] serialize(List<Float> data) {
        java.io.ByteArrayOutputStream bos = new java.io.ByteArrayOutputStream();
        try (java.io.ObjectOutputStream oos = new java.io.ObjectOutputStream(bos)) {
            oos.writeObject(data);
        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
        return bos.toByteArray();
    }
}
