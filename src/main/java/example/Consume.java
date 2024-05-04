package example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.json.JSONArray;
import org.json.JSONObject;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.net.URL;
import java.net.HttpURLConnection;
import java.io.OutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class Consume {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "my-cluster-kafka-bootstrap.kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 2048);

        // Create Kafka consumer
        KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);

        // Subscribe to topic
        String topic = "java";
        consumer.subscribe(Collections.singletonList(topic));

        // Poll for records
        try {
            //noinspection InfiniteLoopStatement
            while (true) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(100));
                if (!records.isEmpty()) {
                    List<List<Float>> batch = new ArrayList<>();
                    for (ConsumerRecord<String, byte[]> record : records) {
                        List<Float> value = deserialize(record.value());
//                        System.out.println("Received record with key: " + key + ", value: " + value);

                        // Add record to batch
                        batch.add(value);
                    }
//                    System.out.println(batch.size());

                    // Send batch for prediction
                    sendBatchForPrediction(batch);
                }
            }
        } finally {
            consumer.close();
        }
    }

    // Deserialize byte[] to List<Float>
    private static List<Float> deserialize(byte[] data) {
        List<Float> result = new ArrayList<>();
        try (java.io.ObjectInputStream ois = new java.io.ObjectInputStream(new java.io.ByteArrayInputStream(data))) {
            result = (List<Float>) ois.readObject();
        } catch (java.io.IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return result;
    }

    // Send batch for prediction
    private static void sendBatchForPrediction(List<List<Float>> batch) {
        try {
            // Create URL for TensorFlow Serving prediction endpoint
            URL url = new URL("http://tfserving.shhung:8501/v1/models/federate:predict");

            // Open connection
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setDoOutput(true);
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");

            // Construct input data JSON object
            JSONObject inputJson = new JSONObject();
            inputJson.put("signature_name", "serving_default");

            JSONArray instancesArray = new JSONArray();
            for (List<Float> instance : batch) {
                instancesArray.put(instance);
            }
            inputJson.put("instances", instancesArray);

            // Send data
            OutputStream os = conn.getOutputStream();
            os.write(inputJson.toString().getBytes());
            os.flush();

            // Check response code
            if (conn.getResponseCode() != HttpURLConnection.HTTP_OK) {
                throw new RuntimeException("Failed : HTTP error code : " + conn.getResponseCode());
            }

            // Read response
            BufferedReader br = new BufferedReader(new InputStreamReader((conn.getInputStream())));
            StringBuilder responseBuilder = new StringBuilder();
            String output;
            while ((output = br.readLine()) != null) {
                responseBuilder.append(output);
            }

            // Close connection
            conn.disconnect();

            // Parse JSON response
            JSONObject responseJson = new JSONObject(responseBuilder.toString());
            JSONArray predictions = responseJson.getJSONArray("predictions");

            // Process predictions (convert to numpy array)
            // Assuming you're using some library to handle numpy-like arrays in Java
            // Here, we're just printing the predictions for demonstration purposes
            for (int i = 0; i < predictions.length(); i++) {
                JSONArray prediction = predictions.getJSONArray(i);
                int maxIndex = getMaxIndex(prediction);
//                System.out.println("Prediction " + (i + 1) + ": Max value index = " + maxIndex);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int getMaxIndex(JSONArray array) {
        float max = Float.MIN_VALUE;
        int maxIndex = -1;
        for (int i = 0; i < array.length(); i++) {
            float value = array.getFloat(i);
            if (value > max) {
                max = value;
                maxIndex = i;
            }
        }
        return maxIndex;
    }
}
