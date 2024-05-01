package example;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class GeneratePayloadFile {

    public static void main(String[] args) {
        String fileName = "payload_file.txt";
        int payloadSize = 20;

        // Generate payload
        List<Float> payload = generateFloatList(payloadSize);

        // Write payload to file
        writePayloadToFile(payload, fileName);

        System.out.println("Payload file generated successfully: " + fileName);
    }

    static List<Float> generateFloatList(int size) {
        List<Float> floatList = new ArrayList<>();
        // Generate random floats
        for (int i = 0; i < size; i++) {
            floatList.add((float) Math.random());
        }
        return floatList;
    }

    static void writePayloadToFile(List<Float> payload, String fileName) {
        try (FileWriter writer = new FileWriter(fileName)) {
            for (Float value : payload) {
                writer.write(value.toString() + "\n");
            }
        } catch (IOException e) {
            System.err.println("Error writing payload to file: " + e.getMessage());
        }
    }
}
