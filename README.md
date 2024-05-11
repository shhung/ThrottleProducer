# ThrottleProducer

ThrottleProducer is a project aimed at creating a test environment capable of generating a fixed amount of data. It consists of two main classes that leverage Kafka:
- `hpds.kafka.ThrottleProducer.Producer`
- `example.consume`

## Introduction

ThrottleProducer offers a solution to create a controlled workload for testing purposes, particularly for systems reliant on Kafka. By utilizing Kafka's producer-perf-test.sh tool and custom Java classes, it enables the generation of a nearly fixed amount of data. The generated data can then be consumed and processed by other components in the system.

## Features

- **Producer**: The `hpds.kafka.ThrottleProducer.Producer` class leverages Kafka's producer-perf-test.sh tool to configure throughput, facilitating the generation of approximately fixed amounts of data.
- **Consumer**: The `example.consume` class collects all data obtained through `consumer.poll()` into batches, which are then sent to a TensorFlow Serving application for prediction.

## Usage

1. **Producer Setup**: Configure and run the `hpds.kafka.ThrottleProducer.Producer` class to generate a controlled workload of data.
2. **Consumer Processing**: Utilize the `example.consume` class to collect and process the generated data batches.

## Getting Started

To use ThrottleProducer, follow these steps:

1. **Installation**: Clone the repository to your local machine.
2. **Configuration**: Adjust the settings in the `hpds.kafka.ThrottleProducer.Producer` class to match your testing requirements.
3. **Build**: Use Gradle to automatically build the project. A shadowJar is provided for easy deployment to the desired environment.
4. **Execution**:
    - **Producer**: Run the following command to execute the Producer:
      ```
      java -cp app.jar hpds.kafka.ThrottleProducer.Producer --topic ${TOPIC} --num-records ${RECORDS} --throughput ${THROUGHPUT} --producer-props bootstrap.servers=${BOOTSTRAP_SERVER}
      ```
    - **Consumer**: Set the following environment variables for the consumer: `BOOTSTRAP_SERVER`, `GROUP_ID`, `MAX_POLL_RECORDS`, `TOPIC`. Then, run the following command to execute the Consumer:
      ```
      java -cp app.jar example.consume
      ```
