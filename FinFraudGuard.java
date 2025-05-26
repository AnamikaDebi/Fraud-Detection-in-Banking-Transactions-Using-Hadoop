package com.pack;

import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

// Main class for Credit Card Fraud Detection using Spark Streaming
public class CCFraudDetectionSystem {

    public static void main(String[] args) throws Exception {
        // Validate command-line arguments
        if (args.length < 5) {
            System.err.println("Usage: CCFraudDetectionSystem <broker> <topic> <groupId> <zipCodeCsvPath> <hbaseMaster>");
            System.exit(1);
        }

        // Extract arguments
        String broker = args[0];
        String topic = args[1];
        String groupId = args[2];
        String zipCodeCsvPath = args[3];
        String hBaseMaster = args[4];

        // Initialize Spark Streaming context (1-second batch interval)
        SparkConf conf = new SparkConf().setAppName("CCFraudDetectionSystem");
        JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(1));
        context.sparkContext().setLogLevel("WARN");

        // Set up Kafka topic and parameters
        Set<String> topics = new HashSet<>(Arrays.asList(topic));
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
        kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        kafkaParams.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        kafkaParams.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Create Kafka stream
        JavaInputDStream<ConsumerRecord<String, JsonNode>> messages = KafkaUtils.createDirectStream(
            context,
            LocationStrategies.PreferConsistent(),
            ConsumerStrategies.Subscribe(topics, kafkaParams)
        );

        // Parse Kafka messages into TransactionData objects
        JavaDStream<TransactionData> transactions = messages.map(record -> {
            ObjectMapper mapper = new ObjectMapper();
            return mapper.treeToValue(record.value(), TransactionData.class);
        });

        // Process each transaction RDD
        transactions.foreachRDD(rdd -> {
            rdd.mapToPair(transaction -> {
                // 1. Fetch lookup record from HBase
                TransactionLookupRecord lookup = HbaseClient.getTransactionLookupRecord(transaction, hBaseMaster);

                // 2. Calculate speed (distance / time)
                double distance = DistanceUtility.GetInstance(zipCodeCsvPath)
                    .getDistanceViaZipCode(transaction.getPostcode().toString(), lookup.getPostcode().toString());
                SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
                long timeDiffSeconds = (sdf.parse(transaction.getTransactionDate()).getTime() -
                                       sdf.parse(lookup.getTransactionDate()).getTime()) / 1000;
                double speed = distance / timeDiffSeconds;

                // 3. Determine transaction status
                String status = (lookup.getScore() < 200 || transaction.getAmount() > lookup.getUcl() || speed > 0.25)
                    ? "FRAUD" : "GENUINE";

                // 4. Insert transaction into HBase and update lookup if genuine
                HbaseClient.InsertTransactionIntoDB(transaction, hBaseMaster, status);

                return new Tuple2<>(transaction, status);
            }).collect().forEach(tuple ->
                System.out.println(tuple._1.toString() + " - " + tuple._2)
            );
        });

        // Start streaming and await termination
        context.start();
        context.awaitTermination();
    }
}
