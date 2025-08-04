package com.kafka.producer.service;

import java.io.FileReader;
import java.nio.file.Path;
import java.util.AbstractMap;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.producer.model.Transaction;
import com.opencsv.CSVReader;
import com.opencsv.bean.CsvToBeanBuilder;

@Service
public class CsvKafkaProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${kafka.topic.name}")
    private String topicName;

    public CsvKafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }
    
    
    public void sendCsvDataToKafka(Path csvFilePath) {
        try (var reader = new CSVReader(new FileReader(csvFilePath.toFile()))) {

            var transactions = new CsvToBeanBuilder<Transaction>(reader)
                    .withType(Transaction.class)
                    .withIgnoreLeadingWhiteSpace(true)
                    .withSkipLines(1)
                    .build()
                    .parse();

            transactions.stream()
                    .map(transaction -> {
                        try {
                            return new AbstractMap.SimpleEntry<>(
                                    transaction.getUserId(),
                                    objectMapper.writeValueAsString(transaction)
                            );
                        } catch (Exception e) {
                            throw new RuntimeException("JSON conversion failed for: " + transaction, e);
                        }
                    })
                    .forEach(entry -> kafkaTemplate.send(topicName, entry.getKey(), entry.getValue())
                            .whenComplete((result, ex) -> {
                                if (ex == null) {
                                    System.out.printf("✅ Sent to Kafka [%s]: %s%n", topicName, entry.getValue());
                                } else {
                                    System.err.printf("❌ Failed to send message: %s%n", ex.getMessage());
                                }
                            })
                    );

            System.out.printf("📦 Successfully sent %d records from %s%n",
                    transactions.size(), csvFilePath.getFileName());

        } catch (Exception e) {
            System.err.printf("❌ Error processing %s: %s%n", csvFilePath.getFileName(), e.getMessage());
            e.printStackTrace();
        }
    }

    
    

	/*
	 * public void sendCsvDataToKafka(Path csvFilePath) { try (CSVReader reader =
	 * new CSVReader(new FileReader(csvFilePath.toFile()))) {
	 * 
	 * CsvToBean<Transaction> csvToBean = new CsvToBeanBuilder<Transaction>(reader)
	 * .withType(Transaction.class) .withIgnoreLeadingWhiteSpace(true)
	 * .withSkipLines(1) // Skip header .build();
	 * 
	 * List<Transaction> transactions = csvToBean.parse();
	 * 
	 * for (Transaction transaction : transactions) { String json =
	 * objectMapper.writeValueAsString(transaction); kafkaTemplate.send(topicName,
	 * transaction.getUserId(), json);
	 * System.out.printf("✅ Sent to Kafka [%s]: %s%n", topicName, json); }
	 * 
	 * System.out.printf("📦 Successfully sent %d records from %s%n",
	 * transactions.size(), csvFilePath.getFileName());
	 * 
	 * } catch (Exception e) { System.err.printf("❌ Error processing %s: %s%n",
	 * csvFilePath.getFileName(), e.getMessage()); e.printStackTrace(); } }
	 */
}
