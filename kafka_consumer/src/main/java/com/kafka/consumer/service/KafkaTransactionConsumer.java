package com.kafka.consumer.service;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.consumer.model.Transaction;

import com.kafka.consumer.repository.TransactionRepository;

@Service
public class KafkaTransactionConsumer {

    private final TransactionRepository nativeRepository;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${consumer.batch.size:50}")
    private int batchSize;

    @Value("${consumer.flush.interval.ms:60000}")
    private long flushIntervalMs;

    // ThreadLocal buffer → one buffer per consumer thread
    private final ThreadLocal<List<Transaction>> transactionBuffer =
            ThreadLocal.withInitial(ArrayList::new);

    // Last flush timestamp for each thread
    private final ThreadLocal<Long> lastFlushTime =
            ThreadLocal.withInitial(System::currentTimeMillis);

    public KafkaTransactionConsumer(TransactionRepository nativeRepository) {
        this.nativeRepository = nativeRepository;
    }

    @Transactional //If even one fails → all records are rolled back ❌, so no partial writes.
    @KafkaListener(
        topics = "${kafka.topic.name}",
        groupId = "${spring.kafka.consumer.group-id}",
        concurrency = "2" // Two consumer threads per instance
    )
    public void consume(ConsumerRecord<String, String> record, Acknowledgment acknowledgment) {
        try {
            Transaction transaction = objectMapper.readValue(record.value(), Transaction.class);

            // Add to thread-local buffer
            List<Transaction> buffer = transactionBuffer.get();
            buffer.add(transaction);

            System.out.printf(
                "📥 Thread %s buffered Txn: %s | 🕒 %s | Partition: %d | Offset: %d | Key: %s%n",
                Thread.currentThread().getName(),
                transaction.getTransactionId(),
                Instant.ofEpochMilli(record.timestamp()),
                record.partition(),
                record.offset(),
                record.key(),record.value()
            );

            long now = System.currentTimeMillis();

            // Flush if batch size reached or time exceeded
            if (buffer.size() >= batchSize || now - lastFlushTime.get() >= flushIntervalMs) {
                flushAndCommit(buffer, acknowledgment);
                lastFlushTime.set(now);
            }

        } catch (Exception e) {
            System.err.printf("❌ Failed to process record: %s%n", e.getMessage());
            e.printStackTrace();
        }
    }

    private void flushAndCommit(List<Transaction> buffer, Acknowledgment acknowledgment) {
        if (!buffer.isEmpty()) {
            for (Transaction t : buffer) {
                // Use INSERT IGNORE to skip duplicates
                nativeRepository.insertIgnore(
                        t.getTransactionId(),
                        t.getCostPerItem(),
                        t.getCountry(),
                        t.getItemCode(),
                        t.getItemDescription(),
                        t.getNumberOfItemPurchased(),
                        t.getTransactionTime(),
                        t.getUserId()
                );
            }

            System.out.printf(
                "💾 Thread %s inserted batch of %d transactions (duplicates skipped)%n",
                Thread.currentThread().getName(),
                buffer.size()
            );

            buffer.clear();
            acknowledgment.acknowledge(); // Commit Kafka offset
        }
    }
}






















/*
 * package com.kafka.consumer.service;
 * 
 * import java.time.Instant; import java.util.ArrayList; import java.util.List;
 * 
 * import org.apache.kafka.clients.consumer.ConsumerRecord; import
 * org.springframework.beans.factory.annotation.Value; import
 * org.springframework.kafka.annotation.KafkaListener; import
 * org.springframework.kafka.support.Acknowledgment; import
 * org.springframework.stereotype.Service; import
 * org.springframework.transaction.annotation.Transactional;
 * 
 * import com.fasterxml.jackson.databind.ObjectMapper; import
 * com.kafka.consumer.model.Transaction; import
 * com.kafka.consumer.repository.TransactionRepository;
 * 
 * @Service public class KafkaTransactionConsumer {
 * 
 * private final TransactionRepository repository; private final ObjectMapper
 * objectMapper = new ObjectMapper();
 * 
 * @Value("${consumer.batch.size:50}") private int batchSize;
 * 
 * @Value("${consumer.flush.interval.ms:60000}") private long flushIntervalMs;
 * 
 * // ThreadLocal buffer → one buffer per consumer thread private final
 * ThreadLocal<List<Transaction>> transactionBuffer =
 * ThreadLocal.withInitial(ArrayList::new);
 * 
 * // Last flush timestamp for each thread private final ThreadLocal<Long>
 * lastFlushTime = ThreadLocal.withInitial(System::currentTimeMillis);
 * 
 * public KafkaTransactionConsumer(TransactionRepository repository) {
 * this.repository = repository; }
 * 
 * @Transactional
 * 
 * @KafkaListener( topics = "${kafka.topic.name}", groupId =
 * "${spring.kafka.consumer.group-id}", concurrency = "2" // Two consumer
 * threads per instance ) public void consume(ConsumerRecord<String, String>
 * record, Acknowledgment acknowledgment) { try { Transaction transaction =
 * objectMapper.readValue(record.value(), Transaction.class);
 * 
 * // Add to thread-local buffer List<Transaction> buffer =
 * transactionBuffer.get(); buffer.add(transaction);
 * 
 * System.out.printf(
 * "📥 Thread %s buffered Txn: %s | 🕒 %s | Partition: %d | Offset: %d | Key: %s%n"
 * , Thread.currentThread().getName(), transaction.getTransactionId(),
 * Instant.ofEpochMilli(record.timestamp()), record.partition(),
 * record.offset(), record.key() );
 * 
 * long now = System.currentTimeMillis();
 * 
 * // Flush if batch size reached or time exceeded if (buffer.size() >=
 * batchSize || now - lastFlushTime.get() >= flushIntervalMs) {
 * flushAndCommit(buffer, acknowledgment); lastFlushTime.set(now); }
 * 
 * } catch (Exception e) { System.err.printf("❌ Failed to process record: %s%n",
 * e.getMessage()); e.printStackTrace(); } }
 * 
 * private void flushAndCommit(List<Transaction> buffer, Acknowledgment
 * acknowledgment) { if (!buffer.isEmpty()) { repository.saveAll(buffer); //
 * Saves in a single transaction System.out.printf(
 * "💾 Thread %s saved batch of %d transactions to DB%n",
 * Thread.currentThread().getName(), buffer.size() ); buffer.clear();
 * acknowledgment.acknowledge(); // Commit Kafka offset
 * 
 * } } }
 */