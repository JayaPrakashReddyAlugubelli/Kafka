package com.kafka.consumer.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.kafka.consumer.model.Transaction;

@Repository
public interface TransactionRepository extends JpaRepository<Transaction, String> {
	
	    @Modifying
	    @Transactional
	    @Query(value = "INSERT IGNORE INTO transaction " +
	            "(transaction_id, cost_per_item, country, item_code, item_description, number_of_item_purchased, transaction_time, user_id) " +
	            "VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)", nativeQuery = true)
	    void insertIgnore(String transactionId, double costPerItem, String country, String itemCode,
	                      String itemDescription, int numberOfItemPurchased, String transactionTime, String userId);
}
