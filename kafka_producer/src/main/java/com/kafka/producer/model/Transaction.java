package com.kafka.producer.model;

import java.util.Objects;

public class Transaction {
    private String userId;
    private String transactionId;
    private String transactionTime;
    private String itemCode;
    private String itemDescription;
    private int numberOfItemPurchased;
    private double costPerItem;
    private String country;

    public Transaction() {}

    public Transaction(String userId, String transactionId, String transactionTime, String itemCode,
                       String itemDescription, int numberOfItemPurchased, double costPerItem, String country) {
        this.userId = userId;
        this.transactionId = transactionId;
        this.transactionTime = transactionTime;
        this.itemCode = itemCode;
        this.itemDescription = itemDescription;
        this.numberOfItemPurchased = numberOfItemPurchased;
        this.costPerItem = costPerItem;
        this.country = country;
    }

    // Getters & Setters
    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }
    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
    public String getTransactionTime() { return transactionTime; }
    public void setTransactionTime(String transactionTime) { this.transactionTime = transactionTime; }
    public String getItemCode() { return itemCode; }
    public void setItemCode(String itemCode) { this.itemCode = itemCode; }
    public String getItemDescription() { return itemDescription; }
    public void setItemDescription(String itemDescription) { this.itemDescription = itemDescription; }
    public int getNumberOfItemPurchased() { return numberOfItemPurchased; }
    public void setNumberOfItemPurchased(int numberOfItemPurchased) { this.numberOfItemPurchased = numberOfItemPurchased; }
    public double getCostPerItem() { return costPerItem; }
    public void setCostPerItem(double costPerItem) { this.costPerItem = costPerItem; }
    public String getCountry() { return country; }
    public void setCountry(String country) { this.country = country; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Transaction)) return false;
        Transaction that = (Transaction) o;
        return numberOfItemPurchased == that.numberOfItemPurchased &&
                Double.compare(that.costPerItem, costPerItem) == 0 &&
                Objects.equals(userId, that.userId) &&
                Objects.equals(transactionId, that.transactionId) &&
                Objects.equals(transactionTime, that.transactionTime) &&
                Objects.equals(itemCode, that.itemCode) &&
                Objects.equals(itemDescription, that.itemDescription) &&
                Objects.equals(country, that.country);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, transactionId, transactionTime, itemCode, itemDescription,
                numberOfItemPurchased, costPerItem, country);
    }
}
