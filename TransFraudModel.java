package com.pack;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

public class TransactionData implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonProperty("card_id")
    private Long cardId;

    @JsonProperty("member_id")
    private Long memberId;

    @JsonProperty("amount")
    private Double amount;

    @JsonProperty("pos_id")
    private Long posId;

    @JsonProperty("postcode")
    private Integer postcode;

    @JsonProperty("transaction_dt")
    private String transactionDate;

    public TransactionData() {}

    public TransactionData(long cardId, long memberId, double amount, long posId, int postcode, String transactionDate) {
        this.cardId = cardId;
        this.memberId = memberId;
        this.amount = amount;
        this.posId = posId;
        this.postcode = postcode;
        this.transactionDate = transactionDate;
    }

    public Long getCardId() { return cardId; }
    public void setCardId(long cardId) { this.cardId = cardId; }

    public Long getMemberId() { return memberId; }
    public void setMemberId(long memberId) { this.memberId = memberId; }

    public Double getAmount() { return amount; }
    public void setAmount(double amount) { this.amount = amount; }

    public Long getPosId() { return posId; }
    public void setPosId(long posId) { this.posId = posId; }

    public Integer getPostcode() { return postcode; }
    public void setPostcode(int postcode) { this.postcode = postcode; }

    public String getTransactionDate() { return transactionDate; }
    public void setTransactionDate(String transactionDate) { this.transactionDate = transactionDate; }

    @Override
    public String toString() {
        return String.format("%d,%.2f,%d,%d,%d,%s", cardId, amount, memberId, posId, postcode, transactionDate);
    }
}
