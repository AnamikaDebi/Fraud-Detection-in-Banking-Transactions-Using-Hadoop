package com.pack;

import java.io.Serializable;

public class TransactionLookupRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long cardId;
    private Double ucl;
    private Integer postcode;
    private String transactionDate;
    private Integer score;

    public TransactionLookupRecord(Long cardId) {
        this.cardId = cardId;
    }

    public Long getCardId() { return cardId; }
    public void setCardId(Long cardId) { this.cardId = cardId; }

    public Double getUcl() { return ucl; }
    public void setUcl(Double ucl) { this.ucl = ucl; }

    public Integer getPostcode() { return postcode; }
    public void setPostcode(Integer postcode) { this.postcode = postcode; }

    public String getTransactionDate() { return transactionDate; }
    public void setTransactionDate(String transactionDate) { this.transactionDate = transactionDate; }

    public Integer getScore() { return score; }
    public void setScore(Integer score) { this.score = score; }

    @Override
    public String toString() {
        return String.format("%d,%.2f,%d,%d,%s", cardId, ucl, postcode, score, transactionDate);
    }
}
