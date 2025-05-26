package com.pack;

import java.io.IOException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class HbaseClient {
    private static Connection hBaseConnection;
    private static final String MASTER_TABLE = "card_transactions_master";
    private static final String LOOKUP_TABLE = "card_transaction_lookup";

    private static Connection getConnection(String hBaseMaster) throws IOException {
        if (hBaseConnection == null || hBaseConnection.isClosed()) {
            var conf = HBaseConfiguration.create();
            conf.set("hbase.master", hBaseMaster + ":60000");
            conf.set("hbase.zookeeper.quorum", hBaseMaster);
            conf.set("hbase.zookeeper.property.clientPort", "2181");
            conf.set("zookeeper.znode.parent", "/hbase");
            conf.setInt("timeout", 1200);
            hBaseConnection = ConnectionFactory.createConnection(conf);
        }
        return hBaseConnection;
    }

    public static TransactionLookupRecord getTransactionLookup(TransactionData data, String hBaseMaster) throws IOException {
        try (Connection conn = getConnection(hBaseMaster);
             Table table = conn.getTable(TableName.valueOf(LOOKUP_TABLE))) {
            Get get = new Get(Bytes.toBytes(data.getCardId().toString()));
            Result result = table.get(get);
            TransactionLookupRecord record = new TransactionLookupRecord(data.getCardId());

            byte[] value = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("ucl"));
            if (value != null) record.setUcl(Double.parseDouble(Bytes.toString(value)));

            value = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("score"));
            if (value != null) record.setScore(Integer.parseInt(Bytes.toString(value)));

            value = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("postcode"));
            if (value != null) record.setPostcode(Integer.parseInt(Bytes.toString(value)));

            value = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("transaction_dt"));
            if (value != null) record.setTransactionDate(Bytes.toString(value));

            return record;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void insertTransaction(TransactionData data, String hBaseMaster, String status) throws IOException {
        try (Connection conn = getConnection(hBaseMaster);
             Table masterTable = conn.getTable(TableName.valueOf(MASTER_TABLE));
             Table lookupTable = conn.getTable(TableName.valueOf(LOOKUP_TABLE))) {
            
            // Insert into master table
            String guid = java.util.UUID.randomUUID().toString().replace("-", "");
            Put put = new Put(Bytes.toBytes(guid));
            put.addColumn(Bytes.toBytes("cardDetail"), Bytes.toBytes("card_id"), Bytes.toBytes(data.getCardId().toString()))
               .addColumn(Bytes.toBytes("cardDetail"), Bytes.toBytes("member_id"), Bytes.toBytes(data.getMemberId().toString()))
               .addColumn(Bytes.toBytes("transactionDetail"), Bytes.toBytes("amount"), Bytes.toBytes(data.getAmount().toString()))
               .addColumn(Bytes.toBytes("transactionDetail"), Bytes.toBytes("postcode"), Bytes.toBytes(data.getPostcode().toString()))
               .addColumn(Bytes.toBytes("transactionDetail"), Bytes.toBytes("pos_id"), Bytes.toBytes(data.getPosId().toString()))
               .addColumn(Bytes.toBytes("transactionDetail"), Bytes.toBytes("transaction_dt"), Bytes.toBytes(data.getTransactionDate()))
               .addColumn(Bytes.toBytes("transactionDetail"), Bytes.toBytes("status"), Bytes.toBytes(status));
            masterTable.put(put);

            // Update lookup table if transaction is genuine
            if ("GENUINE".equalsIgnoreCase(status)) {
                put = new Put(Bytes.toBytes(data.getCardId().toString()));
                put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("postcode"), Bytes.toBytes(data.getPostcode().toString()))
                   .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("transaction_dt"), Bytes.toBytes(data.getTransactionDate()));
                lookupTable.put(put);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
