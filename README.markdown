# Credit Card Fraud Detection Pipeline Using Hadoop Ecosystem

## Overview
This project develops a pipeline to detect fraudulent credit card transactions in near real-time using the Hadoop ecosystem. The pipeline ingests historical and streaming transaction data, calculates fraud detection parameters, and applies rules to classify transactions as genuine or fraudulent. It leverages tools like Sqoop, Hive, HBase, Spark Streaming, and Kafka to handle batch and real-time data processing.

---

## Input Data
The pipeline uses the following datasets:

1. **card_member** (Historical Data)
   - **Source**: AWS RDS
   - **Fields**:
     - `card_id`: Unique identifier for the credit card.
     - `member_id`: Unique identifier for the cardholder.
     - `member_joining_dt`: Date the member joined (format: YYYY-MM-DD).
     - `card_purchase_dt`: Date the card was purchased (format: YYYY-MM-DD).
     - `country`: Country of the member.
     - `city`: City of the member.

2. **member_score** (Historical Data)
   - **Source**: AWS RDS
   - **Fields**:
     - `member_id`: Unique identifier for the cardholder.
     - `score`: Credit score of the member (integer).

3. **card_transactions** (Historical Transaction Data)
   - **Source**: Local file system, ingested into HBase
   - **Fields**:
     - `card_id`: Unique identifier for the credit card.
     - `member_id`: Unique identifier for the cardholder.
     - `amount`: Transaction amount (float).
     - `postcode`: Postal code of the transaction location.
     - `pos_id`: Point of Sale terminal identifier.
     - `transaction_dt`: Transaction timestamp (format: YYYY-MM-DD HH:MM:SS).
     - `status`: Transaction status (`Genuine` or `Fraudulent`).

4. **Transactional Payload** (Real-Time Streaming Data)
   - **Source**: Kafka topic (sent from POS terminals)
   - **Fields**:
     - `card_id`: Unique identifier for the credit card.
     - `member_id`: Unique identifier for the cardholder.
     - `amount`: Transaction amount (float).
     - `pos_id`: Point of Sale terminal identifier.
     - `postcode`: Postal code of the transaction location.
     - `transaction_dt`: Transaction timestamp (format: YYYY-MM-DD HH:MM:SS).

---

## Architecture and Approach
The pipeline combines batch and real-time processing to detect fraudulent transactions:

1. **Batch Processing**:
   - Ingest historical data (`card_member`, `member_score`) from AWS RDS into HDFS using Sqoop.
   - Load historical `card_transactions` data into HBase via Hive for efficient lookups.
   - Calculate fraud detection parameters (Upper Control Limit, Credit Score, Zip Code Distance) and store them in an HBase lookup table.

2. **Real-Time Processing**:
   - Use Spark Streaming to consume real-time transaction data from a Kafka topic.
   - Query the HBase lookup table to apply fraud detection rules.
   - Update the `card_transactions` table and the lookup table based on the transaction status.

---

## Data Ingestion

### 1. Ingest Historical Data from AWS RDS Using Sqoop
#### a. Sqoop Commands
- **card_member** Table:
  - Full ingestion command:
    ```bash
    sqoop import \
      --connect jdbc:mysql://<aws-rds-endpoint>:3306/<database> \
      --username <username> \
      --password <password> \
      --table card_member \
      --target-dir /user/hadoop/card_member \
      --fields-terminated-by ',' \
      --m 1
    ```
  - **Incremental Ingestion**:
    - Use `member_joining_dt` as the incremental check column.
    - Create a Sqoop job for incremental updates:
      ```bash
      sqoop job --create card_member_job \
        -- import \
        --connect jdbc:mysql://<aws-rds-endpoint>:3306/<database> \
        --username <username> \
        --password <password> \
        --table card_member \
        --target-dir /user/hadoop/card_member \
        --check-column member_joining_dt \
        --incremental append \
        --last-value "2025-01-01" \
        --m 1
      ```
    - Run the job periodically:
      ```bash
      sqoop job --exec card_member_job
      ```

- **member_score** Table:
  - Full ingestion command:
    ```bash
    sqoop import \
      --connect jdbc:mysql://<aws-rds-endpoint>:3306/<database> \
      --username <username> \
      --password <password> \
      --table member_score \
      --target-dir /user/hadoop/member_score \
      --fields-terminated-by ',' \
      --m 1
    ```
  - **Incremental Ingestion**:
    - Use Sqoop job for incremental updates (if scores are updated over time):
      ```bash
      sqoop job --create member_score_job \
        -- import \
        --connect jdbc:mysql://<aws-rds-endpoint>:3306/<database> \
        --username <username> \
        --password <password> \
        --table██

---

### 2. Load `card_transactions` into HBase
#### a. Copy Data to HDFS
- Copy the `card_transactions` file from the local file system to HDFS:
  ```bash
  hdfs dfs -put card_transactions.csv /user/hadoop/card_transactions
  ```

#### b. Create Hive Table and Load Data
- Create a raw Hive table to store the `card_transactions` data:
  ```sql
  CREATE EXTERNAL TABLE card_transactions_raw (
    card_id STRING,
    member_id STRING,
    amount DOUBLE,
    postcode STRING,
    pos_id STRING,
    transaction_dt STRING,
    status STRING
  )
  ROW FORMATsexport FORMAT CSV
  WITH (field_delimiter=',')
  STORED AS TEXTFILE
  LOCATION '/user/hadoop/card_transactions';
  ```

#### c. Create Hive-HBase Integrated Table
- Define a row key as `card_id` for efficient lookups in HBase.
- Create an HBase table:
  ```bash
  create 'card_transactions_hbase', {NAME => 'cf', VERSIONS => 1}
  ```
- Create a Hive table integrated with HBase:
  ```sql
  CREATE TABLE card_transactions_hive_hbase (
    card_id STRING,
    member_id STRING,
    amount DOUBLE,
    postcode STRING,
   †

pos_id STRING,
    transaction_dt STRING,
    status STRING
  )
  STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
  WITH SERDEPROPERTIES ('hbase.table.name' = 'card_transactions_hbase')
  TBLPROPERTIES ('hbase.columns.mapping' = ':key,cf:member_id,cf:amount,cf:postcode,cf:pos_id,cf:transaction_dt,cf:status');
  ```

#### d. Create HBase Lookup Table
- Create an HBase table for storing fraud detection parameters (UCL, last postcode, last transaction timestamp):
  ```bash
  create 'fraud_lookup', {NAME => 'cf', VERSIONS => 1}
  ```
- The lookup table will map:
  - Row key: `card_id`
  - Column family: `cf`
  - Columns: `ucl`, `last_postcode`, `last_transaction_dt`

---

## Fraud Detection Parameters

### 1. Upper Control Limit (UCL)
- **Definition**: UCL = Moving Average + 3 * Standard Deviation
- Calculated based on the last 10 genuine transactions for each `card_id`.

#### Steps:
a. **Create Staging Table for Moving Average and Standard Deviation**
   - Create a Hive table to store intermediate results:
     ```sql
     CREATE TABLE card_transactions_stats (
       card_id STRING,
       moving_avg DOUBLE,
       std_dev DOUBLE
     )
     STORED AS ORC;
     ```
   - Compute moving average and standard deviation for the last 10 genuine transactions:
     ```sql
     INSERT INTO card_transactions_stats
     SELECT
       card_id,
       AVG(amount) OVER (PARTITION BY card_id ORDER BY transaction_dt ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as moving_avg,
       STDDEV(amount) OVER (PARTITION BY card_id ORDER BY transaction_dt ROWS BETWEEN 9 PRECEDING AND CURRENT ROW) as std_dev
     FROM card_transactions_hive_hbase
     WHERE status = 'Genuine';
     ```

b. **Create Final Table for UCL**
   - Create a Hive table to store UCL values:
     ```sql
     CREATE TABLE card_transactions_ucl (
       card_id STRING,
       ucl DOUBLE
     )
     STORED AS ORC;
     ```
   - Calculate UCL:
     ```sql
     INSERT INTO card_transactions_ucl
     SELECT
       card_id,
       moving_avg + (3 * std_dev) as ucl
     FROM card_transactions_stats;
     ```

c. **Store UCL in HBase Lookup Table**
   - Use a script (e.g., in Python with HappyBase) to update the `fraud_lookup` table:
     - For each row in `card_transactions_ucl`, update HBase:
       ```python
       import happybase
       connection = happybase.Connection('localhost')
       table = connection.table('fraud_lookup')
       for row in card_transactions_ucl_data:
           table.put(row['card_id'], {'cf:ucl': str(row['ucl'])})
       ```

- **Update Frequency**: Recalculate UCL at regular intervals (e.g., every hour) to keep the lookup table up-to-date.

### 2. Credit Score Check
- **Rule**: If `score < 200`, the transaction is fraudulent.
- **Source**: Join `card_transactions` with `member_score` to get the score for each transaction.

#### Steps:
a. **Join and Flag Transactions**
   - Create a temporary table to flag potentially fraudulent transactions:
     ```sql
     CREATE TABLE card_transactions_score_check (
       card_id STRING,
       member_id STRING,
       amount DOUBLE,
       postcode STRING,
       pos_id STRING,
       transaction_dt STRING,
       status STRING,
       score INT
     )
     STORED AS ORC;
     ```
   - Perform the join:
     ```sql
     INSERT INTO card_transactions_score_check
     SELECT
       t.card_id,
       t.member_id,
       t.amount,
       t.postcode,
       t.pos_id,
       t.transaction_dt,
       CASE WHEN m.score < 200 THEN 'Fraudulent' ELSE t.status END as status,
       m.score
     FROM card_transactions_hive_hbase t
     JOIN member_score m ON t.member_id = m.member_id;
     ```

b. **Update Frequency**: Recompute every 4 hours to account for score updates.

### 3. Zip Code Distance
- **Rule**: Calculate the distance between the current transaction's postcode and the last transaction's postcode. Compute the speed (distance/time). If the speed exceeds a threshold (e.g., 500 km/h), mark the transaction as fraudulent.
- **Dependencies**: Use a provided postcode library to calculate distances.

#### Steps:
a. **Retrieve Last Transaction Details**
   - Fetch `last_postcode` and `last_transaction_dt` from the `fraud_lookup` table for the given `card_id`.

b. **Calculate Distance and Speed**
   - Use the postcode library to compute the distance between the current and last postcode.
   - Compute speed:
     \[
     \text{Speed} = \frac{\text{Distance (km)}}{\text{Time Difference (hours)}}
     \]
   - If speed > 500 km/h, mark the transaction as fraudulent.

c. **Update Lookup Table**
   - If the transaction is genuine, update the `fraud_lookup` table with the current `postcode` and `transaction_dt`.

---

## Real-Time Processing with Spark Streaming

### 1. Connect to Kafka Topic
- Consume real-time transactional data from the Kafka topic.
- **Topic**: `transactional_payload`
- **Fields**: `card_id`, `member_id`, `amount`, `pos_id`, `postcode`, `transaction_dt`

### 2. Process Streaming Data
- Use Spark Streaming to process the data in micro-batches (e.g., every 5 seconds).
- For each transaction:
  a. **Query Lookup Table**
     - Retrieve `ucl`, `last_postcode`, and `last_transaction_dt` from the `fraud_lookup` table for the `card_id`.
  b. **Apply Fraud Detection Rules**
     - **UCL Check**: If `amount > ucl`, mark as `Fraudulent`.
     - **Credit Score Check**: Join with `member_score` (cached in memory) to get the score. If `score < 200`, mark as `Fraudulent`.
     - **Zip Code Distance Check**: Calculate speed using the postcode library. If speed > 500 km/h, mark as `Fraudulent`.
  c. **Write Results**
     - Update the `card_transactions_hive_hbase` table with the transaction and its `status`.
  d. **Update Lookup Table**
     - If the transaction is `Genuine`, update the `fraud_lookup` table with the current `postcode` and `transaction_dt`.

### 3. SLA
- Ensure fraud detection and updates occur within **seconds** to meet real-time requirements.

---

## Output
- The `card_transactions_hive_hbase` table will be updated with the `status` of each transaction (`Genuine` or `Fraudulent`).
- The `fraud_lookup` table will be updated with the latest `postcode` and `transaction_dt` for genuine transactions.

---

## Tools and Technologies
- **Hadoop Ecosystem**:
  - HDFS: Storage for raw data.
  - Sqoop: Ingest data from AWS RDS.
  - Hive: Batch processing and analytics.
  - HBase: NoSQL database for fast lookups.
- **Streaming**:
  - Kafka: Real-time data ingestion.
  - Spark Streaming: Process streaming data.
- **Programming**:
  - Python: For HBase updates and postcode library integration.
- **Libraries**:
  - Provided postcode library for distance calculations.

---

## Future Improvements
- **Scalability**: Optimize Hive queries and HBase writes for larger datasets.
- **Accuracy**: Fine-tune fraud detection thresholds (e.g., UCL multiplier, speed threshold).
- **Monitoring**: Add logging and alerts for pipeline failures or delays.
- **Security**: Secure Kafka topics and HBase access with authentication.