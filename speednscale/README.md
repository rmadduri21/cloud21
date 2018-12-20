Data ingestion through Kafka and Spark streaming and Spark batch ingestion

The data from Kafka is picked up by Spark modules in microbatches to simulate near realtime data ingestion into HBase

Spark Modules after reading data from Kafka or Batch HDFS files, perform ETL, and formatting into HBase rows and then load into HBase

There are 5 modules:

1) Models Avro definition files for the structure of the meesages
2) DAO - Data Access Objects module
3) Transformations module for transforming incoming messages into Object formats
4) Infra structure module for connections etc.
5) Ingestion module for loading data into HBase

