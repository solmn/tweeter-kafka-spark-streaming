CREATE EXTERNAL TABLE tweets(
 id String,
 hashTages String,
 count String
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
 "hbase.columns.mapping" = ":key, tweet:hashTages, Popularity:count"
)
TBLPROPERTIES(
 "hbase.table.name" = "tweets",
 "hbase.mapred.output.outputtable" = "tweets"
);