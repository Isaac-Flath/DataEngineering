CREATE EXTERNAL TABLE `customer_landing`(
  `serialnumber` string COMMENT 'from deserializer',
  `sharewithpublicasofdate` decimal(38,18) COMMENT 'from deserializer',
  `birthday` date COMMENT 'from deserializer',
  `registrationdate` bigint COMMENT 'from deserializer',
  `sharewithresearchasofdate` decimal(38,18) COMMENT 'from deserializer',
  `customername` string COMMENT 'from deserializer',
  `email` string COMMENT 'from deserializer',
  `lastupdatedate` bigint COMMENT 'from deserializer',
  `phone` bigint COMMENT 'from deserializer',
  `sharewithfriendsasofdate` decimal(38,18) COMMENT 'from deserializer')
ROW FORMAT SERDE
  'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'case.insensitive'='TRUE',
  'dots.in.keys'='FALSE',
  'ignore.malformed.json'='FALSE',
  'mapping'='TRUE')
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://isaac-data-lakehouse/customer/landing'
TBLPROPERTIES (
  'classification'='json',
  'transient_lastDdlTime'='1697397354')