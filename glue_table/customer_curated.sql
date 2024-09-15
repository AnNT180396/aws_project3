CREATE EXTERNAL TABLE IF NOT EXISTS `stedi_human_balance`.`customer_curated`(
    `customername` string,
    `email` string,
    `phone` string,
    `birthday` string,
    `serialnumber` string,
    `registrationdate` bigint,
    `lastupdatedate` bigint,
    `sharewithresearchasofdate` bigint,
    `sharewithpublicasofdate` bigint,
    `sharewithfriendsasofdate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
    'paths'='customerName,email,phone,birthDay,serialNumber,registrationDate,lastUpdateDate,shareWithResearchAsOfDate,shareWithPublicAsOfDate, shareWithFriendsAsOfDate',
    'case.insensitive' = 'TRUE',
    'mapping' = 'TRUE',
    'dots.in.keys' = 'FALSE',
    'ignore.malformed.json' = 'FALSE'
)
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    's3://stedi-project/customer/curated/'
TBLPROPERTIES ('classification'='json');