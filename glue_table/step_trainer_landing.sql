CREATE EXTERNAL TABLE IF NOT EXISTS  `stedi_human_balance`.`step_trainer_landing` (
    `sensorreadingtime` bigint,
    `serialnumber` string,
    `distanceFromObject` int
)
ROW FORMAT SERDE
    'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ( 
    'paths'='sensorReadingTime,serialNumber,distanceFromObject',
    'case.insensitive' = 'TRUE',
    'mapping' = 'TRUE',
    'ignore.malformed.json' = 'FALSE',
    'dots.in.keys' = 'FALSE'
)
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
    's3://stedi-project/step_trainer/landing/'
TBLPROPERTIES ('classification' = 'json');