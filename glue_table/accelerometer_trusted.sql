CREATE EXTERNAL TABLE IF NOT EXISTS `stedi_human_balance`.`accelerometer_trusted` (
    `user` string,
    `timeStamp` bigint,
    `x` float,
    `y` float,
    `z` float
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES(
    'paths'='user,timeStamp,x,y,x',
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
    's3://stedi-project/accelerometer/trusted/'
TBLPROPERTIES ('classification' = 'json');