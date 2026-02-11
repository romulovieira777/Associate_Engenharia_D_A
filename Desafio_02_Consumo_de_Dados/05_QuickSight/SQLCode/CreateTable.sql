CREATE EXTERNAL TABLE `tb_cap_qs_saas_sales`(
 row_id bigint,
 order_id string,
 order_date string,
 date_key bigint,
 contact_name string,
 country_en string,
 city_en string,
 region string,
 subregion string,
 customer string,
 customer_id bigint,
 industry_en string,
 segment string,
 product string,
 license string,
 sales double,
 quantity bigint,
 discount double,
 profit double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES('separatorChar'=',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://<SEU BUCKET AQUI>/tb_cap_qs_saas_sales'
TBLPROPERTIES ('classification'='csv','skip.header.line.count'='1')