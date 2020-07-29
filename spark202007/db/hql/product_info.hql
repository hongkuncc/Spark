CREATE TABLE `product_info`(
  `product_id` bigint,
  `product_name` string,
  `extend_info` string)
row format delimited fields terminated by '\t';
load data local inpath '/opt/module/datas/product_info.txt' into table sparkpractice.product_info;
