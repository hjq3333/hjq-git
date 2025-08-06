商品基础信息表 ods_shop_product_info
   
   CREATE TABLE ods_shop_product_info (
       product_id STRING COMMENT '商品ID',
       product_name STRING COMMENT '商品名称',
       category_id STRING COMMENT '商品分类ID',
       brand STRING COMMENT '品牌',
       base_price DECIMAL(10,2) COMMENT '基础价格',
       sku_info STRING COMMENT 'SKU信息（JSON格式，含颜色、规格等）',
       is_price_strength TINYINT COMMENT '是否为价格力商品（1是，0否）',
       create_time STRING COMMENT '创建时间'
   ) COMMENT '商品基础信息原始表'
   PARTITIONED BY (dt STRING)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
   STORED AS TEXTFILE;
   
订单明细表 ods_order_detail
   
   CREATE TABLE ods_order_detail (
       order_id STRING COMMENT '订单ID',
       product_id STRING COMMENT '商品ID',
       sku_id STRING COMMENT 'SKU ID',
       pay_amount DECIMAL(10,2) COMMENT '支付金额',
       sales_num INT COMMENT '销量',
       pay_buyer_num INT COMMENT '支付买家数',
       pay_time STRING COMMENT '支付时间'
   ) COMMENT '订单明细原始表'
   PARTITIONED BY (dt STRING)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
   STORED AS TEXTFILE;
   
流量信息表 ods_traffic_info
   
   CREATE TABLE ods_traffic_info (
       product_id STRING COMMENT '商品ID',
       traffic_source STRING COMMENT '流量来源（如效果广告、手淘搜索等）',
       visitor_num INT COMMENT '访客数',
       search_word STRING COMMENT '搜索词',
       visit_time STRING COMMENT '访问时间'
   ) COMMENT '流量信息原始表'
   PARTITIONED BY (dt STRING)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
   STORED AS TEXTFILE;
   
库存信息表 ods_inventory_info
   
   CREATE TABLE ods_inventory_info (
       sku_id STRING COMMENT 'SKU ID',
       current_stock INT COMMENT '当前库存（件）',
       stock_update_time STRING COMMENT '库存更新时间'
   ) COMMENT '库存信息原始表'
   PARTITIONED BY (dt STRING)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
   STORED AS TEXTFILE;
   

价格力信息表 ods_price_strength_info
   
   CREATE TABLE ods_price_strength_info (
       product_id STRING COMMENT '商品ID',
       price_strength_star INT COMMENT '价格力星级',
       coupon_after_price DECIMAL(10,2) COMMENT '普惠券后价',
       price_strength_warn STRING COMMENT '价格力预警状态（如持续低星、高价识别）',
       visit_conversion_rate DECIMAL(5,2) COMMENT '访问转化率',
       pay_conversion_rate DECIMAL(5,2) COMMENT '支付转化率',
       conversion_rate环比变化 DECIMAL(5,2) COMMENT '转化率环比变化'
   ) COMMENT '价格力与商品力原始表'
   PARTITIONED BY (dt STRING)
   ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
   STORED AS TEXTFILE;

