无线端入店与承接原始表（ods_wireless_entry）

    
    CREATE TABLE IF NOT EXISTS ods_wireless_entry (
        page_id STRING COMMENT '页面唯一标识',
        page_type STRING COMMENT '页面类型（店铺页/商品详情页/店铺其他页）',
        visitor_id STRING COMMENT '访客唯一标识',
        visit_time TIMESTAMP COMMENT '访问时间（精确到秒）',
        is_order TINYINT COMMENT '是否下单（1=是，0=否）',
        data_date DATE COMMENT '数据日期'
    ) COMMENT '无线端入店与承接原始数据，存储访客访问及下单行为明细'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

店内路径流转明细表（ods_instore_path）

    
    CREATE TABLE IF NOT EXISTS ods_instore_path (
        path_record_id STRING COMMENT '路径记录唯一标识',
        visitor_id STRING COMMENT '访客唯一标识',
        source_page_id STRING COMMENT '来源页面ID',
        source_page_type STRING COMMENT '来源页面类型',
        target_page_id STRING COMMENT '去向页面ID',
        target_page_type STRING COMMENT '去向页面类型',
        jump_time DATETIME COMMENT '页面跳转时间',
        visit_sequence INT COMMENT '访问顺序'
    ) COMMENT '店内路径流转原始数据，记录访客页面跳转明细'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

PC 端流量入口表（ods_pc_traffic_entry）

    
    CREATE TABLE IF NOT EXISTS ods_pc_traffic_entry (
        source_page_id STRING COMMENT '来源页面唯一标识',
        source_page_name STRING COMMENT '来源页面名称',
        source_page_url STRING COMMENT '来源页面URL',
        visitor_id STRING COMMENT '访客唯一标识',
        visit_time DATETIME COMMENT '访问时间',
        data_date DATE COMMENT '数据日期'
    ) COMMENT 'PC端流量入口原始数据，记录访客来源页面信息'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;


页面访问排行原始表（ods_page_visit_rank）

    
    CREATE TABLE IF NOT EXISTS ods_page_visit_rank (
        page_id STRING COMMENT '页面唯一标识',
        page_type STRING COMMENT '页面类型',
        visitor_id STRING COMMENT '访客唯一标识',
        visit_time DATETIME COMMENT '访问时间',
        leave_time DATETIME COMMENT '离开页面时间',
        data_date DATE COMMENT '数据日期'
    ) COMMENT '页面访问排行原始数据，记录页面访问及停留明细'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;

店铺页细分类型访问原始表（ods_shop_page_visit_detail）

    
    CREATE TABLE IF NOT EXISTS ods_shop_page_visit_detail (
        shop_page_id STRING COMMENT '店铺页唯一标识',
        shop_page_subtype STRING COMMENT '店铺页细分类型（首页/活动页等）',
        visitor_id STRING COMMENT '访客唯一标识',
        visit_time DATETIME COMMENT '访问时间',
        leave_time DATETIME COMMENT '离开页面时间',
        data_date DATE COMMENT '数据日期'
    ) COMMENT '店铺页细分类型访问原始数据，记录细分页面的访问明细'
    PARTITIONED BY (dt STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS TEXTFILE;