set hive.exec.local.mode.auto=True;
drop database if exists dwd;
create database dwd
    LOCATION 'hdfs://cdh01:8020/bigdata_warehouse/gmall/dwd/';


use gmall;


DROP TABLE IF EXISTS dwd.dwd_trade_cart_add_inc;
CREATE EXTERNAL TABLE dwd.dwd_trade_cart_add_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户ID',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `date_id`     STRING COMMENT '日期ID',
    `create_time` STRING COMMENT '加购时间',
    `sku_num`     BIGINT COMMENT '加购物车件数'
) COMMENT '交易域加购事务事实表'
    PARTITIONED BY (`dt` STRING)
    STORED AS ORC
    LOCATION '/bigdata_warehouse/gmall/dwd/dwd_trade_cart_add_inc'
    TBLPROPERTIES ('orc.compress' = 'snappy');

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd.dwd_trade_cart_add_inc partition (dt)
select data.id,
       data.user_id,
       data.sku_id,
       date_format(data.create_time, 'yyyy-MM-dd') date_id,
       data.create_time,
       data.sku_num,
       date_format(data.create_time, 'yyyy-MM-dd')
from ods_cart_info data
where dt = '20250630';

select *
from dwd.dwd_trade_cart_add_inc;



DROP TABLE IF EXISTS dwd.dwd_trade_order_detail_inc;
CREATE EXTERNAL TABLE dwd.dwd_trade_order_detail_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单ID',
    `user_id`               STRING COMMENT '用户ID',
    `sku_id`                STRING COMMENT '商品ID',
    `province_id`           STRING COMMENT '省份ID',
    `activity_id`           STRING COMMENT '参与活动ID',
    `activity_rule_id`      STRING COMMENT '参与活动规则ID',
    `coupon_id`             STRING COMMENT '使用优惠券ID',
    `date_id`               STRING COMMENT '下单日期ID',
    `create_time`           STRING COMMENT '下单时间',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '原始价格',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '优惠券优惠分摊',
    `split_total_amount`    DECIMAL(16, 2) COMMENT '最终价格分摊'
) COMMENT '交易域下单事务事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/bigdata_warehouse/gmall/dwd/dwd_trade_order_detail_inc';


set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd.dwd_trade_order_detail_inc partition (dt)
select od.id,
       order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       date_format(create_time, 'yyyy-MM-dd') date_id,
       create_time,
       sku_num,
       split_original_amount,
       nvl(split_activity_amount, 0.0),
       nvl(split_coupon_amount, 0.0),
       split_total_amount,
       date_format(create_time, 'yyyy-MM-dd')
from (select data.id,
             data.order_id,
             data.sku_id,
             data.create_time,
             data.sku_num,
             data.sku_num * data.order_price split_original_amount,
             data.split_total_amount,
             data.split_activity_amount,
             data.split_coupon_amount
      from ods_order_detail data
      where dt = '20250630') od
         left join
     (select data.id,
             data.user_id,
             data.province_id
      from ods_order_info data
      where dt = '20250630') oi
     on od.order_id = oi.id
         left join
     (select data.order_detail_id,
             data.activity_id,
             data.activity_rule_id
      from ods_order_detail_activity data
      where dt = '20250630') act
     on od.id = act.order_detail_id
         left join
     (select data.order_detail_id,
             data.coupon_id
      from ods_order_detail_coupon data
      where dt = '20250630') cou
     on od.id = cou.order_detail_id;

select *
from dwd.dwd_trade_order_detail_inc;



DROP TABLE IF EXISTS dwd.dwd_trade_pay_detail_suc_inc;
CREATE EXTERNAL TABLE dwd.dwd_trade_pay_detail_suc_inc
(
    `id`                    STRING COMMENT '编号',
    `order_id`              STRING COMMENT '订单ID',
    `user_id`               STRING COMMENT '用户ID',
    `sku_id`                STRING COMMENT 'SKU_ID',
    `province_id`           STRING COMMENT '省份ID',
    `activity_id`           STRING COMMENT '参与活动ID',
    `activity_rule_id`      STRING COMMENT '参与活动规则ID',
    `coupon_id`             STRING COMMENT '使用优惠券ID',
    `payment_type_code`     STRING COMMENT '支付类型编码',
    `payment_type_name`     STRING COMMENT '支付类型名称',
    `date_id`               STRING COMMENT '支付日期ID',
    `callback_time`         STRING COMMENT '支付成功时间',
    `sku_num`               BIGINT COMMENT '商品数量',
    `split_original_amount` DECIMAL(16, 2) COMMENT '应支付原始金额',
    `split_activity_amount` DECIMAL(16, 2) COMMENT '支付活动优惠分摊',
    `split_coupon_amount`   DECIMAL(16, 2) COMMENT '支付优惠券优惠分摊',
    `split_payment_amount`  DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域支付成功事务事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/bigdata_warehouse/gmall/dwd/dwd_trade_pay_detail_suc_inc/';

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd.dwd_trade_pay_detail_suc_inc partition (dt)
select od.id,
       od.order_id,
       user_id,
       sku_id,
       province_id,
       3
                                                activity_id,
       activity_rule_id,
       coupon_id,
       payment_type,
       pay_dic.dic_name,
       date_format(callback_time, 'yyyy-MM-dd') date_id,
       callback_time,
       sku_num,
       split_original_amount,
       nvl(split_activity_amount, 0.0),
       nvl(split_coupon_amount, 0.0),
       split_total_amount,
       date_format(callback_time, 'yyyy-MM-dd')
from (select data.id,
             data.order_id,
             data.sku_id,
             data.sku_num,
             data.sku_num * data.order_price split_original_amount,
             data.split_total_amount,
             data.split_activity_amount,
             data.split_coupon_amount
      from ods_order_detail data
      where dt = '20250630') od
         join
     (select data.user_id,
             data.order_id,
             data.payment_type,
             data.callback_time
      from ods_payment_info data
      where dt = '20250630') pi
     on od.order_id = pi.order_id
         left join
     (select data.id,
             data.province_id
      from ods_order_info data
      where dt = '20250630') oi
     on od.order_id = oi.id
         left join
     (select data.order_detail_id,
             data.activity_id,
             data.activity_rule_id
      from ods_order_detail_activity data
      where dt = '20250630') act
     on od.id = act.order_detail_id
         left join
     (select data.order_detail_id,
             data.coupon_id
      from ods_order_detail_coupon data
      where dt = '20250630') cou
     on od.id = cou.order_detail_id
         left join
     (select dic_code,
             dic_name
      from ods_base_dic
      where dt = '20250630'
        and parent_code = '11') pay_dic
     on pi.payment_type = pay_dic.dic_code;


insert overwrite table dwd.dwd_trade_pay_detail_suc_inc partition (dt = '20250630')
select od.id,
       od.order_id,
       user_id,
       sku_id,
       province_id,
       activity_id,
       activity_rule_id,
       coupon_id,
       payment_type,
       pay_dic.dic_name,
       date_format(callback_time, 'yyyy-MM-dd') date_id,
       callback_time,
       sku_num,
       split_original_amount,
       nvl(split_activity_amount, 0.0),
       nvl(split_coupon_amount, 0.0),
       split_total_amount
from (select data.id,
             data.order_id,
             data.sku_id,
             data.sku_num,
             data.sku_num * data.order_price split_original_amount,
             data.split_total_amount,
             data.split_activity_amount,
             data.split_coupon_amount
      from ods_order_detail data
      where (dt = '20250630' or dt = date_add('20250630', -1))) od
         join
     (select data.user_id,
             data.order_id,
             data.payment_type,
             data.callback_time
      from ods_payment_info data
      where dt = '20250630') pi
     on od.order_id = pi.order_id
         left join
     (select data.id,
             data.province_id
      from ods_order_info data
      where (dt = '20250630' or dt = date_add('20250630', -1))) oi
     on od.order_id = oi.id
         left join
     (select data.order_detail_id,
             data.activity_id,
             data.activity_rule_id
      from ods_order_detail_activity data
      where (dt = '20250630' or dt = date_add('20250630', -1))) act
     on od.id = act.order_detail_id
         left join
     (select data.order_detail_id,
             data.coupon_id
      from ods_order_detail_coupon data
      where (dt = '20250630' or dt = date_add('20250630', -1))) cou
     on od.id = cou.order_detail_id
         left join
     (select dic_code,
             dic_name
      from ods_base_dic
      where dt = '20250630'
        and parent_code = '11') pay_dic
     on pi.payment_type = pay_dic.dic_code;


select *
from dwd.dwd_trade_pay_detail_suc_inc;


DROP TABLE IF EXISTS dwd.dwd_trade_cart_full;
CREATE EXTERNAL TABLE dwd.dwd_trade_cart_full
(
    `id`       STRING COMMENT '编号',
    `user_id`  STRING COMMENT '用户ID',
    `sku_id`   STRING COMMENT 'SKU_ID',
    `sku_name` STRING COMMENT '商品名称',
    `sku_num`  BIGINT COMMENT '现存商品件数'
) COMMENT '交易域购物车周期快照事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/bigdata_warehouse/gmall/dwd/dwd_trade_cart_full/';

insert overwrite table dwd.dwd_trade_cart_full partition (dt = '20250630')
select id,
       user_id,
       sku_id,
       sku_name,
       sku_num
from ods_cart_info
where dt = '20250630'
  and is_ordered = '0';

select *
from dwd.dwd_trade_cart_full;



DROP TABLE IF EXISTS dwd.dwd_trade_trade_flow_acc;
CREATE EXTERNAL TABLE dwd.dwd_trade_trade_flow_acc
(
    `order_id`              STRING COMMENT '订单ID',
    `user_id`               STRING COMMENT '用户ID',
    `province_id`           STRING COMMENT '省份ID',
    `order_date_id`         STRING COMMENT '下单日期ID',
    `order_time`            STRING COMMENT '下单时间',
    `payment_date_id`       STRING COMMENT '支付日期ID',
    `payment_time`          STRING COMMENT '支付时间',
    `finish_date_id`        STRING COMMENT '确认收货日期ID',
    `finish_time`           STRING COMMENT '确认收货时间',
    `order_original_amount` DECIMAL(16, 2) COMMENT '下单原始价格',
    `order_activity_amount` DECIMAL(16, 2) COMMENT '下单活动优惠分摊',
    `order_coupon_amount`   DECIMAL(16, 2) COMMENT '下单优惠券优惠分摊',
    `order_total_amount`    DECIMAL(16, 2) COMMENT '下单最终价格分摊',
    `payment_amount`        DECIMAL(16, 2) COMMENT '支付金额'
) COMMENT '交易域交易流程累积快照事实表'
    PARTITIONED BY (dt STRING)
    STORED AS parquet
    LOCATION '/bigdata_warehouse/gmall/dwd/dwd_trade_trade_flow_acc/';

-- 首次插入：处理当日数据
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd.dwd_trade_trade_flow_acc partition (dt)
select oi.id,
       user_id,
       province_id,
       date_format(create_time, 'yyyy-MM-dd'),
       create_time,
       date_format(callback_time, 'yyyy-MM-dd'),
       callback_time,
       date_format(finish_time, 'yyyy-MM-dd'),
       finish_time,
       original_total_amount,
       activity_reduce_amount,
       coupon_reduce_amount,
       total_amount,
       nvl(payment_amount, 0.0),
       nvl(date_format(finish_time, 'yyyy-MM-dd'), '9999-12-31')
from (select data.id,
             data.user_id,
             data.province_id,
             data.create_time,
             data.original_total_amount,
             data.activity_reduce_amount,
             data.coupon_reduce_amount,
             data.total_amount
      from ods_order_info data
      where dt = '20250630') oi
         left join
     (select data.order_id,
             data.callback_time,
             data.total_amount payment_amount
      from ods_payment_info data
      where dt = '20250630') pi
     on oi.id = pi.order_id
         left join
     (select data.order_id,
             data.operate_time finish_time
      from ods_order_status_log data
      where dt = '20250630'
        and data.order_status = '1004') log
     on oi.id = log.order_id;

-- 二次插入：合并历史数据与新增数据
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd.dwd_trade_trade_flow_acc partition (dt)
select oi.order_id,
       user_id,
       province_id,
       order_date_id,
       order_time,
       nvl(oi.payment_date_id, pi.payment_date_id),
       nvl(oi.payment_time, pi.payment_time),
       nvl(oi.finish_date_id, log.finish_date_id),
       nvl(oi.finish_time, log.finish_time),
       order_original_amount,
       order_activity_amount,
       order_coupon_amount,
       order_total_amount,
       nvl(oi.payment_amount, pi.payment_amount),
       nvl(nvl(oi.finish_time, log.finish_time), '9999-12-31')
from (
         -- 历史数据（保持STRING类型）
         select cast(order_id as string) order_id,
                user_id,
                province_id,
                order_date_id,
                order_time,
                payment_date_id,
                payment_time,
                finish_date_id,
                finish_time,
                order_original_amount,
                order_activity_amount,
                order_coupon_amount,
                order_total_amount,
                payment_amount
         from dwd.dwd_trade_trade_flow_acc
         where dt = '9999-12-31'

         union all

         -- 新增数据（将order_time转为STRING）
         select cast(data.id as string)                     order_id,
                cast(data.user_id as string)                user_id,
                cast(data.province_id as string)            province_id,
                date_format(data.create_time, 'yyyy-MM-dd') order_date_id,
                cast(data.create_time as string)            order_time, -- 关键修改：转STRING类型
                null                                        payment_date_id,
                null                                        payment_time,
                null                                        finish_date_id,
                null                                        finish_time,
                data.original_total_amount,
                data.activity_reduce_amount,
                data.coupon_reduce_amount,
                data.total_amount,
                null                                        payment_amount
         from ods_order_info data) oi
         left join
     (
         -- 支付表：将order_id转为STRING
         select cast(data.order_id as string)                 order_id,
                date_format(data.callback_time, 'yyyy-MM-dd') payment_date_id,
                data.callback_time                            payment_time,
                data.total_amount                             payment_amount
         from ods_payment_info data
         where dt = '20250627') pi
     on oi.order_id = pi.order_id
         left join
     (
         -- 订单状态表：将order_id转为STRING
         select cast(data.order_id as string)                order_id,
                date_format(data.operate_time, 'yyyy-MM-dd') finish_date_id,
                data.operate_time                            finish_time
         from ods_order_status_log data
         where dt = '20250627'
           and data.order_status = '1004') log
     on oi.order_id = log.order_id;


select *
from dwd.dwd_trade_trade_flow_acc;
--alter table ods_order_info change column province_id province_id string;


DROP TABLE IF EXISTS dwd.dwd_tool_coupon_used_inc;
CREATE EXTERNAL TABLE dwd.dwd_tool_coupon_used_inc
(
    `id`           STRING COMMENT '编号',
    `coupon_id`    STRING COMMENT '优惠券ID',
    `user_id`      STRING COMMENT '用户ID',
    `order_id`     STRING COMMENT '订单ID',
    `date_id`      STRING COMMENT '日期ID',
    `payment_time` STRING COMMENT '使用(支付)时间'
) COMMENT '优惠券使用（支付）事务事实表'
    PARTITIONED BY (dt STRING)
    LOCATION '/bigdata_warehouse/gmall/dwd/dwd_tool_coupon_used_inc/';

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd.dwd_tool_coupon_used_inc partition (dt)
select data.id,
       data.coupon_id,
       data.user_id,
       data.order_id,
       date_format(data.used_time, 'yyyy-MM-dd') date_id,
       data.used_time,
       date_format(data.used_time, 'yyyy-MM-dd')
from ods_coupon_use data
where dt = '20250630'
  and data.used_time is not null;

insert overwrite table dwd.dwd_tool_coupon_used_inc partition (dt = '2025-06-28')
select data.id,
       data.coupon_id,
       data.user_id,
       data.order_id,
       date_format(data.used_time, 'yyyy-MM-dd') date_id,
       '2025-06-26'
from ods_coupon_use data
limit 50;


insert into table dwd.dwd_tool_coupon_used_inc partition (dt = '2025-06-28')
select data.id,
       data.coupon_id,
       data.user_id,
       data.order_id,
       date_format(data.used_time, 'yyyy-MM-dd') date_id,
       '2025-06-24' as                           ds
from ods_coupon_use data
limit 20;

select *
from dwd.dwd_tool_coupon_used_inc;



DROP TABLE IF EXISTS dwd.dwd_interaction_favor_add_inc;
CREATE EXTERNAL TABLE dwd.dwd_interaction_favor_add_inc
(
    `id`          STRING COMMENT '编号',
    `user_id`     STRING COMMENT '用户ID',
    `sku_id`      STRING COMMENT 'SKU_ID',
    `date_id`     STRING COMMENT '日期ID',
    `create_time` STRING COMMENT '收藏时间'
) COMMENT '互动域收藏商品事务事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/bigdata_warehouse/gmall/dwd/dwd_interaction_favor_add_inc/';

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd.dwd_interaction_favor_add_inc partition (dt)
select data.id,
       data.user_id,
       data.sku_id,
       date_format(data.create_time, 'yyyy-MM-dd') date_id,
       data.create_time,
       date_format(data.create_time, 'yyyy-MM-dd')
from ods_favor_info data
where dt = '20250630';


insert overwrite table dwd.dwd_interaction_favor_add_inc partition (dt = '2025-06-28')
select data.id,
       data.user_id,
       data.sku_id,
       date_format(data.create_time, 'yyyy-MM-dd') date_id,
       data.create_time
from ods_favor_info data
where dt = '2025-06-28';
select *
from dwd.dwd_interaction_favor_add_inc;


DROP TABLE IF EXISTS dwd.dwd_traffic_page_view_inc;
CREATE EXTERNAL TABLE dwd.dwd_traffic_page_view_inc
(
    `province_id`    STRING COMMENT '省份ID',
    `brand`          STRING COMMENT '手机品牌',
    `channel`        STRING COMMENT '渠道',
    `is_new`         STRING COMMENT '是否首次启动',
    `model`          STRING COMMENT '手机型号',
    `mid_id`         STRING COMMENT '设备ID',
    `operate_system` STRING COMMENT '操作系统',
    `user_id`        STRING COMMENT '会员ID',
    `version_code`   STRING COMMENT 'APP版本号',
    `page_item`      STRING COMMENT '目标ID',
    `page_item_type` STRING COMMENT '目标类型',
    `last_page_id`   STRING COMMENT '上页ID',
    `page_id`        STRING COMMENT '页面ID ',
    `from_pos_id`    STRING COMMENT '点击坑位ID',
    `from_pos_seq`   STRING COMMENT '点击坑位位置',
    `refer_id`       STRING COMMENT '营销渠道ID',
    `date_id`        STRING COMMENT '日期ID',
    `view_time`      STRING COMMENT '跳入时间',
    `session_id`     STRING COMMENT '所属会话ID',
    `during_time`    BIGINT COMMENT '持续时间毫秒'
) COMMENT '流量域页面浏览事务事实表'
    PARTITIONED BY (dt STRING)
    STORED AS parquet
    LOCATION '/bigdata_warehouse/gmall/dwd/dwd_traffic_page_view_inc';

set hive.cbo.enable=false;
insert overwrite table dwd.dwd_traffic_page_view_inc partition (dt = '20250630')
select common.province_id,
       common.brand,
       common.channel,
       common.is_new,
       common.model,
       common.mid_id,
       common.operate_system,
       common.user_id,
       common.version_code,
       page_data.item      as                                              page_item,
       page_data.item_type as                                              page_item_type,
       page_data.last_page_id,
       page_data.page_id,
       page_data.from_pos_id,
       page_data.from_pos_seq,
       page_data.refer_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          date_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') view_time,
       common.session_id,
       page_data.during_time
from (select get_json_object(log, '$.common') as common_json,
             get_json_object(log, '$.page')   as page_json,
             get_json_object(log, '$.ts')     as ts
      from ods_z_log
      where dt = '20250630') base
         lateral view json_tuple(common_json, 'ar', 'ba', 'ch', 'is_new', 'md', 'mid', 'os', 'uid', 'vc',
                                 'sid') common as province_id, brand, channel, is_new, model, mid_id, operate_system,
                                                  user_id, version_code, session_id
         lateral view json_tuple(page_json, 'item', 'item_type', 'last_page_id', 'page_id', 'from_pos_id',
                                 'from_pos_seq', 'refer_id', 'during_time') page_data as item, item_type, last_page_id,
                                                                                         page_id, from_pos_id,
                                                                                         from_pos_seq, refer_id,
                                                                                         during_time
where page_json is not null;

select *
from dwd.dwd_traffic_page_view_inc;



DROP TABLE IF EXISTS dwd.dwd_user_register_inc;
CREATE EXTERNAL TABLE dwd.dwd_user_register_inc
(
    `user_id`        STRING COMMENT '用户ID',
    `date_id`        STRING COMMENT '日期ID',
    `create_time`    STRING COMMENT '注册时间',
    `channel`        STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份ID',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`         STRING COMMENT '设备ID',
    `brand`          STRING COMMENT '设备品牌',
    `model`          STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
) COMMENT '用户域用户注册事务事实表'
    PARTITIONED BY (dt STRING)
    STORED AS ORC
    LOCATION '/bigdata_warehouse/gmall/dwd/dwd_user_register_inc/';

set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table dwd.dwd_user_register_inc partition (dt)
select ui.user_id,
       date_format(ui.create_time, 'yyyy-MM-dd')    date_id,
       ui.create_time,
       log.channel,
       log.province_id,
       log.version_code,
       log.mid_id,
       log.brand,
       log.model,
       log.operate_system,
       date_format(ui.create_time, 'yyyy-MM-dd') as ds
from (select data.id as user_id,
             data.create_time
      from ods_user_info data
      where dt = '20220628') ui
         left join
     (select get_json_object(log, '$.common.ar')  as province_id, -- 修改此处，使用正确的列名log
             get_json_object(log, '$.common.ba')  as brand,
             get_json_object(log, '$.common.ch')  as channel,
             get_json_object(log, '$.common.md')  as model,
             get_json_object(log, '$.common.mid') as mid_id,
             get_json_object(log, '$.common.os')  as operate_system,
             get_json_object(log, '$.common.uid') as user_id,
             get_json_object(log, '$.common.vc')  as version_code
      from ods_z_log
      where dt = '20220628'
        and get_json_object(log, '$.page.page_id') = 'register' -- 修改此处，使用正确的列名log
        and get_json_object(log, '$.common.uid') is not null) log
     on ui.user_id = log.user_id;

insert overwrite table dwd.dwd_user_register_inc partition (dt = '20250629')
select ui.user_id,
       date_format(ui.create_time, 'yyyy-MM-dd') date_id,
       ui.create_time,
       log.channel,
       log.province_id,
       log.version_code,
       log.mid_id,
       log.brand,
       log.model,
       log.operate_system
from (select data.id as user_id,
             data.create_time
      from ods_user_info data
      where dt = '20250630') ui
         left join
     (select get_json_object(log, '$.common.ar')  as province_id, -- 修改此处，使用正确的列名log
             get_json_object(log, '$.common.ba')  as brand,
             get_json_object(log, '$.common.ch')  as channel,
             get_json_object(log, '$.common.md')  as model,
             get_json_object(log, '$.common.mid') as mid_id,
             get_json_object(log, '$.common.os')  as operate_system,
             get_json_object(log, '$.common.uid') as user_id,
             get_json_object(log, '$.common.vc')  as version_code
      from ods_z_log
      where dt = '20250630'
        and get_json_object(log, '$.page.page_id') = 'register' -- 修改此处，使用正确的列名log
        and get_json_object(log, '$.common.uid') is not null) log
     on ui.user_id = log.user_id;

select *
from dwd.dwd_user_register_inc;



DROP TABLE IF EXISTS dwd.dwd_user_login_inc;

-- 重建用户登录事实表（分区字段为 ds）
CREATE EXTERNAL TABLE dwd.dwd_user_login_inc
(
    `user_id`        STRING COMMENT '用户ID',
    `date_id`        STRING COMMENT '日期ID',
    `login_time`     STRING COMMENT '登录时间',
    `channel`        STRING COMMENT '应用下载渠道',
    `province_id`    STRING COMMENT '省份ID',
    `version_code`   STRING COMMENT '应用版本',
    `mid_id`         STRING COMMENT '设备ID',
    `brand`          STRING COMMENT '设备品牌',
    `model`          STRING COMMENT '设备型号',
    `operate_system` STRING COMMENT '设备操作系统'
)
    COMMENT '用户域用户登录事务事实表'
    PARTITIONED BY (dt STRING) -- 分区字段 ds
    STORED AS parquet-- 显式指定存储格式
    LOCATION '/bigdata_warehouse/gmall/dwd/dwd_user_login_inc/';

-- 插入数据（修正 log 列为 log，优化子查询）
insert overwrite table dwd.dwd_user_login_inc partition (dt = '20250630')
select user_id,
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd')          as date_id,    -- 日期ID
       date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as login_time, -- 登录时间
       channel,
       province_id,
       version_code,
       mid_id,
       brand,
       model,
       operate_system
from (select common_uid                                              as user_id,
             common_ch                                               as channel,
             common_ar                                               as province_id,
             common_vc                                               as version_code,
             common_mid                                              as mid_id,
             common_ba                                               as brand,
             common_md                                               as model,
             common_os                                               as operate_system,
             ts,
             row_number() over (partition by common_sid order by ts) as rn -- 按 session 去重
      from (select
                -- 解析 JSON 字段（假设日志存在 log 列）
                get_json_object(log, '$.common.uid') as common_uid,
                get_json_object(log, '$.common.ch')  as common_ch,
                get_json_object(log, '$.common.ar')  as common_ar,
                get_json_object(log, '$.common.vc')  as common_vc,
                get_json_object(log, '$.common.mid') as common_mid,
                get_json_object(log, '$.common.ba')  as common_ba,
                get_json_object(log, '$.common.md')  as common_md,
                get_json_object(log, '$.common.os')  as common_os,
                get_json_object(log, '$.ts')         as ts,         -- 解析时间戳
                get_json_object(log, '$.common.sid') as common_sid, -- 解析 session_id
                get_json_object(log, '$.page')       as page        -- 解析 page 字段（过滤用）
            from ods_z_log
            where dt = '20250630'
              -- 过滤条件：page 非空 + 用户 ID 非空
              and get_json_object(log, '$.page') is not null
              and get_json_object(log, '$.common.uid') is not null) t1) t2
where rn = 1; -- 取每个 session 的首次登录记录

select *
from dwd.dwd_user_login_inc;