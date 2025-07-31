
use gmall;

CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_activity_info (
    `id` BIGINT COMMENT '活动id',
    `activity_name` STRING COMMENT '活动名称',
    `activity_type` STRING COMMENT '活动类型（1：满减，2：折扣）',
    `activity_desc` STRING COMMENT '活动描述',
    `start_time` STRING COMMENT '开始时间',
    `end_time` STRING COMMENT '结束时间',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '活动表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_activity_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_activity_rule (
    `id` INT COMMENT '编号',
    `activity_id` INT COMMENT '活动id',
    `activity_type` STRING COMMENT '活动类型',
    `condition_amount` DECIMAL(16,2) COMMENT '满减金额',
    `condition_num` BIGINT COMMENT '满减件数',
    `benefit_amount` DECIMAL(16,2) COMMENT '优惠金额',
    `benefit_discount` DECIMAL(10,2) COMMENT '优惠折扣',
    `benefit_level` BIGINT COMMENT '优惠级别',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '活动规则表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_activity_rule/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_activity_sku (
    `id` BIGINT COMMENT '编号',
    `activity_id` BIGINT COMMENT '活动id ',
    `sku_id` BIGINT COMMENT 'sku_id',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '活动商品关联表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_activity_sku/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_attr_info (
    `id` BIGINT COMMENT '编号',
    `attr_name` STRING COMMENT '属性名称',
    `category_id` BIGINT COMMENT '分类id',
    `category_level` INT COMMENT '分类层级',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '属性表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_attr_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_attr_value (
    `id` BIGINT COMMENT '编号',
    `value_name` STRING COMMENT '属性值名称',
    `attr_id` BIGINT COMMENT '属性id',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '属性值表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_attr_value/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_category1 (
    `id` BIGINT COMMENT '一级品类id',
    `name` STRING COMMENT '一级品类名称',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '一级品类表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_category1/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_category2 (
    `id` BIGINT COMMENT '二级品类id',
    `name` STRING COMMENT '二级品类名称',
    `category1_id` BIGINT COMMENT '一级品类编号',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '二级品类表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_category2/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_category3 (
    `id` BIGINT COMMENT '三级品类id',
    `name` STRING COMMENT '三级品类名称',
    `category2_id` BIGINT COMMENT '二级品类编号',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '三级品类表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_category3/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_dic (
    `dic_code` STRING COMMENT '编号',
    `dic_name` STRING COMMENT '编码名称',
    `parent_code` STRING COMMENT '父编号',
    `create_time` STRING COMMENT '创建日期',
    `operate_time` STRING COMMENT '修改日期'
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_dic/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_frontend_param (
    `id` BIGINT COMMENT '编号',
    `code` STRING COMMENT '属性名称',
    `delete_id` BIGINT COMMENT '分类id',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '前端数据保护表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_frontend_param/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_province (
    `id` BIGINT COMMENT 'id',
    `name` STRING COMMENT '省份名称',
    `region_id` STRING COMMENT '地区id',
    `area_code` STRING COMMENT '地区编码',
    `iso_code` STRING COMMENT '旧版国际标准地区编码，供可视化使用',
    `iso_3166_2` STRING COMMENT '新版国际标准地区编码，供可视化使用',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '省份表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_province/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_region (
    `id` STRING COMMENT '地区id',
    `region_name` STRING COMMENT '地区名称',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING
) COMMENT '地区表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_region/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_sale_attr (
    `id` BIGINT COMMENT '编号',
    `name` STRING COMMENT '销售属性名称',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '基本销售属性表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_sale_attr/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_base_trademark (
    `id` BIGINT COMMENT '编号',
    `tm_name` STRING COMMENT '品牌名称',
    `logo_url` STRING COMMENT '品牌logo的图片路径',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '品牌表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_base_trademark/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_cart_info (
    `id` BIGINT COMMENT '编号',
    `user_id` STRING COMMENT '用户id',
    `sku_id` BIGINT COMMENT 'skuid',
    `cart_price` DECIMAL(10,2) COMMENT '放入购物车时价格',
    `sku_num` INT COMMENT '数量',
    `img_url` STRING COMMENT '商品图片地址',
    `sku_name` STRING COMMENT 'sku名称 (冗余)',
    `is_checked` INT COMMENT '是否被选中',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间',
    `is_ordered` BIGINT COMMENT '是否已经下单',
    `order_time` STRING COMMENT '下单时间'
) COMMENT '购物车表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_cart_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_cms_banner (
    `id` BIGINT COMMENT 'ID',
    `title` STRING COMMENT '标题',
    `image_url` STRING COMMENT '图片地址',
    `link_url` STRING COMMENT '链接地址',
    `sort` INT COMMENT '排序',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '首页banner表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_cms_banner/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_comment_info (
    `id` BIGINT COMMENT '编号',
    `user_id` BIGINT COMMENT '用户id',
    `nick_name` STRING COMMENT '用户昵称',
    `head_img` STRING,
    `sku_id` BIGINT COMMENT 'skuid',
    `spu_id` BIGINT COMMENT 'spuid',
    `order_id` BIGINT COMMENT '订单编号',
    `appraise` STRING COMMENT '评价 1 好评 2 中评 3 差评',
    `comment_txt` STRING COMMENT '评价内容',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '商品评论表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_comment_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_coupon_info (
    `id` BIGINT COMMENT '购物券编号',
    `coupon_name` STRING COMMENT '购物券名称',
    `coupon_type` STRING COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` DECIMAL(10,2) COMMENT '满额数',
    `condition_num` BIGINT COMMENT '满件数',
    `activity_id` BIGINT COMMENT '活动编号',
    `benefit_amount` DECIMAL(16,2) COMMENT '减免金额',
    `benefit_discount` DECIMAL(10,2) COMMENT '折扣',
    `create_time` STRING COMMENT '创建时间',
    `range_type` STRING COMMENT '范围类型 1、商品(spuid) 2、品类(三级分类id) 3、品牌',
    `limit_num` INT COMMENT '最多领用次数',
    `taken_count` INT COMMENT '已领用次数',
    `start_time` STRING COMMENT '可以领取的开始时间',
    `end_time` STRING COMMENT '可以领取的结束时间',
    `operate_time` STRING COMMENT '修改时间',
    `expire_time` STRING COMMENT '过期时间',
    `range_desc` STRING COMMENT '范围描述'
) COMMENT '优惠券信息表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_coupon_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_coupon_range (
    `id` BIGINT COMMENT '购物券编号',
    `coupon_id` BIGINT COMMENT '优惠券id',
    `range_type` STRING COMMENT '范围类型 1、商品(spuid) 2、品类(三级分类id) 3、品牌',
    `range_id` BIGINT,
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '优惠券范围表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_coupon_range/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_coupon_use (
    `id` BIGINT COMMENT '编号',
    `coupon_id` BIGINT COMMENT '购物券ID',
    `user_id` BIGINT COMMENT '用户ID',
    `order_id` BIGINT COMMENT '订单ID',
    `coupon_status` STRING COMMENT '购物券状态（1：未使用 2：已使用）',
    `get_time` STRING COMMENT '获取时间',
    `using_time` STRING COMMENT '使用时间',
    `used_time` STRING COMMENT '支付时间',
    `expire_time` STRING COMMENT '过期时间',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '优惠券领用表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_coupon_use/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_favor_info (
    `id` BIGINT COMMENT '编号',
    `user_id` BIGINT COMMENT '用户id',
    `sku_id` BIGINT COMMENT 'skuid',
    `spu_id` BIGINT COMMENT 'spuid',
    `is_cancel` STRING COMMENT '是否已取消 0 正常 1 已取消',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '商品收藏表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_favor_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_financial_sku_cost (
    `id` STRING,
    `sku_id` BIGINT COMMENT 'sku_id',
    `sku_name` STRING COMMENT 'sku名称',
    `busi_date` STRING COMMENT '业务日期',
    `is_lastest` STRING COMMENT '是否最近',
    `sku_cost` DECIMAL(16,2) COMMENT '商品结算成本',
    `create_time` STRING COMMENT '创建时间'
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_financial_sku_cost/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_order_detail (
    `id` BIGINT COMMENT '编号',
    `order_id` BIGINT COMMENT '订单id',
    `sku_id` BIGINT COMMENT 'sku_id',
    `sku_name` STRING COMMENT 'sku名称（冗余)',
    `img_url` STRING COMMENT '图片链接（冗余)',
    `order_price` DECIMAL(10,2) COMMENT '购买价格(下单时sku价格）',
    `sku_num` BIGINT COMMENT '购买个数',
    `create_time` STRING COMMENT '创建时间',
    `split_total_amount` DECIMAL(16,2),
    `split_activity_amount` DECIMAL(16,2),
    `split_coupon_amount` DECIMAL(16,2),
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '订单明细表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_order_detail/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_order_detail_activity (
    `id` BIGINT COMMENT '编号',
    `order_id` BIGINT COMMENT '订单id',
    `order_detail_id` BIGINT COMMENT '订单明细id',
    `activity_id` BIGINT COMMENT '活动id',
    `activity_rule_id` BIGINT COMMENT '活动规则id',
    `sku_id` BIGINT COMMENT 'skuid',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '订单明细活动关联表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_order_detail_activity/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_order_detail_coupon (
    `id` BIGINT COMMENT '编号',
    `order_id` BIGINT COMMENT '订单id',
    `order_detail_id` BIGINT COMMENT '订单明细id',
    `coupon_id` BIGINT COMMENT '购物券id',
    `coupon_use_id` BIGINT COMMENT '购物券领用id',
    `sku_id` BIGINT COMMENT 'skuid',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '订单明细优惠券关联表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_order_detail_coupon/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_order_info (
    `id` BIGINT COMMENT '编号',
    `consignee` STRING COMMENT '收货人',
    `consignee_tel` STRING COMMENT '收件人电话',
    `total_amount` DECIMAL(10,2) COMMENT '总金额',
    `order_status` STRING COMMENT '订单状态',
    `user_id` BIGINT COMMENT '用户id',
    `payment_way` STRING COMMENT '付款方式',
    `delivery_address` STRING COMMENT '送货地址',
    `order_comment` STRING COMMENT '订单备注',
    `out_trade_no` STRING COMMENT '订单交易编号（第三方支付用)',
    `trade_body` STRING COMMENT '订单描述(第三方支付用)',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '操作时间',
    `expire_time` STRING COMMENT '失效时间',
    `process_status` STRING COMMENT '进度状态',
    `tracking_no` STRING COMMENT '物流单编号',
    `parent_order_id` BIGINT COMMENT '父订单编号',
    `img_url` STRING COMMENT '图片链接',
    `province_id` INT COMMENT '省份id',
    `activity_reduce_amount` DECIMAL(16,2) COMMENT '活动减免金额',
    `coupon_reduce_amount` DECIMAL(16,2) COMMENT '优惠券减免金额',
    `original_total_amount` DECIMAL(16,2) COMMENT '原始总金额',
    `feight_fee` DECIMAL(16,2) COMMENT '运费金额',
    `feight_fee_reduce` DECIMAL(16,2) COMMENT '运费减免金额',
    `refundable_time` STRING COMMENT '可退款时间（签收后30天）'
) COMMENT '订单表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_order_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_order_refund_info (
    `id` BIGINT COMMENT '编号',
    `user_id` BIGINT COMMENT '用户id',
    `order_id` BIGINT COMMENT '订单id',
    `sku_id` BIGINT COMMENT 'skuid',
    `refund_type` STRING COMMENT '退款类型',
    `refund_num` BIGINT COMMENT '退货件数',
    `refund_amount` DECIMAL(16,2) COMMENT '退款金额',
    `refund_reason_type` STRING COMMENT '原因类型',
    `refund_reason_txt` STRING COMMENT '原因内容',
    `refund_status` STRING COMMENT '退款状态（0：待审批 1：已退款）',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '退单表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_order_refund_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_order_status_log (
    `id` BIGINT COMMENT '编号',
    `order_id` BIGINT COMMENT '订单id',
    `order_status` STRING COMMENT '订单状态',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '订单状态流水表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_order_status_log/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_payment_info (
    `id` INT COMMENT '编号',
    `out_trade_no` STRING COMMENT '对外业务编号',
    `order_id` BIGINT COMMENT '订单id',
    `user_id` BIGINT COMMENT '用户id',
    `payment_type` STRING COMMENT '支付类型（微信 支付宝）',
    `trade_no` STRING COMMENT '交易编号',
    `total_amount` DECIMAL(10,2) COMMENT '支付金额',
    `subject` STRING COMMENT '交易内容',
    `payment_status` STRING COMMENT '支付状态',
    `create_time` STRING COMMENT '创建时间',
    `callback_time` STRING COMMENT '回调时间',
    `callback_content` STRING COMMENT '回调信息',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '支付信息表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_payment_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_promotion_pos (
    `id` BIGINT COMMENT '营销坑位id',
    `pos_location` STRING COMMENT '营销坑位位置',
    `pos_type` STRING COMMENT '营销坑位类型：banner,宫格,列表, 瀑布',
    `promotion_type` STRING COMMENT '营销类型：算法、固定、搜索',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '营销坑位表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_promotion_pos/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_promotion_refer (
    `id` BIGINT COMMENT '外部营销渠道id',
    `refer_name` STRING COMMENT '外部营销渠道名称',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '营销渠道表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_promotion_refer/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_refund_payment (
    `id` INT COMMENT '编号',
    `out_trade_no` STRING COMMENT '对外业务编号',
    `order_id` BIGINT COMMENT '订单id',
    `sku_id` BIGINT COMMENT 'skuid',
    `payment_type` STRING COMMENT '支付类型（微信 支付宝）',
    `trade_no` STRING COMMENT '交易编号',
    `total_amount` DECIMAL(10,2) COMMENT '退款金额',
    `subject` STRING COMMENT '交易内容',
    `refund_status` STRING COMMENT '退款状态',
    `create_time` STRING COMMENT '创建时间',
    `callback_time` STRING COMMENT '回调时间',
    `callback_content` STRING COMMENT '回调信息',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '退款支付表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_refund_payment/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_seckill_goods (
    `id` BIGINT,
    `spu_id` BIGINT COMMENT 'spu_id',
    `sku_id` BIGINT COMMENT 'sku_id',
    `sku_name` STRING COMMENT '标题',
    `sku_default_img` STRING COMMENT '商品图片',
    `price` DECIMAL(10,2) COMMENT '原价格',
    `cost_price` DECIMAL(10,2) COMMENT '秒杀价格',
    `create_time` STRING COMMENT '添加日期',
    `check_time` STRING COMMENT '审核日期',
    `status` STRING COMMENT '审核状态',
    `start_time` STRING COMMENT '开始时间',
    `end_time` STRING COMMENT '结束时间',
    `num` INT COMMENT '秒杀商品数',
    `stock_count` INT COMMENT '剩余库存数',
    `sku_desc` STRING COMMENT '描述'
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_seckill_goods/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_sku_attr_value (
    `id` BIGINT COMMENT '编号',
    `attr_id` BIGINT COMMENT '平台属性id（冗余)',
    `value_id` BIGINT COMMENT '平台属性值id',
    `sku_id` BIGINT COMMENT 'skuid',
    `attr_name` STRING COMMENT '平台属性名称',
    `value_name` STRING COMMENT '平台属性值名称',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT 'sku平台属性值关联表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_sku_attr_value/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_sku_image (
    `id` BIGINT COMMENT '编号',
    `sku_id` BIGINT COMMENT 'skuid',
    `img_name` STRING COMMENT '图片名称（冗余）',
    `img_url` STRING COMMENT '图片路径(冗余)',
    `spu_img_id` BIGINT COMMENT 'spu图片id',
    `is_default` STRING COMMENT '是否默认',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '库存单元图片表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_sku_image/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_sku_info (
    `id` BIGINT COMMENT 'skuid',
    `spu_id` BIGINT COMMENT 'spuid',
    `price` DECIMAL(10,0) COMMENT '价格',
    `sku_name` STRING COMMENT 'sku名称',
    `sku_desc` STRING COMMENT '商品规格描述',
    `weight` DECIMAL(10,2) COMMENT '重量',
    `tm_id` BIGINT COMMENT '品牌id(冗余)',
    `category3_id` BIGINT COMMENT '三级品类id（冗余)',
    `sku_default_img` STRING COMMENT '默认显示图片地址(冗余)',
    `is_sale` INT COMMENT '是否在售（1：是 0：否）',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT 'sku表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_sku_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_sku_sale_attr_value (
    `id` BIGINT COMMENT '编号',
    `sku_id` BIGINT COMMENT 'sku_id',
    `spu_id` INT COMMENT 'spu_id(冗余)',
    `sale_attr_value_id` BIGINT COMMENT '销售属性值id',
    `sale_attr_id` BIGINT COMMENT '销售属性id',
    `sale_attr_name` STRING COMMENT '销售属性名称',
    `sale_attr_value_name` STRING COMMENT '销售属性值名称',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT 'sku销售属性值表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_sku_sale_attr_value/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_spu_image (
    `id` BIGINT COMMENT '编号',
    `spu_id` BIGINT COMMENT '商品id',
    `img_name` STRING COMMENT '图片名称',
    `img_url` STRING COMMENT '图片路径',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT 'spu图片表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_spu_image/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_spu_info (
    `id` BIGINT COMMENT 'spu_id',
    `spu_name` STRING COMMENT 'spu名称',
    `description` STRING COMMENT '描述信息',
    `category3_id` BIGINT COMMENT '三级品类id',
    `tm_id` BIGINT COMMENT '品牌id',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT 'spu表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_spu_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_spu_poster (
    `id` BIGINT COMMENT '编号',
    `spu_id` BIGINT COMMENT '商品id',
    `img_name` STRING COMMENT '文件名称',
    `img_url` STRING COMMENT '文件路径',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '更新时间',
    `is_deleted` INT COMMENT '逻辑删除 1（true）已删除， 0（false）未删除'
) COMMENT '商品海报表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_spu_poster/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_spu_sale_attr (
    `id` BIGINT COMMENT '编号(业务中无关联)',
    `spu_id` BIGINT COMMENT '商品id',
    `base_sale_attr_id` BIGINT COMMENT '销售属性id',
    `sale_attr_name` STRING COMMENT '销售属性名称(冗余)',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT 'spu销售属性表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_spu_sale_attr/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_spu_sale_attr_value (
    `id` BIGINT COMMENT '销售属性值编号',
    `spu_id` BIGINT COMMENT 'spuid',
    `base_sale_attr_id` BIGINT COMMENT '销售属性id',
    `sale_attr_value_name` STRING COMMENT '销售属性值名称',
    `sale_attr_name` STRING COMMENT '销售属性名称(冗余)',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT 'spu销售属性值表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_spu_sale_attr_value/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_user_address (
    `id` BIGINT COMMENT '编号',
    `user_id` BIGINT COMMENT '用户id',
    `province_id` BIGINT COMMENT '省份id',
    `user_address` STRING COMMENT '用户地址',
    `consignee` STRING COMMENT '收件人',
    `phone_num` STRING COMMENT '联系方式',
    `is_default` STRING COMMENT '是否是默认',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间'
) COMMENT '用户地址表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_user_address/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_user_info (
    `id` BIGINT COMMENT '编号',
    `login_name` STRING COMMENT '用户名称',
    `nick_name` STRING COMMENT '用户昵称',
    `passwd` STRING COMMENT '用户密码',
    `name` STRING COMMENT '用户姓名',
    `phone_num` STRING COMMENT '手机号',
    `email` STRING COMMENT '邮箱',
    `head_img` STRING COMMENT '头像',
    `user_level` STRING COMMENT '用户级别',
    `birthday` STRING COMMENT '用户生日',
    `gender` STRING COMMENT '性别 M男,F女',
    `create_time` STRING COMMENT '创建时间',
    `operate_time` STRING COMMENT '修改时间',
    `status` STRING COMMENT '状态'
) COMMENT '用户表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_user_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_ware_info (
    `id` BIGINT,
    `name` STRING,
    `address` STRING,
    `areacode` STRING
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_ware_info/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_ware_order_task (
    `id` BIGINT COMMENT '编号',
    `order_id` BIGINT COMMENT '订单编号',
    `consignee` STRING COMMENT '收货人',
    `consignee_tel` STRING COMMENT '收货人电话',
    `delivery_address` STRING COMMENT '送货地址',
    `order_comment` STRING COMMENT '订单备注',
    `payment_way` STRING COMMENT '付款方式 1:在线付款 2:货到付款',
    `task_status` STRING COMMENT '工作单状态',
    `order_body` STRING COMMENT '订单描述',
    `tracking_no` STRING COMMENT '物流单号',
    `create_time` STRING COMMENT '创建时间',
    `ware_id` BIGINT COMMENT '仓库编号',
    `task_comment` STRING COMMENT '工作单备注'
) COMMENT '库存工作单表 库存工作单表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_ware_order_task/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_ware_order_task_detail (
    `id` BIGINT COMMENT '编号',
    `sku_id` BIGINT COMMENT 'sku_id',
    `sku_name` STRING COMMENT 'sku名称',
    `sku_num` INT COMMENT '购买个数',
    `task_id` BIGINT COMMENT '工作单编号',
    `refund_status` STRING
) COMMENT '库存工作单明细表 库存工作单明细表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_ware_order_task_detail/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_ware_sku (
    `id` BIGINT COMMENT '编号',
    `sku_id` BIGINT COMMENT 'skuid',
    `warehouse_id` BIGINT COMMENT '仓库id',
    `stock` INT COMMENT '库存数',
    `stock_name` STRING COMMENT '存货名称',
    `stock_locked` INT COMMENT '锁定库存数'
) COMMENT 'sku与仓库关联表'
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_ware_sku/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');



CREATE EXTERNAL TABLE IF NOT EXISTS gmall.ods_z_log (
    `id` BIGINT,
    `log` STRING
) COMMENT ''
PARTITIONED BY (`dt` STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/warehouse/gmall/ods/ods_z_log/'
TBLPROPERTIES ('compression.codec'='org.apache.hadoop.io.compress.GzipCodec');
