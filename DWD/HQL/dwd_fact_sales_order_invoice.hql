-- Hive SQL
-- Function： 销售订单的发票事实表
-- History: 
--  2021-05-27    Donny   v1.0    init

drop table if exists dwd_fact_sales_order_invoice;
create external table dwd_fact_sales_order_invoice
(
    bill_id           string,
    accounting_no     string,
    bill_date         string,
    bill_type         string,
    sales_id          string,
    delivery_no       string,
    material          string,
    sales_line_no     bigint,
    batch             string,
    bill_qty          bigint,
    net_amount        decimal(18,2),
    currency          string,
    ship_to           string,
    sold_to           string,
    tax_amount        decimal(18,2),
    tax_rate          decimal(18,2),
    purchase_order    string
) comment 'Sales Order Invoice'
partitioned by(dt string) 
stored as parquet
location '/bsc/opsdw/dwd/dwd_fact_sales_order_invoice/'
tblproperties ("parquet.compression"="lzo");