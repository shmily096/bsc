-- Hive SQL
-- Function： CFDA数据 （DWD 层）
-- History: 
-- 2021-11-16    Amanda   v1.0    draft

drop table if exists dwd_duty_by_upn;
create external table dwd_duty_by_upn
(
        CustomsDeclarationNo        string
        ,SeriaNumber                 string
        ,TaxNumber                   string
        ,ContractNo                  string
        ,TaxBillCreateDT             string
        ,RecordArea                  string
        ,Amount                      decimal(9,2)
        ,TaxPaymentDate              string
        ,MailDeliveryDate            string
        ,UPN                         string
        ,Quantity                    decimal(9,2)
        ,BU                          string
        ,YEARMONTH                   string
) comment 'DutybyUPN'
partitioned by(dt string)
stored as parquet 
location '/bsc/opsdw/dwd/dwd_duty_by_upn/'
tblproperties ("parquet.compression"="lzo");