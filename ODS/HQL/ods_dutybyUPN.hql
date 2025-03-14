-- Hive SQL
-- Function： CFDA数据 （ODS 层）
-- History: 
-- 2021-11-15    Amanda   v1.0    draft

drop table if exists ods_DutybyUPN;
create external table ods_DutybyUPN
(
        UpdateDT                    string
        ,Active                      string
        ,CustomsDeclarationNo        string
        ,SeriaNumber                 string
        ,TaxNumber                   string
        ,TaxType                     string
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
row format delimited fields terminated by '\t' 
location '/bsc/opsdw/ods/ods_DutybyUPN/'
;