-- Hive SQL
-- Function： CFDA数据 （DWD 层）
-- History: 
-- 2021-11-16    Amanda   v1.0    draft

drop table if exists dwd_ctm_CPC;
create external table dwd_ctm_CPC
(
    Number                                 string
    ,Contract_No                           string
    ,Application_No                        string
    ,Category                              string
    ,Category_Description                  string
    ,Type                                  string
    ,Type_Description                      string
    ,MAX_Usage                             string
    ,Remaining_Usage                       string
    ,StartDate                             string
    ,StopDate                              string
    ,DED_Type_Code                         string
    ,DED_Type_Name                         string
    ,Deduction_Type_Code                   string
    ,Deduction_Type_Desc                   string
    ,Domestic_Consignee_Code               string
    ,Domestic_Consignee_USCC               string
    ,Domestic_Consignor                    string
    ,Owner_Code                            string
    ,Owner_USCC                            string
    ,Owner_Name                            string
    ,Trade_Country                         string
    ,Permit_Origin_Destination_Country     string
    ,Customs_Jurisdiction                  string
    ,CIQ_Jurisdiction                      string
    ,Destination                           string
    ,Requestor                             string
    ,Responsible_Person                    string
    ,Veriifcation_Status_Code              string
    ,Veriifcation_Status_Desc              string
    ,Verification_Date                     string
    ,Stop_Alarm                            string
    ,Company_Transportation_Method         string
    ,Sequence_Number                       string
    ,Part_Number                           string
    ,Chinese_Description                   string
    ,HS_Code                               string
    ,Can_Use_Qty                           decimal(9,2)
    ,Use_Qty                               decimal(9,2)
    ,Remaining_Qty                         decimal(9,2)
    ,Customs_UM                            string
    ,Available_Amount                      decimal(9,2)
    ,Used_Amount                           decimal(9,2)
    ,Remaining_Amount                      decimal(9,2)
    ,Currency                              string
    ,Record_Net_Weight                     decimal(9,2)
    ,Used_NET_Weight                       decimal(9,2)
    ,Remain_Net_Weight                     decimal(9,2)
    ,Cost_Center                           string
    ,Division                              string
    ,Check_Flag                            string
    ,Check_Number                          string
    ,Check_Status                          string
    ,Usage                                 string
    ,Part_Model                            string
    ,Brand                                 string
    ,Origin_Destination_Country            string
) comment 'CTMCustomsPermissionCertification'
partitioned by(dt string)
stored as parquet 
location '/bsc/opsdw/dwd/dwd_ctm_CPC/'
tblproperties ("parquet.compression"="lzo");