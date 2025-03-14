-- Hive SQL
-- Function： CFDA数据 （ODS 层）
-- History: 
-- 2021-11-15    Amanda   v1.0    draft

drop table if exists ods_RRSalesForecast;
create external table ods_RRSalesForecast
(
      UpdateDT                              string
      ,Active                                string
      ,Part                                  string
      ,SuperDivision                         string
      ,Division2                             string
      ,Division                              string
      ,ProcurementSegment                    string
      ,ProductGroup                          string
      ,Family                                string
      ,Type                                  string
      ,Part1                                 string
      ,Month1                                decimal(9,2)
      ,Month2                                decimal(9,2)
      ,Month3                                decimal(9,2)
      ,Month4                                decimal(9,2)
      ,Month5                                decimal(9,2)
      ,Month6                                decimal(9,2)
      ,Month7                                decimal(9,2)
      ,Month8                                decimal(9,2)
      ,Month9                                decimal(9,2)
      ,Month10                               decimal(9,2)
      ,Month11                               decimal(9,2)
      ,Month12                               decimal(9,2)
      ,Month13                               decimal(9,2)
      ,Month14                               decimal(9,2)
      ,Month15                               decimal(9,2)
      ,Month16                               decimal(9,2)
      ,Month17                               decimal(9,2)
      ,Month18                               decimal(9,2)
      ,Month19                               decimal(9,2)
      ,Month20                               decimal(9,2)
      ,Month21                               decimal(9,2)
      ,Month22                               decimal(9,2)
      ,Month23                               decimal(9,2)
      ,Month24                               decimal(9,2)
      ,ForcastVersion                        string
      ,UpdateDate                            string
      ,ForcastCycle                          string
      ,PLANT                                 string
) comment 'RRSalesForecast'
partitioned by(dt string)
row format delimited fields terminated by '\t' 
location '/bsc/opsdw/ods/ods_RRSalesForecast/'
;