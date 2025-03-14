drop table if exists ads_demo;
create external table ads_demo
(
    ID                string comment 'ID',
    name              string comment 'name',
    age               int comment 'Age'
) comment 'ads DEMO '
row format delimited fields terminated by '\t' 
location '/bsc/opsdw/ads/ads_demo/';

---
insert  OVERWRITE directory '/bsc/opsdw/export/ads_demo'
row format delimited fields terminated by '\t' 
stored as textfile
select id, name, age from ads_demo where id is not null;