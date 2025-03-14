# PG 与 HDFS
## 1. Hive export to HDFS
```SQL
insert OVERWRITE directory '/bsc/opsdw/export/ads_demo'
row format delimited fields terminated by '\t' 
stored as textfile
select * from ads_demo where id is not null;
```
## 2. PG Function
```sql
-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION demo_merge(new_id integer, new_name TEXT, new_age integer) 
RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        UPDATE demo SET name = new_name, age = new_age WHERE demo.id = new_id;
        IF found THEN
            RETURN;
        END IF;
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO demo(id, name, age) VALUES (new_id, new_name, new_age);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;
```

## 3. HDFS to PG (Update&Insert)
```sh
sqoop export --connect jdbc:postgresql://10.226.98.58:55433/bscdb --username postgres --password 1qazxsw2 \
--call demo_merge \
--export-dir '/bsc/opsdw/export/ads_demo' \
--fields-terminated-by '\t' \
--input-null-string '\\N' \
--input-null-non-string '\\N'
```
## 4. HDFS to PG (udpateOnly)
``` sh
sqoop export --connect jdbc:postgresql://10.226.98.58:55433/bscdb --username postgres --password 1qazxsw2 \
--table demo2 \
--export-dir '/bsc/opsdw/export/ads_demo' \
--fields-terminated-by '\t' \
--update-key 'id' \
--update-mode 'updateonly' \
--input-null-string '\\N' \
--input-null-non-string '\\N'
```