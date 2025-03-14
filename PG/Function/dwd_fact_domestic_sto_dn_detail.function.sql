-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dwd_fact_domestic_sto_dn_detail_merge(
 new_sto_no	text
,new_delivery_no	text
,new_line_number	text
,new_material	text
,new_qty	bigint
,new_batch	text
,new_qr_code	text
,new_chinese_dncreatedt	text
,new_dt	text)
RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO dwd_fact_domestic_sto_dn_detail(sto_no, delivery_no, line_number, material, qty, batch, qr_code, chinese_dncreatedt, dt) VALUES (
                     new_sto_no	
                    ,new_delivery_no	
                    ,new_line_number	
                    ,new_material	
                    ,new_qty	
                    ,new_batch	
                    ,new_qr_code	
                    ,new_chinese_dncreatedt	
                    ,new_dt	
                );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;