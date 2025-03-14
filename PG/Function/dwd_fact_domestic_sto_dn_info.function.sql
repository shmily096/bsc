-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dwd_fact_domestic_sto_dn_info_merge(
 new_sto_no	text
,new_delivery_no	text
,new_reference_dn_number	text
,new_create_datetime	text
,new_create_by	text
,new_update_datetime	text
,new_update_by	text
,new_delivery_mode	text
,new_dn_status	text
,new_ship_from_location	text
,new_ship_from_plant	text
,new_ship_to_plant	text
,new_ship_to_location	text
,new_carrier	text
,new_actual_migo_date	text
,new_planned_good_issue_datetime	text
,new_actua_good_issue_datetime	text
,new_total_qty	bigint
,new_actual_putaway_datetime	text
,new_pgi_datetime	text
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
            INSERT INTO dwd_fact_domestic_sto_dn_info(sto_no, delivery_no, reference_dn_number, create_datetime, create_by, update_datetime, update_by, delivery_mode, dn_status, ship_from_location, ship_from_plant, ship_to_plant, ship_to_location, carrier, actual_migo_date, planned_good_issue_datetime, actua_good_issue_datetime, total_qty, actual_putaway_datetime, pgi_datetime, chinese_dncreatedt, dt) VALUES (
 new_sto_no	
,new_delivery_no	
,new_reference_dn_number	
,new_create_datetime	
,new_create_by	
,new_update_datetime	
,new_update_by	
,new_delivery_mode	
,new_dn_status	
,new_ship_from_location	
,new_ship_from_plant	
,new_ship_to_plant	
,new_ship_to_location	
,new_carrier	
,new_actual_migo_date	
,new_planned_good_issue_datetime	
,new_actua_good_issue_datetime	
,new_total_qty	
,new_actual_putaway_datetime	
,new_pgi_datetime	
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