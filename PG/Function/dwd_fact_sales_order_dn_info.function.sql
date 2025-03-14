-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dwd_fact_sales_order_dn_info_merge(
  new_so_no text  , 
  new_delivery_id text  , 
  new_created_datetime text  , 
  new_updated_datetime text  , 
  new_created_by text  , 
  new_updated_by text  , 
  new_ship_to_address text  , 
  new_real_shipto_address text  , 
  new_planned_gi_date text  , 
  new_actual_gi_date text  , 
  new_receiving_confirmation_date text  , 
  new_delivery_mode text  , 
  new_carrier_id text , 
  new_pick_location_id text  , 
  new_total_qty decimal(18,2)  , 
  new_plant text  , 
  new_chinese_dncreatedt text,
  new_dt text)
RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO dwd_fact_sales_order_dn_info(so_no, delivery_id, created_datetime, updated_datetime, created_by, updated_by, ship_to_address, real_shipto_address, planned_gi_date, actual_gi_date, receiving_confirmation_date, delivery_mode, carrier_id, pick_location_id, total_qty, plant, chinese_dncreatedt, dt) VALUES (
                new_so_no
                ,new_delivery_id
                ,new_created_datetime
                ,new_updated_datetime
                ,new_created_by
                ,new_updated_by
                ,new_ship_to_address
                ,new_real_shipto_address
                ,new_planned_gi_date
                ,new_actual_gi_date
                ,new_receiving_confirmation_date
                ,new_delivery_mode
                ,new_carrier_id
                ,new_pick_location_id
                ,new_total_qty
                ,new_plant
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