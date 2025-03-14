-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_kpi_sales_waybill_timi_merge(
   new_so_no text,
 new_delivery_id text,
 new_created_datetime timestamp ,
 new_actual_gi_date timestamp ,
 new_receiving_confirmation_date timestamp ,
 new_ship_to_address text,
 new_pick_location_id text,
 new_plant text, 
 new_supplier text, 
 new_material text,
 new_batch text,
 new_qty float8 , 
 new_lt_cd_hr integer ,
 new_no_work_hr integer ,
 new_lt_dw_hr integer , 
 new_kpi_no text,
 new_dt_yearmon text ,
 new_dt_week text, 
 new_dt text 
)
RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO dws_kpi_sales_waybill_timi(so_no, delivery_id, created_datetime, actual_gi_date, receiving_confirmation_date, ship_to_address, pick_location_id, plant,supplier, material, batch, qty, lt_cd_hr, no_work_hr, lt_dw_hr, kpi_no, dt_yearmon,dt_week, dt) VALUES (
                new_so_no ,
                new_delivery_id ,
                new_created_datetime  ,
                new_actual_gi_date  ,
                new_receiving_confirmation_date  ,
                new_ship_to_address ,
                new_pick_location_id ,
                new_plant , 
                new_supplier ,
                new_material ,
                new_batch ,
                new_qty  , 
                new_lt_cd_hr  ,
                new_no_work_hr  ,
                new_lt_dw_hr  , 
                new_kpi_no , 
                new_dt_yearmon  ,
                new_dt_week ,
                new_dt  
                );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;