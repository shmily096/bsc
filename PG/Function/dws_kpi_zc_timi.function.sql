-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_kpi_zc_timi_merge(
   new_sto_no text 
  ,new_delivery_no text 
  ,new_reference_dn_number text 
  ,new_create_datetime timestamp  
  ,new_actual_migo_date timestamp  
  ,new_pgi_datetime timestamp  
  ,new_delivery_mode text 
  ,new_dn_status text 
  ,new_ship_from_location text 
  ,new_ship_from_plant text 
  ,new_from_supplier text
  ,new_to_supplier text
  ,new_ship_to_plant text 
  ,new_ship_to_location text 
  ,new_material text 
  ,new_batch text 
  ,new_qty integer  
  ,new_lt_cd_hr integer  
  ,new_no_work_hr integer  
  ,new_lt_dw_hr integer  
  ,new_kpi_no text 

  ,new_dt_yearmon text 
  ,new_dt_week text
    ,new_dt text)
RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO dws_kpi_zc_timi(sto_no, delivery_no, reference_dn_number, create_datetime, actual_migo_date, pgi_datetime, delivery_mode, dn_status, ship_from_location, ship_from_plant, from_supplier, to_supplier, ship_to_plant, ship_to_location, material, batch, qty, lt_cd_hr, no_work_hr, lt_dw_hr, kpi_no, dt_yearmon, dt_week, dt) VALUES (
                   new_sto_no  
                    ,new_delivery_no  
                    ,new_reference_dn_number  
                    ,new_create_datetime   
                    ,new_actual_migo_date   
                    ,new_pgi_datetime   
                    ,new_delivery_mode  
                    ,new_dn_status  
                    ,new_ship_from_location  
                    ,new_ship_from_plant 
                    ,new_from_supplier 
                    ,new_to_supplier  
                    ,new_ship_to_plant  
                    ,new_ship_to_location  
                    ,new_material  
                    ,new_batch  
                    ,new_qty   
                    ,new_lt_cd_hr   
                    ,new_no_work_hr   
                    ,new_lt_dw_hr   
                    ,new_kpi_no  
                    
                    ,new_dt_yearmon  
                    ,new_dt_week 
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