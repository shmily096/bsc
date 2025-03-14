-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_kpi_cc_so_delivery_merge(
  new_material text , 
  new_batch text , 
  new_so_no text , 
  new_delivery_id text , 
  new_plant text , 
  new_pick_location_id text , 
  new_qty decimal(18,2) , 
  new_line int, 
  new_this_mon_flag int , 
  new_flag text ,
  new_dt date )
RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO dws_kpi_cc_so_delivery(material, batch, so_no, delivery_id, plant, pick_location_id, qty, line, this_mon_flag, flag, dt) VALUES (
                    new_material  , 
                    new_batch  , 
                    new_so_no  , 
                    new_delivery_id  , 
                    new_plant  , 
                    new_pick_location_id  , 
                    new_qty  , 
                    new_line , 
                    new_this_mon_flag  , 
                    new_flag  ,
                    new_dt    );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;