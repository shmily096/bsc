-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_kpi_monthly_isolate_stock_merge(
  new_name text, 
  new_plant text , 
  new_location text , 
  new_status text , 
  new_material text , 
  new_batch text , 
  new_qty decimal(18,2) , 
  new_flag text, 
  new_supplier text ,
  new_kpi_code text,
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
            INSERT INTO dws_kpi_monthly_isolate_stock(name, plant, location, status, material, batch, qty, flag, supplier,kpi_code,dt) VALUES (
                    new_name , 
                    new_plant  , 
                    new_location  , 
                    new_status  , 
                    new_material  , 
                    new_batch  , 
                    new_qty  , 
                    new_flag , 
                    new_supplier  ,
                    new_kpi_code,
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