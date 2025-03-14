-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dwd_fact_inventory_onhand_merge(
  new_trans_date text, 
  new_inventory_type text, 
  new_plant text, 
  new_storage_loc text, 
  new_profic_center text, 
  new_material text, 
  new_batch text, 
  new_quantity decimal(18,2), 
  new_unrestricted decimal(18,2), 
  new_inspection decimal(18,2), 
  new_blocked_material decimal(18,2), 
  new_expiration_date text, 
  new_standard_cost decimal(18,2), 
  new_extended_cost decimal(18,2), 
  new_update_date text, 
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
            INSERT INTO dwd_fact_inventory_onhand(trans_date, inventory_type, plant, storage_loc, profic_center, material, batch, quantity, unrestricted, inspection, blocked_material, expiration_date, standard_cost, extended_cost, update_date, dt) VALUES (
                new_trans_date
                ,new_inventory_type
                ,new_plant
                ,new_storage_loc
                ,new_profic_center
                ,new_material
                ,new_batch
                ,new_quantity
                ,new_unrestricted
                ,new_inspection
                ,new_blocked_material
                ,new_expiration_date
                ,new_standard_cost
                ,new_extended_cost
                ,new_update_date
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