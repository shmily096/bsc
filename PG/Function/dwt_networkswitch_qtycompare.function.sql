-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dwt_networkswitch_qtycompare_merge(
	new_plant text ,
    new_storage_loc text,
    new_sloc text,
    new_default_location text,
	new_material text ,
	new_division_id text ,
	new_division_display_name text ,
	new_qty decimal(18,2) ,
	new_category text ,
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
            INSERT INTO dwt_networkswitch_qtycompare(plant,storage_loc,sloc,default_location, material, division_id, division_display_name, qty, category, dt) VALUES (
                        new_plant  ,
                        new_storage_loc ,
                        new_sloc ,
                        new_default_location ,
                        new_material  ,
                        new_division_id  ,
                        new_division_display_name  ,
                        new_qty  ,
                        new_category  ,
                        new_dt  );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;