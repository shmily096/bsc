-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dwt_networkswitch_upn_list_merge(
	new_plant text ,
	new_material text ,
	new_upn_status text ,
	new_create_date date ,
	new_finish_date date ,
	new_update_dt text )
RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO dwt_networkswitch_upn_list(plant, material, upn_status,create_date,finish_date,update_dt) VALUES (
                        new_plant  ,
                        new_material  ,
                        new_upn_status  ,
                        new_create_date  ,
                        new_finish_date  ,
                        new_update_dt    );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;