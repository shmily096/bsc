-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dwd_trans_lpgreport_merge(
  new_upn text, 
  new_coo text, 
  new_coo_status text, 
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
            INSERT INTO dwd_trans_lpgreport(upn,coo,coo_status,dt)VALUES (
                         new_upn , 
                        new_coo , 
                        new_coo_status , 
                        new_dt  );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;