-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dwd_dim_cfda_upn_merge(
  new_registration_no text, 
  new_upn text, 
  new_valid_fromdate text, 
  new_valid_enddate text,
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
            INSERT INTO dwd_dim_cfda_upn(registration_no, upn, valid_fromdate, valid_enddate, dt) VALUES (
  new_registration_no , 
  new_upn , 
  new_valid_fromdate , 
  new_valid_enddate ,
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