-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_eeo_detail_merge(
  new_plant text, 
  new_material text, 
  new_profitcenter text, 
  new_unrestricted numeric(18,2), 
  new_qualinspect numeric(18,2), 
  new_blockedstock numeric(18,2), 
  new_totalinventory numeric(18,2) , 
  new_extk numeric(18,2), 
  new_reserve numeric(18,2) , 
  new_file_name text, 
  new_small_version text, 
  new_locations text, 
  new_pul text, 
  new_io text, 
  new_update_dt text,
  new_eeo_flag text, 
  new_versions text)
RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO dws_eeo_detail(    plant , 
  material , 
  profitcenter , 
  unrestricted , 
  qualinspect , 
  blockedstock , 
  totalinventory  , 
  extk , 
  reserve  , 
  file_name , 
  small_version , 
  locations , 
  pul , 
  io , 
  update_dt ,
  eeo_flag , 
  versions  ) VALUES (
            new_plant , 
            new_material , 
            new_profitcenter , 
            new_unrestricted , 
            new_qualinspect , 
            new_blockedstock , 
            new_totalinventory  , 
            new_extk , 
            new_reserve  , 
            new_file_name , 
            new_small_version , 
            new_locations , 
            new_pul , 
            new_io , 
            new_update_dt ,
            new_eeo_flag , 
            new_versions );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;