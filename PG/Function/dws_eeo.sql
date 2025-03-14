-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_eeo_merge(
  new_material text, 
  new_profitcenter int, 
  new_file_name text, 
  new_small_version text , 
  new_unrestricted numeric(18,2), 
  new_qualinspect numeric(18,2), 
  new_blockedstock numeric(18,2), 
  new_totalinventory numeric(18,2), 
  new_reserve numeric(18,2), 
  new_pricechange numeric(18,2), 
  new_last_reserve numeric(18,2) , 
  new_finall_reserve numeric(18,2),
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
            INSERT INTO dws_eeo(     material , 
  profitcenter , 
  file_name , 
  small_version  , 
  unrestricted , 
  qualinspect , 
  blockedstock , 
  totalinventory , 
  reserve , 
  pricechange , 
  last_reserve  , 
  finall_reserve ,
  eeo_flag , 
  versions ) VALUES (
    new_material , 
    new_profitcenter , 
    new_file_name , 
    new_small_version  , 
    new_unrestricted , 
    new_qualinspect , 
    new_blockedstock , 
    new_totalinventory , 
    new_reserve , 
    new_pricechange , 
    new_last_reserve  , 
    new_finall_reserve ,
    new_eeo_flag , 
    new_versions);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;