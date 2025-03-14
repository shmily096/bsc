-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dwd_dim_dsr_le_srr_test_merge(
  new_division text , 
  new_year text , 
  new_month text , 
  new_le_cny decimal(18,4) , 
  new_le_usd decimal(18,4) , 
  new_srr_cny decimal(18,4) , 
  new_srr_usd decimal(18,4) , 
  new_srr_version int , 
  new_dp_prder_project decimal(18,4), 
  new_sales_ro_comment text,
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
            INSERT INTO dwd_dim_dsr_le_srr_test( division, "year", "month", le_cny, le_usd, srr_cny, srr_usd, srr_version, dp_prder_project, sales_ro_comment, dt
) VALUES (
                         new_division  , 
  new_year  , 
  new_month  , 
  new_le_cny  , 
  new_le_usd  , 
  new_srr_cny  , 
  new_srr_usd  , 
  new_srr_version  , 
  new_dp_prder_project , 
  new_sales_ro_comment ,
  new_dt);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;