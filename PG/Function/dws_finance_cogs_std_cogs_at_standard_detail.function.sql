-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_finance_cogs_std_cogs_at_standard_detail_merge(
  new_disvision text, 
  new_material text, 
  new_value_cny numeric(36,2), 
  new_qty numeric(36,2), 
  new_standard_cost_usd numeric(18,2), 
  new_standard_cost numeric(18,2), 
  new_standard_cost_value numeric(36,2), 
  new_currency text,
  new_year_mon text)
RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO dws_finance_cogs_std_cogs_at_standard_detail(
                                                                        disvision , 
                                                                        material , 
                                                                        value_cny , 
                                                                        qty , 
                                                                        standard_cost_usd , 
                                                                        standard_cost , 
                                                                        standard_cost_value , 
                                                                        currency ,
                                                                        year_mon ) 
                     VALUES (
                         new_disvision , 
                        new_material , 
                        new_value_cny , 
                        new_qty , 
                        new_standard_cost_usd , 
                        new_standard_cost , 
                        new_standard_cost_value , 
                        new_currency ,
                        new_year_mon );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;