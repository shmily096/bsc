-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_finance_cogs_inventory_revaluation_detail_merge(
  new_disvision text, 
  new_material text, 
  new_qty numeric(18,2), 
  new_unit_price numeric(38,10), 
  new_inventory_revaluation_value numeric(38,10), 
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
            INSERT INTO dws_finance_cogs_inventory_revaluation_detail(
                                                    disvision , 
                                                    material , 
                                                    qty , 
                                                    unit_price , 
                                                    inventory_revaluation_value , 
                                                    currency ,
                                                    year_mon) 
                     VALUES (
                              new_disvision , 
                                new_material , 
                                new_qty , 
                                new_unit_price , 
                                new_inventory_revaluation_value , 
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