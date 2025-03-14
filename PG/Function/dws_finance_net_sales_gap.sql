-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_finance_net_sales_gap_merge(
  new_disvision text, 
  new_value_all numeric(18,2), 
  new_value_invoice numeric(18,2), 
  new_gap_value numeric(18,2), 
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
            INSERT INTO dws_finance_net_sales_gap(  disvision , 
                                                    value_all , 
                                                    value_invoice , 
                                                    gap_value , 
                                                    currency ,
                                                    year_mon ) VALUES (
            new_disvision , 
            new_value_all , 
            new_value_invoice , 
            new_gap_value , 
            new_currency ,
            new_year_mon  );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;