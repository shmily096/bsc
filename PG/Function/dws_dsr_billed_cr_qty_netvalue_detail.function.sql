-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_dsr_billed_cr_qty_netvalue_detail_merge(
  new_disvision TEXT, 
  new_material TEXT, 
  new_value_all numeric(36,2), 
  new_qty numeric(36,2), 
  new_currency TEXT,
  new_year_mon TEXT)
RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO dws_dsr_billed_cr_qty_netvalue_detail(
                                                            disvision , 
                                                            material , 
                                                            value_all , 
                                                            qty , 
                                                            currency ,
                                                            year_mon) VALUES (
                    new_disvision , 
                    new_material , 
                    new_value_all , 
                    new_qty , 
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