-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_finance_upn_qty_merge(
 new_finance_bu text,
 new_displayname text, 
 new_material text, 
 new_order_no text,
 new_delivery_id text, 
 new_order_type text, 
 new_value numeric(18,2), 
 new_dt text,
 new_category text, 
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
            INSERT INTO dws_finance_upn_qty(finance_bu, displayname, material, order_no, delivery_id, order_type, value, dt, category, year_mon) VALUES (
                    new_finance_bu ,
                    new_displayname , 
                    new_material , 
                    new_order_no ,
                    new_delivery_id , 
                    new_order_type , 
                    new_value, 
                    new_dt ,
                    new_category , 
                    new_year_mon 
                );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;