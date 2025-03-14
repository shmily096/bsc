-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dwt_finance_monthly_cs_freight_ocgs_htm_merge(
new_years text, 
new_mon_n text, 
new_mon_e text, 
new_file_name text,
new_disvision_old text, 
new_disvision text,
new_bu text,
new_items text, 
new_country text, 
new_items_2 text, 
new_value_all decimal(18,2), 
new_currency text, 
new_updatetime text, 
new_sheet_name text, 
new_versions text, 
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
            INSERT INTO dwt_finance_monthly_cs_freight_ocgs_htm(years, 
mon_n, 
mon_e, 
file_name, 
disvision_old, disvision,bu, items, country, items_2, value_all, currency, updatetime, sheet_name, versions, dt) VALUES (
                       new_years, 
                        new_mon_n, 
                        new_mon_e, 
                        new_file_name,
                        new_disvision_old, 
                        new_disvision,
                        new_bu,
                        new_items, 
                        new_country, 
                        new_items_2, 
                        new_value_all, 
                        new_currency, 
                        new_updatetime, 
                        new_sheet_name, 
                        new_versions, 
                        new_dt);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;