-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dwd_dim_kpi_merge(
  new_kpicode text , 
  new_functio text , 
  new_stream text , 
  new_sub_stream text , 
  new_category text , 
  new_supplier text , 
  new_index text , 
  new_index_level int , 
  new_criteria text , 
  new_target decimal(18,2) , 
  new_unit text , 
  new_yr text , 
  new_mon text,
  new_formula text)
RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO dwd_dim_kpi(kpicode, functio, stream, sub_stream, category, supplier, "index", index_level, criteria, target, unit, yr, mon,formula) VALUES (
                     new_kpicode  , 
                    new_functio  , 
                    new_stream  , 
                    new_sub_stream  , 
                    new_category  , 
                    new_supplier  , 
                    new_index  , 
                    new_index_level  , 
                    new_criteria  , 
                    new_target  , 
                    new_unit  , 
                    new_yr  , 
                    new_mon ,
                    new_formula
                );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;