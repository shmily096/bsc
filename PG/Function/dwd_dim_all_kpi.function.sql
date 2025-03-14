-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dwd_dim_all_kpi_merge(
  new_kpicode text, 
  new_func text, 
  new_stream text, 
  new_sub_stream text, 
  new_category text, 
  new_supplier text, 
  new_index text, 
  new_unit text, 
  new_formula text, 
  new_index_level int, 
  new_criteria decimal(18,2), 
  new_target decimal(18,2), 
  new_vaild_from text, 
  new_vaild_to text,
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
            INSERT INTO dwd_dim_all_kpi(kpicode, func, stream, sub_stream, category, supplier, index, unit, formula, index_level, criteria, target, vaild_from, vaild_to, dt) VALUES (
                       new_kpicode , 
                        new_func , 
                        new_stream , 
                        new_sub_stream , 
                        new_category , 
                        new_supplier , 
                        new_index , 
                        new_unit , 
                        new_formula , 
                        new_index_level , 
                        new_criteria , 
                        new_target , 
                        new_vaild_from , 
                        new_vaild_to ,
                        new_dt );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;