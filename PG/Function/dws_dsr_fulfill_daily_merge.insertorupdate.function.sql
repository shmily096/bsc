-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_dsr_fulfill_daily_merge(
  new_open_qty integer, 
  new_total_onhand_qty integer, 
  new_order_datetime timestamp, 
  new_material text, 
  new_net_value numeric(18,2), 
  new_is_cr text, 
  new_division text, 
  new_rebate_rate numeric(18,2), 
  new_plant text, 
  new_total_open_qty integer, 
  new_total_value numeric(18,2), 
  new_so_no text ,
  new_dt_year text, 
  new_dt_month text)
RETURNS VOID AS
$$

BEGIN
    
    LOOP
        -- first try to update the key
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO dws_dsr_fulfill_daily(open_qty, total_onhand_qty, order_datetime, material, net_value, is_cr, division, rebate_rate, plant, total_open_qty, total_value, so_no, dt_year, dt_month)
             VALUES (   new_open_qty , 
                    new_total_onhand_qty , 
                    new_order_datetime , 
                    new_material , 
                    new_net_value , 
                    new_is_cr , 
                    new_division , 
                    new_rebate_rate , 
                    new_plant , 
                    new_total_open_qty , 
                    new_total_value , 
                    new_so_no  ,
                    new_dt_year , 
                    new_dt_month);
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;