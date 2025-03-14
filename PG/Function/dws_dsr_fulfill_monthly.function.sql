-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_dsr_fulfill_monthly_merge(
new_so_no text, 
new_order_datetime text , 
new_material text , 
new_division text , 
new_open_qty text , 
new_qty text , 
new_net_value decimal(18,2) ,
new_rebate_rate  decimal(18,2) ,
new_is_cr text  , 
new_balance_tag int  , 
new_fulfill_amount decimal(18,2) ,
new_fulfill_rebate decimal(18,4) ,
new_rnk int ,
new_dt_year int, 
new_dt_month int)
RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO dws_dsr_fulfill_monthly(so_no, order_datetime, material, division, open_qty, qty, net_value, rebate_rate, is_cr, balance_tag, fulfill_amount, fulfill_rebate, rnk, dt_year, dt_month) VALUES (
                        new_so_no , 
                        new_order_datetime  , 
                        new_material  , 
                        new_division  , 
                        new_open_qty  , 
                        new_qty  , 
                        new_net_value  ,
                        new_rebate_rate   ,
                        new_is_cr   , 
                        new_balance_tag   , 
                        new_fulfill_amount ,
                        new_fulfill_rebate  ,
                        new_rnk  ,
                        new_dt_year , 
                        new_dt_month 
                                                        );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;