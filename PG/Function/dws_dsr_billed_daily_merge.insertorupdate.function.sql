-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_dsr_billed_daily_merge(
new_so_no text, 
new_net_billed numeric(18, 2), 
new_bill_date text, 
new_material text, 
new_billed_rebate numeric(18, 2), 
new_division text, 
new_sub_division text, 
new_upn_del_flag text, 
new_cust_del_flag text, 
new_orderreason_del_flag text, 
new_billtype_del_flag text, 
new_customer_code text, 
new_dt_year text, 
new_dt_month text, 
new_bill_qty text ,
new_dt date,
new_num integer)
RETURNS VOID AS
$$

BEGIN
    
    LOOP
        -- first try to update the key
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO dws_dsr_billed_daily(so_no, net_billed, bill_date, material, billed_rebate, division, sub_division, upn_del_flag, cust_del_flag, orderreason_del_flag, billtype_del_flag, customer_code, dt_year, dt_month, bill_qty, dt) VALUES (  new_so_no ,  new_net_billed ,   new_bill_date ,   new_material ,   new_billed_rebate ,   new_division ,   new_sub_division ,
            new_upn_del_flag , 
            new_cust_del_flag , 
            new_orderreason_del_flag , 
            new_billtype_del_flag , 
            new_customer_code , 
            new_dt_year , 
            new_dt_month , 
            new_bill_qty  ,
            new_dt );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;