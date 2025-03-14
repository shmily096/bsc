-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_dsr_cr_daily_merge(
  new_so_no text, 
  new_bill_date text, 
  new_material text, 
  new_net_cr numeric(18, 2), 
  new_division_display_name text, 
  new_upn_del_flag text, 
  new_cust_del_flag text, 
  new_orderreason_del_flag text, 
  new_billtype_del_flag text, 
  new_dt_year text, 
  new_dt_month text,
  new_cr_qty float8,
  new_customer_code text,
  new_dt date)
RETURNS VOID AS
$$
BEGIN
    
    -- first try to update the key
            -- not there, so try to insert the key
    -- if someone else inserts the same key concurrently,
    -- we could get a unique-key failure
    BEGIN
        INSERT INTO dws_dsr_cr_daily(so_no, bill_date, material, net_cr, division_display_name, upn_del_flag, cust_del_flag, orderreason_del_flag, billtype_del_flag, dt_year, dt_month,cr_qty,customer_code, dt)
        VALUES( new_so_no  
                ,new_bill_date  
                ,new_material  
                ,new_net_cr  
                ,new_division_display_name  
                ,new_upn_del_flag  
                ,new_cust_del_flag  
                ,new_orderreason_del_flag  
                ,new_billtype_del_flag  
                ,new_dt_year  
                ,new_dt_month 
                ,new_cr_qty 
                ,new_customer_code
                ,new_dt );
        RETURN;
    EXCEPTION WHEN unique_violation THEN
        -- Do nothing, and loop to try the UPDATE again.
    END;

END;
$$
LANGUAGE plpgsql;