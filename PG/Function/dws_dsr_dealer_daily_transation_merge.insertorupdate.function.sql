-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_dsr_dealer_daily_transation_merge(
   new_division text 
  ,new_sub_division text 
  ,new_customer_code text 
  ,new_cust_level3 text 
  ,new_bill_month text
  ,new_bill_year text 
  ,new_bill_date date 
  ,new_quarter integer 
  ,new_dealer_name text 
  ,new_parent_dealer_name text 
  ,new_dealer_complete numeric(16,2) 
  ,new_dealer_mon_target numeric(16,2) 
  ,new_dealer_mon_total_target numeric(16,2) 
  ,new_dt_year text
  ,new_dt_month text
  ,new_bill_qty integer  
  ,new_dt date)
RETURNS VOID AS
$$

BEGIN
    
    LOOP
        -- first try to update the key
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO dws_dsr_dealer_daily_transation(division, sub_division, customer_code, cust_level3, bill_month, bill_year, bill_date, quarter, dealer_name, parent_dealer_name, dealer_complete, dealer_mon_target, dealer_mon_total_target, dt_year, dt_month, bill_qty, dt)
            values(  new_division  
                    ,new_sub_division  
                    ,new_customer_code  
                    ,new_cust_level3  
                    ,new_bill_month 
                    ,new_bill_year  
                    ,new_bill_date 
                    ,new_quarter  
                    ,new_dealer_name  
                    ,new_parent_dealer_name  
                    ,new_dealer_complete 
                    ,new_dealer_mon_target 
                    ,new_dealer_mon_total_target 
                    ,new_dt_year 
                    ,new_dt_month 
                    ,new_bill_qty   
                    ,new_dt );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;