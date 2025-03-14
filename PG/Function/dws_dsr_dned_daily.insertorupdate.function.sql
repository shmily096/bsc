-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_dsr_dned_daily_merge(
      new_so_no text, 
  new_material text, 
  new_qty float8, 
  new_net_dned numeric(18,2), 
  new_dn_rebate numeric(18,2), 
  new_division_display_name text, 
  new_plant text, 
  new_dn_create_datetime date, 
  new_if_cr text, 
  new_upn_del_flag text, 
  new_cust_del_flag text, 
  new_orderreason_del_flag text, 
  new_billtype_del_flag text, 
  new_dt_year text, 
  new_dt_month text
) 
RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO dws_dsr_dned_daily(so_no, material, qty, net_dned, dn_rebate, division_display_name, plant, dn_create_datetime, if_cr, upn_del_flag, cust_del_flag, orderreason_del_flag, billtype_del_flag, dt_year, dt_month) VALUES (
                  new_so_no , 
                    new_material , 
                    new_qty , 
                    new_net_dned , 
                    new_dn_rebate , 
                    new_division_display_name , 
                    new_plant , 
                    new_dn_create_datetime , 
                    new_if_cr , 
                    new_upn_del_flag , 
                    new_cust_del_flag , 
                    new_orderreason_del_flag , 
                    new_billtype_del_flag , 
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