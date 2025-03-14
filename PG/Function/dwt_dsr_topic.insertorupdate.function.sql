-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dwt_dsr_topic_merge(
  new_bill_month text, 
  new_bill_year text, 
  new_division text, 
  new_net_amount_shiped numeric(18,2), 
  new_net_amount_dn numeric(18,2), 
  new_fulfilled_value numeric(18,2), 
  new_rebate_value numeric(18,2), 
  new_net_cr_shiped numeric(18,2), 
  new_estimate_index numeric(18,2), 
  new_shiped_and_fulfill numeric(18,2), 
  new_net_amount_shiped_usd numeric(18,2), 
  new_net_amount_dn_usd numeric(18,2), 
  new_fulfilled_value_usd numeric(18,2), 
  new_rebate_value_usd numeric(18,2), 
  new_net_cr_shiped_usd numeric(18,2), 
  new_estimate_index_usd numeric(18,2), 
  new_shiped_and_fulfill_usd numeric(18,2), 
  new_le_cny numeric(18,2), 
  new_le_usd numeric(18,2), 
  new_srr_cny numeric(18,2), 
  new_srr_usd numeric(18,2), 
  new_bill_qty float8 ,
  new_available_qty float8, 
  new_pick_up_qty float8, 
  new_pick_up_qty_cr float8,
  new_total_workday int,
  new_total_workdaybytoday int,
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
            INSERT INTO dwt_dsr_topic(bill_month, bill_year, division, net_amount_shiped, net_amount_dn, fulfilled_value, rebate_value, net_cr_shiped, estimate_index, shiped_and_fulfill, net_amount_shiped_usd, net_amount_dn_usd, fulfilled_value_usd, rebate_value_usd, net_cr_shiped_usd, estimate_index_usd, shiped_and_fulfill_usd, le_cny, le_usd, srr_cny, srr_usd, bill_qty, available_qty, pick_up_qty, pick_up_qty_cr,total_workday,total_workdaybytoday, dt_year, dt_month) VALUES (
               new_bill_month , 
                new_bill_year , 
                new_division , 
                new_net_amount_shiped , 
                new_net_amount_dn , 
                new_fulfilled_value , 
                new_rebate_value , 
                new_net_cr_shiped , 
                new_estimate_index , 
                new_shiped_and_fulfill , 
                new_net_amount_shiped_usd , 
                new_net_amount_dn_usd , 
                new_fulfilled_value_usd , 
                new_rebate_value_usd , 
                new_net_cr_shiped_usd , 
                new_estimate_index_usd , 
                new_shiped_and_fulfill_usd , 
                new_le_cny , 
                new_le_usd , 
                new_srr_cny , 
                new_srr_usd , 
                new_bill_qty  ,
                new_available_qty , 
                new_pick_up_qty , 
                new_pick_up_qty_cr ,
                new_total_workday ,
                new_total_workdaybytoday ,
                new_dt_year , 
                new_dt_month   );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;