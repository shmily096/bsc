-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_kpi_dwd_fact_import_export_dn_info_pdt_merge(
  new_sto_no text , 
  new_delivery_no text , 
  new_reference_dn_no text , 
  new_order_status text , 
  new_ship_from_plant text , 
  new_ship_to_plant text , 
  new_total_qty bigint , 
  new_actual_good_issue_datetime timestamp , 
  new_actual_migo_datetime timestamp , 
  new_fin_dim_id text , 
  new_item_business_group text ,
  new_lt_cd_hr float ,
  new_lt_dw_hr float ,
  new_KPI_no text,
  new_dt_yearmon text,
  new_dt date)
RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO dws_kpi_dwd_fact_import_export_dn_info_pdt(sto_no, delivery_no, reference_dn_no, order_status, ship_from_plant, ship_to_plant, total_qty, actual_good_issue_datetime, actual_migo_datetime, fin_dim_id, item_business_group, lt_cd_hr, lt_dw_hr, kpi_no, dt_yearmon, dt) VALUES (
                      new_sto_no  , 
                        new_delivery_no  , 
                        new_reference_dn_no  , 
                        new_order_status  , 
                        new_ship_from_plant  , 
                        new_ship_to_plant  , 
                        new_total_qty  , 
                        new_actual_good_issue_datetime  , 
                        new_actual_migo_datetime  , 
                        new_fin_dim_id  , 
                        new_item_business_group  ,
                        new_lt_cd_hr  ,
                        new_lt_dw_hr  ,
                        new_KPI_no ,
                        new_dt_yearmon ,
                        new_dt 
                );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;