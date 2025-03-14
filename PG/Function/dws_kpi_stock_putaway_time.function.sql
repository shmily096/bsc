-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_kpi_stock_putaway_time_merge(
	 new_plant 	text
	,new_stock_location 	text
    ,new_delivery_plant text 
	,new_default_location text 
	,new_supplier 	text
	,new_material 	text
	,new_batch	text
	,new_delivery_no	text
	,new_qty	int4
	,new_qty_gap	text
	,new_start_time	text
	,new_end_time	text
	,new_processtime_cd  int4
	,new_no_work_hr 	int4
	,new_process_wd_m	int4
	,new_process_category text
	,new_kpicode 	text
	,new_dt text
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
            INSERT INTO dws_kpi_stock_putaway_time(plant, stock_location, delivery_plant,default_location,supplier, material, batch, delivery_no, qty, qty_gap, start_time, end_time, processtime_cd, no_work_hr, process_wd_m, process_category, kpicode, dt) VALUES (
                   	 new_plant 	
                    ,new_stock_location 	
                    ,new_delivery_plant
                    ,new_default_location 
                    ,new_supplier 	
                    ,new_material 	
                    ,new_batch	
                    ,new_delivery_no	
                    ,new_qty	
                    ,new_qty_gap	
                    ,new_start_time	
                    ,new_end_time	
                    ,new_processtime_cd  
                    ,new_no_work_hr 	
                    ,new_process_wd_m	
                    ,new_process_category 
                    ,new_kpicode 	
                    ,new_dt 
                );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;