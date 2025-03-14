-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_kpi_sto_migo2pgi_merge(
    new_plant text, 
    new_upn text, 
    new_bt text, 
    new_inventorystatus text, 
    new_sapinbounddn text, 
    new_workorderno text, 
    new_batch text, 
    new_inbounddn text,
    new_outbounddn text, 
    new_supplier text, 
    new_id text, 
    new_inboundtime text, 
    new_outboundtime timestamp, 
    new_localizationfinishtime timestamp,
    new_qty float, 
    new_lt_cd_hr float , 
    new_no_work_hr float , 
    new_lt_dw_hr float, 
    new_kpi_no text ,
    new_is_pacemaker text,
    new_distribution_properties text,
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
            INSERT INTO dws_kpi_sto_migo2pgi(plant, upn, bt, inventorystatus, sapinbounddn, workorderno, batch, inbounddn, outbounddn, supplier, id, inboundtime, outboundtime, localizationfinishtime,qty, lt_cd_hr, no_work_hr, lt_dw_hr, kpi_no,is_pacemaker,distribution_properties, dt) VALUES (
                 	new_plant , 
                    new_upn , 
                    new_bt , 
                    new_inventorystatus , 
                    new_sapinbounddn , 
                    new_workorderno , 
                    new_batch , 
                    new_inbounddn ,
                    new_outbounddn , 
                    new_supplier , 
                    new_id , 
                    new_inboundtime , 
                    new_outboundtime , 
                    new_localizationfinishtime ,
                    new_qty , 
                    new_lt_cd_hr  , 
                    new_no_work_hr  , 
                    new_lt_dw_hr , 
                    new_kpi_no  ,
                    new_is_pacemaker,
                    new_distribution_properties,
                    new_dt );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;