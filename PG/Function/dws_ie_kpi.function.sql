-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dws_ie_kpi_merge(
	new_kpicode text , 
    new_plant text , 
	new_housewaybillno text,
	new_forwording text ,
	new_supplier text ,
	new_need_calcaulated text ,
	new_t1pickupdate timestamp ,
	new_dockwarrantdate timestamp ,
	new_intoinventorydate timestamp , 
    new_intoinventorydate_kpi timestamp , 
	new_migo_date timestamp ,
	new_shipment_number text ,
	new_shipmenttype text ,
	new_invoice text , 
	new_upn text , 
	new_qty decimal(9,2) ,
	new_lt_day_cd  decimal(9,2) ,
    new_lt_day_wd  decimal(9,2) ,
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
            INSERT INTO dws_ie_kpi(kpicode,  plant,housewaybillno, forwording, supplier, need_calcaulated, t1pickupdate, dockwarrantdate, intoinventorydate, intoinventorydate_kpi,migo_date, shipment_number, shipmenttype, invoice, upn, qty, lt_day_cd  ,lt_day_wd, dt) VALUES (
                 	new_kpicode  , 
                      new_plant , 
                    new_housewaybillno ,
                    new_forwording  ,
                    new_supplier  ,
                    new_need_calcaulated  ,
                    new_t1pickupdate  ,
                    new_dockwarrantdate  ,
                    new_intoinventorydate  ,
                    new_intoinventorydate_kpi, 
                    new_migo_date  ,
                    new_shipment_number  ,
                    new_shipmenttype  ,
                    new_invoice  , 
                    new_upn  , 
                    new_qty  ,
                    new_lt_day_cd  ,
                    new_lt_day_wd  ,
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