-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dwd_iekpi_e2e_tj_merge(
 new_customdockdate	timestamp
,new_dockwarrantdate	timestamp
,new_inbounddeclaration_finishdate	timestamp
,new_intoinventorydate	timestamp
,new_invoice	text
,new_pickupdocument_no	text
,new_commercialinvoice	text
,new_commodityinspection_outboundcheck	timestamp
,new_dock_customdock_cd	decimal(18,2)
,new_dock_customdock_holiday	decimal(18,2)
,new_dock_customdock_wd	decimal(18,2)
,new_declaration_customdock_cd	decimal(18,2)
,new_declaration_customdock_holiday	decimal(18,2)
,new_declaration_customdock_wd	decimal(18,2)
,new_invent_customdock_cd	decimal(18,2)
,new_invent_customdock_holiday	decimal(18,2)
,new_invent_customdock_wd	decimal(18,2)
,new_outbound_invent_cd	decimal(18,2)
,new_outbound_invent_holiday	decimal(18,2)
,new_outbound_invent_wd	decimal(18,2)
,new_jsons text
,new_outbound_yr text)
RETURNS VOID AS
$$
BEGIN
    LOOP
        -- first try to update the key
        -- not there, so try to insert the key
        -- if someone else inserts the same key concurrently,
        -- we could get a unique-key failure
        BEGIN
            INSERT INTO dwd_iekpi_e2e_tj(customdockdate, dockwarrantdate, inbounddeclaration_finishdate, intoinventorydate, invoice, pickupdocument_no, commercialinvoice, commodityinspection_outboundcheck, dock_customdock_cd, dock_customdock_holiday, dock_customdock_wd, declaration_customdock_cd, declaration_customdock_holiday, declaration_customdock_wd, invent_customdock_cd, invent_customdock_holiday, invent_customdock_wd, outbound_invent_cd, outbound_invent_holiday, outbound_invent_wd,jsons,outbound_yr) VALUES (
                    new_customdockdate
                    ,new_dockwarrantdate
                    ,new_inbounddeclaration_finishdate
                    ,new_intoinventorydate
                    ,new_invoice
                    ,new_pickupdocument_no
                    ,new_commercialinvoice
                    ,new_commodityinspection_outboundcheck
                    ,new_dock_customdock_cd
                    ,new_dock_customdock_holiday
                    ,new_dock_customdock_wd
                    ,new_declaration_customdock_cd
                    ,new_declaration_customdock_holiday
                    ,new_declaration_customdock_wd
                    ,new_invent_customdock_cd
                    ,new_invent_customdock_holiday
                    ,new_invent_customdock_wd
                    ,new_outbound_invent_cd
                    ,new_outbound_invent_holiday
                    ,new_outbound_invent_wd
                    ,new_jsons
                    ,new_outbound_yr

                );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;