-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dwd_outbound_distribution_merge(
new_noo int8 ,  
new_signal_no text, 
new_biz_no text , 
new_css_no text , 
new_distributedoc_receivedate timestamp ,   
new_get_certificate text ,  
new_endorselist_receiveddate timestamp ,   
new_customclearance_preparationfinishdate timestamp ,
new_taxpayment_applicationdate timestamp ,  
new_taxpayment_completiondate timestamp ,  
new_customclearance_date timestamp , 
new_remark text ,                                  
new_is_excluded text , 
new_get_certificatelist text ,                                        
new_status text ,
new_distirib_custom_cd decimal(18,2),
new_distirib_custom_wd decimal(18,2),
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
            INSERT INTO dwd_outbound_distribution(noo, signal_no, biz_no, css_no, distributedoc_receivedate, get_certificate, endorselist_receiveddate, customclearance_preparationfinishdate, taxpayment_applicationdate, taxpayment_completiondate, customclearance_date, remark, is_excluded, get_certificatelist, status, distirib_custom_cd, distirib_custom_wd, dt) VALUES (
                       new_noo  ,  
                        new_signal_no , 
                        new_biz_no  , 
                        new_css_no  , 
                        new_distributedoc_receivedate  ,   
                        new_get_certificate  ,  
                        new_endorselist_receiveddate  ,   
                        new_customclearance_preparationfinishdate  ,
                        new_taxpayment_applicationdate  ,  
                        new_taxpayment_completiondate  ,  
                        new_customclearance_date  , 
                        new_remark  ,                                  
                        new_is_excluded  , 
                        new_get_certificatelist  ,                                        
                        new_status  ,
                        new_distirib_custom_cd ,
                        new_distirib_custom_wd ,
                        new_dt );
            RETURN;
        EXCEPTION WHEN unique_violation THEN
            -- Do nothing, and loop to try the UPDATE again.
        END;
    END LOOP;
END;
$$
LANGUAGE plpgsql;