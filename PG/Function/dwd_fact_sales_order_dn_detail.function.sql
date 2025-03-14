-- PG stored procedure template for inserted or uppdated action

create or replace FUNCTION dwd_fact_sales_order_dn_detail_merge(
   new_so_no text , 
  new_delivery_id text , 
  new_line_number text , 
  new_material text , 
  new_qty decimal(18,2) , 
  new_batch text , 
  new_qr_code text , 
  new_plant text , 
  new_chinese_dncreatedt text,
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
            INSERT INTO dwd_fact_sales_order_dn_detail(so_no, delivery_id, line_number, material, qty, batch, qr_code, plant, chinese_dncreatedt,dt) VALUES (
            new_so_no  , 
            new_delivery_id  , 
            new_line_number  , 
            new_material  , 
            new_qty  , 
            new_batch  , 
            new_qr_code  , 
            new_plant  , 
            new_chinese_dncreatedt ,
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