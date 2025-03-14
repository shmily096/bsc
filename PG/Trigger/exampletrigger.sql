CREATE TRIGGER dwt_networkswitch_qtycompare_trigger AFTER INSERT ON dwt_networkswitch_qtycompare 
FOR EACH STATEMENT EXECUTE PROCEDURE upnlistfunc();

CREATE OR REPLACE FUNCTION upnlistfunc() RETURNS TRIGGER AS $$
   BEGIN
      INSERT INTO networkswitch_upn_list(material, plant,upn_status,create_date,finish_date) 
        select
            material,
            plant,
            '切换中' as upn_status,
            dt as create_date,
            null as finish_date
        from dwt_networkswitch_qtycompare 
        where case when plant='SH' AND coalesce(ur,0)+coalesce(qi,0)+coalesce(git,0)-coalesce(open_dn_qty,0)+coalesce(in_transit,0) <=coalesce(openor_qty,0) THEN 1 
        WHEN plant='TJ' AND coalesce(ur,0)+coalesce(qi,0)+coalesce(git,0)-coalesce(open_dn_qty,0)<=coalesce(openor_qty,0) THEN 1 
        ELSE 0 END =1
        and not exists(select 1 from networkswitch_upn_list a where a.material=dwt_networkswitch_qtycompare.material  and a.plant=dwt_networkswitch_qtycompare.plant );

        UPDATE networkswitch_upn_list
        SET upn_status = '切换完成' 
        AND finish_date =current_date
        WHERE exists (select 1 from dwt_networkswitch_qtycompare b where networkswitch_upn_list.material=b.material 
            and networkswitch_upn_list.plant=b.plant 
            and case when plant='SH' AND coalesce(ur,0)+coalesce(qi,0)+coalesce(git,0)-coalesce(open_dn_qty,0)+coalesce(in_transit,0) +coalesce(openor_qty,0)=0 THEN 1 
        WHEN plant='TJ' AND coalesce(ur,0)+coalesce(qi,0)+coalesce(git,0)-coalesce(open_dn_qty,0)+coalesce(openor_qty,0)=0 THEN 1 
        ELSE 0 END =1 );

      RETURN NEW;
   END;
$$ LANGUAGE plpgsql;