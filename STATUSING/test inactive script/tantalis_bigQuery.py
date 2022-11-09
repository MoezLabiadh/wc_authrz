def load_sql ():
    """Returns a dictionnary of SQL queries"""
    
    sql = {}
    
    sql['inactive'] = """
                    select
                      cast(mm.intrid_sid as number) intrid_sid,
                      cast(mm.disposition_transaction_sid as number) disposition_transaction_sid,
                      mm.expiry_dat as p_expiry_dat,
                      nn.file_chr,
                      nn.stage_nme,
                      nn.purpose_nme,
                      nn.subpurpose_nme,
                      nn.type_nme,
                      nn.subtype_nme,
                      nn.status_nme,
                      nn.dts_activation_cde,
                      nn.location_dsc

                from (select
                      cast(jj.disposition_transaction_sid as number) disposition_transaction_sid,
                      cast(jj.disposition_sid as number) disposition_sid,
                      kk.activation_cde as d_activation_cde,
                      kk.file_chr,
                      jj.purpose_nme,
                      jj.subpurpose_nme,
                      jj.type_nme,
                      jj.subtype_nme,
                      jj.unit_name,
                      jj.street_address,
                      jj.city,
                      jj.province_abbr,
                      jj.commencement_dat,
                      jj.document_chr,
                      jj.entered_dat,
                      jj.expiry_dat as dt_expiry_dat,
                      jj.location_dsc,
                      jj.offer_timber_deferment_yrn,
                      jj.received_dat,
                      jj.surface_or_under_cde,
                      jj.timber_value_dlr,
                      ll.effective_dat,
                      ll.code_chr_status,
                      ll.status_nme,
                      ll.dts_activation_cde,
                      ll.status_expiry_dat,
                      ll.code_chr_stage,
                      ll.stage_nme,
                      ll.stage_expiry_dat
                from (select
                      cast(aa.disposition_transaction_sid as number) disposition_transaction_sid,
                      cast(aa.disposition_sid as number) disposition_sid,
                      bb.purpose_nme,
                      cc.subpurpose_nme,
                      dd.type_nme,
                      ee.subtype_nme,
                      ff.unit_name,
                      ff.street_address,
                      ff.city,
                      ff.province_abbr,
                      ff.country,
                      ff.postal_code,
                      aa.commencement_dat,
                      aa.document_chr,
                      aa.entered_dat,
                      aa.expiry_dat,
                      aa.location_dsc,
                      aa.offer_timber_deferment_yrn,
                      aa.received_dat,
                      aa.surface_or_under_cde,
                      aa.timber_value_dlr
                from
                      whse_tantalis.ta_disposition_transactions aa,
                      whse_tantalis.ta_available_purposes bb,
                      whse_tantalis.ta_available_subpurposes cc,
                      whse_tantalis.ta_available_types dd,
                      whse_tantalis.ta_available_subtypes ee,
                      whse_tantalis.ta_organization_units ff
                where
                      aa.purpose_sid = bb.purpose_sid and
                      bb.purpose_sid = cc.purpose_sid and
                      aa.subpurpose_sid = cc.subpurpose_sid and
                      aa.type_sid = dd.type_sid and
                      dd.type_sid = ee.type_sid and
                      aa.subtype_sid = ee.subtype_sid and
                      aa.org_unit_sid = ff.org_unit_sid 
                order by
                      aa.disposition_transaction_sid) jj,
                      whse_tantalis.ta_dispositions kk,
                      (select gg.disposition_transaction_sid,
                      gg.effective_dat,
                      gg.code_chr_status,
                      hh.status_nme,
                      hh.activation_cde as dts_activation_cde,
                      hh.expiry_dat as status_expiry_dat,
                      gg.code_chr_stage,
                      ii.stage_nme,
                      gg.expiry_dat as stage_expiry_dat
                 from
                      whse_tantalis.ta_disp_trans_statuses gg,
                      whse_tantalis.ta_status hh,
                      whse_tantalis.ta_stages ii
                 where
                      gg.code_chr_status = hh.code_chr and
                      gg.code_chr_stage = ii.code_chr and
                      gg.expiry_dat is null
                 order by
                      gg.disposition_transaction_sid) ll
                 where
                      jj.disposition_sid = kk.disposition_sid(+) and
                      jj.disposition_transaction_sid = ll.disposition_transaction_sid(+)
                 order by
                      jj.disposition_transaction_sid) nn,
                      whse_tantalis.ta_interest_parcels mm
                  where
                      mm.disposition_transaction_sid(+) = nn.disposition_transaction_sid and
                      
                      nn.dts_activation_cde = 'INACT' and  
                      nn.stage_nme != 'APPLICATION' and
                      mm.intrid_sid IN ({prcl})

                    """
                    
    return sql
    
