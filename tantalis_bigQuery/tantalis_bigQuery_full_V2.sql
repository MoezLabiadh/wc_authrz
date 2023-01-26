SELECT*

FROM whse_tantalis.ta_disposition_transactions dt 
  JOIN whse_tantalis.ta_interest_parcels ip 
    ON dt.DISPOSITION_TRANSACTION_SID = ip.DISPOSITION_TRANSACTION_SID
  JOIN whse_tantalis.ta_disp_trans_statuses ts
    ON dt.DISPOSITION_TRANSACTION_SID = ts.DISPOSITION_TRANSACTION_SID AND ts.EXPIRY_DAT is NULL
  JOIN whse_tantalis.ta_dispositions ds
    ON ds.DISPOSITION_SID = dt.DISPOSITION_SID
  JOIN whse_tantalis.ta_stages sg 
    ON sg.CODE_CHR = ts.CODE_CHR_STAGE
  JOIN whse_tantalis.ta_status tt 
    ON tt.CODE_CHR = ts.CODE_CHR_STATUS
  JOIN whse_tantalis.ta_available_types ty 
    ON ty.TYPE_SID = dt.TYPE_SID    
  JOIN whse_tantalis.ta_available_subtypes sty 
    ON sty.SUBTYPE_SID = dt.SUBTYPE_SID AND sty.TYPE_SID = dt.TYPE_SID 
  JOIN whse_tantalis.ta_available_purposes pu 
    ON pu.PURPOSE_SID = dt.PURPOSE_SID    
  JOIN whse_tantalis.ta_available_subpurposes spu 
    ON spu.SUBPURPOSE_SID = dt.SUBPURPOSE_SID AND spu.PURPOSE_SID = dt.PURPOSE_SID 
   JOIN whse_tantalis.ta_organization_units ou 
    ON ou.ORG_UNIT_SID = dt.ORG_UNIT_SID 
