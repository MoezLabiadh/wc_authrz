-- FOREST AUTHORIZATIONS
SELECT 
  frr.FOREST_FILE_ID,
  frr.CUTTING_PERMIT_ID,
  frr.MAP_LABEL,
  frr.FILE_TYPE_DESCRIPTION,
  frr.FILE_STATUS_CODE,
  frr.LIFE_CYCLE_STATUS_CODE,
  frr.HARVEST_AUTH_STATUS_CODE,
  frr.ISSUE_DATE,
  frr.EXPIRY_DATE,
  (EXTRACT(YEAR FROM frr.EXPIRY_DATE) - EXTRACT(YEAR FROM frr.ISSUE_DATE)) AS TENURE_LENGTH_YRS,
  frr.ADMIN_DISTRICT_NAME,
   ROUND(SDO_GEOM.SDO_AREA(frr.GEOMETRY, 0.005, 'unit=HECTARE'), 2) AREA_HA

FROM WHSE_FOREST_TENURE.FTEN_HARVEST_AUTH_POLY_SVW frr
  JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
    ON SDO_RELATE (frr.GEOMETRY, pip.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
      AND pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'

WHERE frr.ISSUE_DATE BETWEEN TO_DATE('01/09/2022', 'DD/MM/YYYY') AND TO_DATE('31/08/2023', 'DD/MM/YYYY');




-- FOREST ROADS
SELECT 
  ftr.MAP_LABEL,
  ftr.ROAD_SECTION_LENGTH AS ROAD_SECTION_LENGTH_KM,
  ftr.FILE_TYPE_DESCRIPTION,
  ftr.FILE_STATUS_CODE,
  ftr.LIFE_CYCLE_STATUS_CODE,
  ftr.AWARD_DATE,
  ftr.EXPIRY_DATE,
  (EXTRACT(YEAR FROM ftr.EXPIRY_DATE) - EXTRACT(YEAR FROM ftr.AWARD_DATE)) AS TENURE_LENGTH_YRS,
  ftr.GEOGRAPHIC_DISTRICT_NAME
  
FROM WHSE_FOREST_TENURE.FTEN_ROAD_SECTION_LINES_SVW ftr
  JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
    ON SDO_RELATE (ftr.GEOMETRY, pip.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
      AND pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
      
WHERE ftr.AWARD_DATE BETWEEN TO_DATE('01/09/2022', 'DD/MM/YYYY') AND TO_DATE('31/08/2023', 'DD/MM/YYYY');



