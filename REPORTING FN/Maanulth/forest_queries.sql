-- FOREST AUTHORIZATIONS
	-- NEW_AMEND column indicates whether the athorization is NEW or AMENDEMENT 
	-- REGION column indicates whether the athorization is located in maanulth NORTH or SOUTH areas
SELECT 
  frr.FOREST_FILE_ID,
  frr.CUTTING_PERMIT_ID,
  frr.MAP_LABEL,
  frr.FILE_TYPE_DESCRIPTION,
  frr.FILE_STATUS_CODE,
  frr.LIFE_CYCLE_STATUS_CODE,
  frr.HARVEST_AUTH_STATUS_CODE,
  frr.ISSUE_DATE,
  amdd.AMEND_STATUS_DATE,
  
  CASE 
    WHEN amdd.AMEND_STATUS_DATE > frr.ISSUE_DATE+ 5 
      THEN 'AMENDEMENT' 
        ELSE 'NEW' 
          END AS NEW_AMEND,
          
  frr.EXPIRY_DATE,
  EXTRACT(YEAR FROM frr.EXPIRY_DATE) - EXTRACT(YEAR FROM frr.ISSUE_DATE) AS TENURE_LENGTH_YRS,
  ROUND(SDO_GEOM.SDO_AREA(frr.GEOMETRY, 0.005, 'unit=HECTARE'), 2) AREA_HA,
  
   CASE 
    WHEN frr.ADMIN_DISTRICT_CODE = 'DSI' 
      THEN 'SOUTH' 
        ELSE 'NORTH' 
          END AS REGION,
          
   frr.GEOMETRY

FROM WHSE_FOREST_TENURE.FTEN_HARVEST_AUTH_POLY_SVW frr

  JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
    ON SDO_RELATE (frr.GEOMETRY, pip.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
      AND pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
      
  LEFT JOIN (
            WITH CTE AS (
                SELECT
                    amd.FOREST_FILE_ID || ' ' || amd.CUTTING_PERMIT_ID AS MAP_LABEL,
                    amd.AMEND_STATUS_DATE,
                    ROW_NUMBER() OVER (PARTITION BY amd.FOREST_FILE_ID, amd.CUTTING_PERMIT_ID ORDER BY amd.AMEND_STATUS_DATE) AS rn
                FROM WHSE_FOREST_TENURE.FTEN_HARVEST_AMEND amd
                WHERE amd.AMEND_STATUS_DATE BETWEEN TO_DATE('01/09/2022', 'DD/MM/YYYY') AND TO_DATE('31/08/2023', 'DD/MM/YYYY')
            )
            SELECT MAP_LABEL, AMEND_STATUS_DATE
            FROM CTE
            WHERE rn = 1
  ) amdd
    ON amdd.MAP_LABEL = frr.MAP_LABEL
        
WHERE frr.LIFE_CYCLE_STATUS_CODE <> 'PENDING'
  AND (amdd.AMEND_STATUS_DATE BETWEEN TO_DATE('01/09/2022', 'DD/MM/YYYY') AND TO_DATE('31/08/2023', 'DD/MM/YYYY') 
        OR 
      (frr.ISSUE_DATE BETWEEN TO_DATE('01/09/2022', 'DD/MM/YYYY') AND TO_DATE('31/08/2023', 'DD/MM/YYYY')AND amdd.AMEND_STATUS_DATE is NULL)) 
        
ORDER BY frr.MAP_LABEL;




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





-- FOREST REACREATION POLY
SELECT
  rcp.MAP_LABEL,
  ROUND(SDO_GEOM.SDO_AREA(rcp.GEOMETRY, 0.005, 'unit=HECTARE'), 2) AREA_HA,
  rcp.FILE_STATUS_CODE,
  rcp.PROJECT_TYPE,
  rcp.PROJECT_ESTABLISHED_DATE,
  rcp.LIFE_CYCLE_STATUS_CODE,
  rcp.GEOGRAPHIC_DISTRICT_NAME
  
FROM WHSE_FOREST_TENURE.FTEN_RECREATION_POLY_SVW rcp
  JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
    ON SDO_RELATE (rcp.GEOMETRY, pip.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
      AND pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'

WHERE rcp.PROJECT_ESTABLISHED_DATE BETWEEN TO_DATE('01/09/2022', 'DD/MM/YYYY') AND TO_DATE('31/08/2023', 'DD/MM/YYYY');




-- FOREST REACREATION LINE
SELECT
  rcl.MAP_LABEL,
  rcl.FEATURE_LENGTH AS LENGTH_KM,
  rcl.FILE_STATUS_CODE,
  rcl.PROJECT_TYPE,
  rcl.PROJECT_ESTABLISHED_DATE,
  rcl.LIFE_CYCLE_STATUS_CODE

FROM WHSE_FOREST_TENURE.FTEN_RECREATION_LINES_SVW rcl
  JOIN WHSE_ADMIN_BOUNDARIES.PIP_CONSULTATION_AREAS_SP pip
    ON SDO_RELATE (rcl.GEOMETRY, pip.SHAPE, 'mask=ANYINTERACT') = 'TRUE'
      AND pip.CONTACT_ORGANIZATION_NAME = q'[Maa-nulth First Nations]'
      
WHERE rcl.PROJECT_ESTABLISHED_DATE BETWEEN TO_DATE('01/09/2022', 'DD/MM/YYYY') AND TO_DATE('31/08/2023', 'DD/MM/YYYY');