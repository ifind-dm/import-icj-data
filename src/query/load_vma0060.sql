SELECT
    DISTINCT
      DT
FROM `{target_table}` 
WHERE DT between date_add(CURRENT_DATE(), INTERVAL -7 DAY) and CURRENT_DATE()