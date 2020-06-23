WITH period_structure AS(      
    SELECT periodid,
            iso AS period,
            enddate
--            startdate
    FROM _periodstructure         
    {% if period_start or period_end %}
        WHERE
   {% endif %}
   {% if period_start %}
        {{period_start}}
   {% endif %}
   {% if period_start and period_end %}
        AND
   {% endif %}
   {% if period_end %}
        {{period_end}}
   {% endif %}
),

dataelement_filtered AS(
        SELECT dataelementid
        FROM dataelement
        WHERE {{de_ids_conditions}}
        
),
datavalue_restricted AS(
        SELECT value,
               dataelementid,
               periodid,
               sourceid,
               categoryoptioncomboid
        FROM datavalue
)

SELECT 
--        CAST(datavalue_restricted.value AS INT) AS value,
       datavalue_restricted.value,
       period_structure.period,
       period_structure.enddate AS period_end,
       dataelement_filtered.dataelementid,
       _orgunitstructure.organisationunitid,
       datavalue_restricted.categoryoptioncomboid

FROM datavalue_restricted 
JOIN dataelement_filtered
  ON dataelement_filtered.dataelementid = datavalue_restricted.dataelementid
JOIN period_structure
  ON datavalue_restricted.periodid = period_structure.periodid
JOIN _orgunitstructure
  ON datavalue_restricted.sourceid = _orgunitstructure.organisationunitid
JOIN public.categoryoptioncombo
  ON datavalue_restricted.categoryoptioncomboid = categoryoptioncombo.categoryoptioncomboid
    
    
    
