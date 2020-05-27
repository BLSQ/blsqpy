WITH period_structure AS(      
    SELECT periodid,
            iso AS period,
            enddate,
            startdate
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
        
)

SELECT CAST(datavalue.value AS INT) AS value,
       period_structure.period,
       period_structure.enddate AS period_end,
       dataelement_filtered.dataelementid,
       _orgunitstructure.organisationunitid

FROM datavalue 
JOIN dataelement_filtered
  ON dataelement_filtered.dataelementid = datavalue.dataelementid
JOIN period_structure
  ON datavalue.periodid = period_structure.periodid
JOIN _orgunitstructure
  ON datavalue.sourceid = _orgunitstructure.organisationunitid
JOIN public.categoryoptioncombo
  ON datavalue.categoryoptioncomboid = categoryoptioncombo.categoryoptioncomboid
    
    
    
