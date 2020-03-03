WITH period_structure AS(      
SELECT
    period_structure_reduced.periodid,
    period_structure_reduced.enddate,
    period_structure_reduced.startdate,
    lower(periodtype.name) AS frequency,
    period_structure_reduced.iso AS period_name
    
FROM     
    (SELECT periodid,
            iso,
            enddate,
            startdate            
    {% if period_start or period_end %}
        WHERE
   {% endif %}
   {% if period_start %}
        _periodstructure.startdate >= '{{period_start}}'
   {% endif %}
   {% if period_start and period_end %}
        AND
   {% endif %}
   {% if period_end %}
        _periodstructure.enddate <= '{{period_end}}'
   {% endif %}
    
    ) AS period_structure_reduced 
JOIN 
    (SELECT periodid,periodtypeid 
    FROM period) AS periodtypejoin 
ON  periodtypejoin.periodid =  period_structure_reduced.periodid
JOIN periodtype 
ON periodtype.periodtypeid = periodtypejoin.periodtypeid
)

SELECT 

       AVG(DATE(datavalue.created) - period_structure.enddate) AS timeliness,
       {% if averaged != True and averaged != 'over_de' %}
           dataelement.name AS dataelement_name,
       {% endif %}
       {% if averaged != True and averaged != 'over_period' %}
           period_structure.enddate,
           period_structure.frequency,
           period_structure.period_name,
       {% endif %}
       {{ou_labeling}}
       
FROM datavalue
JOIN dataelement
  ON dataelement.dataelementid = datavalue.dataelementid
JOIN period_structure
  ON datavalue.periodid = period_structure.periodid
JOIN _orgunitstructure
  ON datavalue.sourceid = _orgunitstructure.organisationunitid
JOIN categoryoptioncombo
  ON datavalue.categoryoptioncomboid = categoryoptioncombo.categoryoptioncomboid
WHERE {{de_ids_conditions}}

   {% if period_start %}
    and period_structure.startdate >= '{{period_start}}'
   {% endif %}
   {% if period_end %}
    and period_structure.enddate <= '{{period_end}}'
   {% endif %}



GROUP BY 
       {% if averaged != True and averaged != 'over_de' %}
             dataelement.name,
       {% endif %}
       {% if averaged != True and averaged != 'over_period' %}
         period_structure.enddate,
         period_structure.frequency,
         period_structure.period_name,
       {% endif %}
       {{ou_structure}}
       
