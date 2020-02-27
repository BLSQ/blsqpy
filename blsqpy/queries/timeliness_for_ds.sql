WITH period_structure AS(      
SELECT
    period_structure_reduced.periodid,
    period_structure_reduced.enddate,
    period_structure_reduced.startdate,
    lower(periodtype.name) AS frequency,
    CASE WHEN lower(periodtype.name) = 'daily' THEN period_structure_reduced.daily
    WHEN lower(periodtype.name) = 'weekly' THEN period_structure_reduced.weekly
    WHEN lower(periodtype.name) = 'biweekly' THEN period_structure_reduced.biweekly
    WHEN lower(periodtype.name) = 'monthly' THEN period_structure_reduced.monthly
    WHEN lower(periodtype.name) = 'bimonthly' THEN period_structure_reduced.bimonthly
    WHEN lower(periodtype.name) = 'quarterly' THEN period_structure_reduced.quarterly
    WHEN lower(periodtype.name) = 'sixmonthly' THEN period_structure_reduced.sixmonthly
    WHEN lower(periodtype.name) = 'yearly' THEN period_structure_reduced.yearly
    ELSE 'Others'
    END AS period_name
    
FROM     
    (SELECT periodid,
            enddate,
            startdate,
            daily,weekly,biweekly,monthly,bimonthly,quarterly,
            sixmonthly,yearly            
    FROM _periodstructure
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
),

dataset_filtered AS(
        SELECT 
               dataelement.uid AS dataelement_uid,
               dataset.name AS dataset_name
        FROM dataelement
        JOIN datasetelement ON datasetelement.dataelementid = dataelement.dataelementid
        JOIN dataset ON dataset.datasetid = datasetelement.datasetid
        WHERE {{dataset_ids_conditions}}      
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
           dataset_filtered.dataset_name,
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
JOIN dataset_filtered ON dataset_filtered.dataelement_uid = dataelement.uid

   {% if period_start or period_end %}
        WHERE
   {% endif %}
   {% if period_start %}
        period_structure.startdate >= '{{period_start}}'
   {% endif %}   
   {% if period_start and period_end %}
        AND
   {% endif %}
   {% if period_end %}
        period_structure.enddate <= '{{period_end}}'
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
        dataset_filtered.dataset_name,
       {{ou_structure}}



