WITH period_structure AS(      
SELECT
    period_structure_reduced.periodid,
    period_structure_reduced.enddate,
    lower(periodtype.name) AS frequency
    
FROM     
    (SELECT periodid,
            enddate           
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
    
    ) AS period_structure_reduced 

JOIN 
    (SELECT periodid,periodtypeid 
    FROM period) AS periodtypejoin 
ON  periodtypejoin.periodid =  period_structure_reduced.periodid
JOIN periodtype 
ON periodtype.periodtypeid = periodtypejoin.periodtypeid
),
dataset_filtered AS(
        SELECT datasetid
        FROM dataset
        WHERE {{dataset_ids_conditions}}
),
dataset_set_filtered AS(
        SELECT 
               dataelement.dataelementid,
               dataset_filtered.datasetid
        FROM dataelement
        JOIN datasetelement ON datasetelement.dataelementid = dataelement.dataelementid
        JOIN dataset_filtered ON dataset_filtered.datasetid = datasetelement.datasetid     
   )
   
{% if organisation_uids_to_path_filter %}     
, 
organisation_info_filtered AS (
            SELECT 
                organisationunitid
            FROM organisationunit
            WHERE {{organisation_uids_to_path_filter}}    
            )
{% endif %} 

SELECT 

       AVG(DATE(datavalue.created) - period_structure.enddate) AS timeliness,
       {% if averaged != True and averaged != 'over_de' %}
           dataset_set_filtered.dataelementid,
       {% endif %}
       {% if averaged != True and averaged != 'over_period' %}
           period_structure.enddate,
           period_structure.frequency,
           period_structure.periodid,
       {% endif %}
           dataset_set_filtered.datasetid,
           {{level_to_group}}  
       
FROM datavalue
JOIN dataset_set_filtered
  ON dataset_set_filtered.dataelementid = datavalue.dataelementid
JOIN period_structure
  ON datavalue.periodid = period_structure.periodid
JOIN _orgunitstructure
  ON datavalue.sourceid = _orgunitstructure.organisationunitid
JOIN categoryoptioncombo
  ON datavalue.categoryoptioncomboid = categoryoptioncombo.categoryoptioncomboid
   {% if organisation_uids_to_path_filter %}
        WHERE
    _orgunitstructure.organisationunitid in (SELECT organisationunitid FROM organisation_info_filtered )
   {% endif %}


GROUP BY 
       {% if averaged != True and averaged != 'over_de' %}
             dataelement.name,
       {% endif %}
       {% if averaged != True and averaged != 'over_period' %}
         period_structure.enddate,
         period_structure.frequency,
         period_structure.periodid,
       {% endif %}
        dataset_set_filtered.datasetid,
       {{level_to_group}}  



