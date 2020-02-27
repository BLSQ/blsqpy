SELECT 

       AVG(DATE(datavalue.created) - _periodstructure.enddate) AS timeliness,
       {% if averaged == False or averaged == 'in_period' %}
           dataelement.name AS name,
       {% endif %}
       {% if averaged == False or averaged == 'in_de' %}
           _periodstructure.enddate,
       {% endif %}
       {{ou_labeling}}
       
FROM datavalue
JOIN dataelement
  ON dataelement.dataelementid = datavalue.dataelementid
JOIN _periodstructure
  ON datavalue.periodid = _periodstructure.periodid
JOIN _orgunitstructure
  ON datavalue.sourceid = _orgunitstructure.organisationunitid
JOIN public.categoryoptioncombo
  ON datavalue.categoryoptioncomboid = categoryoptioncombo.categoryoptioncomboid
WHERE {{de_ids_conditions}}

   {% if period_start %}
    and _periodstructure.startdate >= '{{period_start}}'
   {% endif %}
   {% if period_end %}
    and _periodstructure.enddate <= '{{period_end}}'
   {% endif %}



GROUP BY 
       {% if averaged == False or averaged == 'in_period' %}
             dataelement.name,
       {% endif %}
       {% if averaged == False or averaged == 'in_de' %}
         _periodstructure.enddate,
       {% endif %}
       {{ou_structure}}