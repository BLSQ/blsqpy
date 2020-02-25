SELECT 

   {% if averaged %}
       AVG(DATE(datavalue.created) - _periodstructure.enddate) AS timeliness ,
   {% else %}
       DATE(datavalue.created) - _periodstructure.enddate AS timeliness ,
       dataelement.name AS name,
   {% endif %}

   {{ou_labeling}}
       
FROM datavalue
JOIN dataelement
  ON DE.dataelementid = datavalue.dataelementid
JOIN _periodstructure
  ON datavalue.periodid = _periodstructure.periodid
JOIN public._orgunitstructure
  ON datavalue.sourceid = _orgunitstructure.organisationunitid
JOIN public.categoryoptioncombo
  ON datavalue.categoryoptioncomboid = categoryoptioncombo.categoryoptioncomboid
WHERE  dataelement.uid in (
       select dataelement.uid from dataelement
       join datasetelement ON datasetelement.dataelementid = dataelement.dataelementid
       join dataset ON dataset.datasetid = datasetelement.datasetid
       where {{dataset_ids_conditions}})

   {% if period_start %}
    and _periodstructure.startdate >= '{{period_start}}'
   {% endif %}
   {% if period_end %}
    and _periodstructure.enddate <= '{{period_end}}'
   {% endif %}


GROUP BY 
         {% if averaged is False %}
             dataelement.name,
         {% endif %}
         {% if averaged =='full' %}
             _periodstructure.enddate,
             {{ou_structure}}
         {% else %}
             dataset.name,
             _periodstructure.enddate,
             {{ou_structure}}
        {% endif %}
