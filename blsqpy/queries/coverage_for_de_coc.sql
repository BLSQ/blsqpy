SELECT
    period.startdate as start_date,
    period.enddate as end_date,
    lower(periodtype.name) as frequency ,
    dataelement.uid as deid, categoryoptioncombo.uid as coc_id ,
    {{orgunitstructure_table}}.uidlevel{{aggregation_level}},
    count(datavalue) values_count
FROM datavalue
JOIN organisationunit ON organisationunit.organisationunitid = datavalue.sourceid
JOIN {{orgunitstructure_table}} ON {{orgunitstructure_table}}.organisationunituid = organisationunit.uid
JOIN dataelement ON dataelement.dataelementid = datavalue.dataelementid
JOIN categoryoptioncombo ON categoryoptioncombo.categoryoptioncomboid = datavalue.categoryoptioncomboid
JOIN period ON period.periodid = datavalue.periodid
JOIN periodtype ON periodtype.periodtypeid = period.periodtypeid
WHERE
organisationunit.path like '{{org_unit_path_starts_with}}%' and
datavalue.dataelementid in ({{data_element_selector}})
{% if period_start %}
and period.startdate >= '{{period_start}}'
{% endif %}
{% if period_end %}
and period.enddate <= '{{period_end}}'
{% endif %}
GROUP BY
    {{orgunitstructure_table}}.uidlevel{{aggregation_level}},
    period.startdate,
    period.enddate,
    periodtype.name,
    dataelement.uid,
    categoryoptioncombo.uid;