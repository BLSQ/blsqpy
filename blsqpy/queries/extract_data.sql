SELECT datavalue.value,
organisationunit.path,
period.startdate as start_date, period.enddate as end_date, lower(periodtype.name) as frequency,
dataelement.uid AS dataelementid, dataelement.name AS dataelementname,
categoryoptioncombo.uid AS CatComboID , categoryoptioncombo.name AS CatComboName,
dataelement.created,
organisationunit.uid as uidorgunit
FROM datavalue
JOIN dataelement ON dataelement.dataelementid = datavalue.dataelementid
JOIN categoryoptioncombo ON categoryoptioncombo.categoryoptioncomboid = datavalue.categoryoptioncomboid
JOIN organisationunit ON organisationunit.organisationunitid = datavalue.sourceid
JOIN period ON period.periodid = datavalue.periodid
JOIN periodtype ON periodtype.periodtypeid = period.periodtypeid
WHERE
    organisationunit.path like '{{org_unit_path_starts_with}}%'
   {% if period_start %}
    and period.startdate >= '{{period_start}}'
   {% endif %}
   {% if period_end %}
    and period.enddate <= '{{period_end}}'
   {% endif %}
    and {{de_ids_conditions}}