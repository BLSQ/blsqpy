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
WHERE {{de_ids_conditions}}