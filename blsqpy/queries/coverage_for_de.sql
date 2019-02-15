
with orgunit_values as (
  SELECT
    organisationunit.uid as organisationunit_uid,
    dataelement.uid as dataelement_uid,
    categoryoptioncombo.uid as categoryoptioncombo_uid,
    datavalue.value as datavalue_value,
    period.periodid as period_id
  FROM datavalue
  JOIN organisationunit ON organisationunit.organisationunitid = datavalue.sourceid
  JOIN period ON period.periodid = datavalue.periodid
  JOIN dataelement ON dataelement.dataelementid = datavalue.dataelementid
  JOIN categoryoptioncombo ON categoryoptioncombo.categoryoptioncomboid = datavalue.categoryoptioncomboid
  WHERE
   dataelement.uid= '{{ data_element_uid }}'
), orgunit_has_values as (
  SELECT
    organisationunit_uid,
    dataelement_uid,
    period_id
  FROM orgunit_values
  GROUP BY
    organisationunit_uid,
    dataelement_uid,
    period_id
), de_coverage as (
  SELECT
    uidlevel{{aggregation_level}},
    dataelement_uid,
    period.startdate as period_start,
    period.enddate as period_end,
    lower(periodtype.name)
    orgunit_has_values.period_id,
    count(*) as orgunit_count
  FROM orgunit_has_values
  JOIN {{orgunitstructure_table}} on {{orgunitstructure_table}}.organisationunituid = orgunit_has_values.organisationunit_uid
  JOIN period ON period.periodid = orgunit_has_values.period_id
  JOIN periodtype ON periodtype.periodtypeid = period.periodtypeid
  GROUP BY
    uidlevel{{aggregation_level}},
    dataelement_uid,
    period_start,
    period_end,
    periodtype.name,
    orgunit_has_values.period_id
)
SELECT *
FROM de_coverage