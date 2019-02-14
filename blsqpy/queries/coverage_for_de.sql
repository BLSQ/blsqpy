
with orgunit_values as (
  select
    organisationunit.uid as organisationunit_uid,
    dataelement.uid as dataelement_uid,
    categoryoptioncombo.uid as categoryoptioncombo_uid,
    datavalue.value as datavalue_value,
    period.periodid as period_id
  from datavalue
  join organisationunit ON organisationunit.organisationunitid = datavalue.sourceid
  join period ON period.periodid = datavalue.periodid
  join dataelement ON dataelement.dataelementid = datavalue.dataelementid
  join categoryoptioncombo ON categoryoptioncombo.categoryoptioncomboid = datavalue.categoryoptioncomboid
  where
   dataelement.uid= '{{ data_element_uid }}'
), orgunit_has_values as (
  select
    organisationunit_uid,
    dataelement_uid,
    period_id
  from orgunit_values
  group by
    organisationunit_uid,
    dataelement_uid,
    period_id
), de_coverage as (
  select
    uidlevel{{aggregation_level}},
    dataelement_uid,
    period.startdate as period_start,
    period.enddate as period_end,
    orgunit_has_values.period_id,
    count(*) as orgunit_count
  from orgunit_has_values
  join {{orgunitstructure_table}} on {{orgunitstructure_table}}.organisationunituid = orgunit_has_values.organisationunit_uid
  join period ON period.periodid = orgunit_has_values.period_id
  group by
    uidlevel{{aggregation_level}},
    dataelement_uid,
    period_start,
    period_end,
    orgunit_has_values.period_id
)
select * from de_coverage