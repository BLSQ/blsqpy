
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
   organisationunit.path like '{{org_unit_path_starts_with}}%'
   {% if period_start %}
    and period.startdate >= '{{period_start}}'
   {% endif %}
   {% if period_end %}
    and period.enddate <= '{{period_end}}'
   {% endif %}
   and dataelement.uid in (
     select dataelement.uid from dataelement
     join datasetelement ON datasetelement.dataelementid = dataelement.dataelementid
     join dataset ON dataset.datasetid = datasetelement.datasetid
     where dataset.uid = '{{dataset_uid}}')

), orgunit_has_values as (
  select
    organisationunit_uid,
    period_id
  from orgunit_values
  group by
    organisationunit_uid,
    period_id
), ds_coverage as (
  select
    uidlevel{{aggregation_level}},
    period.startdate as period_start,
    period.enddate as period_end,
    lower(periodtype.name) as frequency,
    count(*) as values_count
  from orgunit_has_values
  join {{orgunitstructure_table}} on {{orgunitstructure_table}}.organisationunituid = orgunit_has_values.organisationunit_uid
  join period ON period.periodid = orgunit_has_values.period_id
  join periodtype ON periodtype.periodtypeid = period.periodtypeid
  group by
    uidlevel{{aggregation_level}},
    period_start,
    period_end,
    periodtype.name,
    orgunit_has_values.period_id
)
select * from ds_coverage