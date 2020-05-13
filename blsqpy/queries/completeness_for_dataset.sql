WITH dataset_reduced AS (
    SELECT
--            dataset.name,
            dataset.uid,
            dataset.periodtypeid,
            dataset.datasetid
    FROM dataset
    WHERE {{dataset_uid_conditions}} 

),
period_structure AS(      
    SELECT 
            periodid,
            iso
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
),




{% if oug_uid_conditions %} 
org_unit_group AS(      
    SELECT  orgunitgroup.uid AS oug_uid,
            organisationunitid
    FROM orgunitgroup
    JOIN orgunitgroupmembers 
    ON orgunitgroup.orgunitgroupid = orgunitgroupmembers.orgunitgroupid 
    WHERE {{oug_uid_conditions}}
),
{% endif %}

{% if deg_uid_conditions %} 
de_group AS(      
    SELECT  dataelementgroup.uid AS deg_uid,
            dataelementid
    FROM dataelementgroup
    JOIN dataelementgroupmembers 
    ON dataelementgroup.dataelementgroupid = dataelementgroupmembers.dataelementgroupid 
    WHERE {{deg_uid_conditions}}
),
{% endif %}

dataset_info AS(
        SELECT 
--            dataset_reduced.name AS dataset_name,
            dataset_reduced.uid AS dataset_uid,
            dataset_reduced.periodtypeid,
            datasetsource.sourceid,
            dataelement.dataelementid,
            dataelement.uid AS dataelement_uid,
            dataelement.name AS datalement_name,
            categoryoptioncombo.categoryoptioncomboid,
            categoryoptioncombo.uid AS categoryoptioncombo_uid
                
    FROM dataset_reduced
    JOIN datasetsource ON datasetsource.datasetid = dataset_reduced.datasetid
    JOIN datasetelement ON datasetelement.datasetid = dataset_reduced.datasetid
    JOIN dataelement ON dataelement.dataelementid = datasetelement.dataelementid
    JOIN categorycombos_optioncombos ON categorycombos_optioncombos.categorycomboid = dataelement.categorycomboid
    JOIN categoryoptioncombo ON categoryoptioncombo.categoryoptioncomboid = categorycombos_optioncombos.categoryoptioncomboid      
    ),

period_info_filtered AS (
        SELECT 
            period_structure.periodid,
            period_structure.iso,
            period.periodtypeid
--            periodtype.name AS frequency
        FROM period_structure
        JOIN period ON period_structure.periodid = period.periodid
        JOIN periodtype ON periodtype.periodtypeid = period.periodtypeid
        WHERE period.periodtypeid in (SELECT periodtypeid FROM dataset_reduced)
        ),
        
{% if organisation_uids_to_path_filter %}      
organisation_info_filtered AS (
            SELECT 
                organisationunitid
            FROM organisationunit
            WHERE {{organisation_uids_to_path_filter}}    
            ),
{% endif %} 

dataset_structure AS(
        SELECT
--            dataset_info.dataset_name,
            dataset_info.dataset_uid,
            dataset_info.sourceid,
            dataset_info.dataelementid,
--            dataset_info.datalement_name,
            dataset_info.dataelement_uid,
            dataset_info.categoryoptioncomboid,
            dataset_info.categoryoptioncombo_uid,
            period_info_filtered.periodid,
            {% if deg_uid_conditions %} 
                de_group.deg_uid,
            {% endif %}
            {% if oug_uid_conditions %} 
                org_unit_group.oug_uid,
            {% endif %}
--            period_info_filtered.frequency,
            period_info_filtered.iso
FROM dataset_info
JOIN period_info_filtered
ON dataset_info.periodtypeid = period_info_filtered.periodtypeid

{% if deg_uid_conditions %} 
    JOIN de_group ON dataset_info.dataelementid = de_group.dataelementid
{% endif %}

{% if oug_uid_conditions %} 
    JOIN org_unit_group ON dataset_info.sourceid = org_unit_group.organisationunitid
{% endif %}

{% if organisation_uids_to_path_filter %} 
    WHERE dataset_info.sourceid in (SELECT organisationunitid FROM organisation_info_filtered )
{% endif %}

    )
    
        SELECT
            dataset_structure.dataset_uid,
--            dataset_structure.sourceid,
            dataset_structure.dataelement_uid,
            {% if coc_disagg %}
            dataset_structure.categoryoptioncombo_uid,
            {% endif %}
--            dataset_structure.periodid,

            {% if deg_uid_conditions %} 
                dataset_structure.deg_uid,
            {% endif %}
            {% if oug_uid_conditions %} 
                dataset_structure.oug_uid,
            {% endif %}
            
            dataset_structure.iso,
            COUNT (*) AS values_expected,
            COUNT(datavalue.value) AS values_reported,
            _orgunitstructure.organisationunituid   
            
        FROM dataset_structure
        LEFT JOIN datavalue ON  (
                    dataset_structure.dataelementid = datavalue.dataelementid 
                AND dataset_structure.periodid = datavalue.periodid 
                AND dataset_structure.sourceid = datavalue.sourceid
                AND dataset_structure.categoryoptioncomboid = datavalue.categoryoptioncomboid
                )
        JOIN _orgunitstructure ON dataset_structure.sourceid = _orgunitstructure.organisationunitid
        
        
        GROUP BY
            dataset_structure.dataset_uid,
--            dataset_structure.sourceid,
            dataset_structure.dataelement_uid,
            {% if coc_disagg %}
            dataset_structure.categoryoptioncombo_uid,
            {% endif %}
--            dataset_structure.periodid,

            {% if deg_uid_conditions %} 
                dataset_structure.deg_uid,
            {% endif %}
            {% if oug_uid_conditions %} 
                dataset_structure.oug_uid,
            {% endif %}


            dataset_structure.iso,
            _orgunitstructure.organisationunituid