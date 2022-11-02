WITH dataset_reduced AS (
    SELECT
            dataset.periodtypeid,
            dataset.datasetid
    FROM dataset
    WHERE {{dataset_uid_conditions}} 

),
period_structure AS(      
    SELECT 
            periodid
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
    SELECT  orgunitgroupid,
            organisationunitid
    FROM orgunitgroup
    JOIN orgunitgroupmembers 
    ON orgunitgroup.orgunitgroupid = orgunitgroupmembers.orgunitgroupid 
    WHERE {{oug_uid_conditions}}
),
{% endif %}

{% if deg_uid_conditions %} 
de_group AS(      
    SELECT  dataelementgroupid,
            dataelementid
    FROM dataelementgroup
    JOIN dataelementgroupmembers 
    ON dataelementgroup.dataelementgroupid = dataelementgroupmembers.dataelementgroupid 
    WHERE {{deg_uid_conditions}}
),
{% endif %}


dataset_info AS(
        SELECT 
            dataset_reduced.datasetid,
            dataset_reduced.periodtypeid,
            datasetsource.sourceid,
            dataelement.dataelementid,
            categoryoptioncombo.categoryoptioncomboid
                
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
            period.periodtypeid
            
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
            dataset_info.datasetid,
            dataset_info.sourceid,
            
            dataset_info.dataelementid,
            dataset_info.categoryoptioncomboid,

            {% if deg_uid_conditions %} 
                de_group.dataelementgroupid,
            {% endif %}
            
            {% if hashed_deg_uid_conditions %} 
                {{hashed_deg_uid_conditions}}.dataelementgroupid,
            {% endif %}            
            
            
            {% if oug_uid_conditions %} 
                org_unit_group.orgunitgroupid,
            {% endif %}
            period_info_filtered.periodid
            
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

{% if hashed_deg_uid_conditions %} 
    JOIN {{hashed_deg_uid_conditions}} 
    ON dataset_info.dataelementid = {{hashed_deg_uid_conditions}}.dataelementid
{% endif %}

    )
    
        SELECT
            dataset_structure.datasetid,
            dataset_structure.periodid,

            {% if deg_uid_conditions %} 
                dataset_structure.dataelementgroupid,
            {% endif %}
            {% if hashed_deg_uid_conditions %} 
                dataset_structure.dataelementgroupid,
            {% endif %}    
            {% if oug_uid_conditions %} 
                dataset_structure.orgunitgroupid,
            {% endif %}
            
            COUNT (*) AS values_expected,
            COUNT(datavalue.value) AS values_reported,
            {{level_to_group}}  

       
        FROM dataset_structure
        LEFT JOIN datavalue ON  (
                    dataset_structure.dataelementid = datavalue.dataelementid 
                AND dataset_structure.periodid = datavalue.periodid 
                AND dataset_structure.sourceid = datavalue.sourceid
                AND dataset_structure.categoryoptioncomboid = datavalue.categoryoptioncomboid
                )
        JOIN _orgunitstructure ON dataset_structure.sourceid = _orgunitstructure.organisationunitid
        
        
        GROUP BY
                dataset_structure.datasetid,
                dataset_structure.periodid,
            {% if deg_uid_conditions %} 
                dataset_structure.dataelementgroupid,
            {% endif %}
            {% if hashed_deg_uid_conditions %} 
                dataset_structure.dataelementgroupid,
            {% endif %}
            {% if oug_uid_conditions %} 
                dataset_structure.orgunitgroupid,
            {% endif %}
            {{level_to_group}}