WITH dataset_info AS(
        SELECT 
            dataset.name AS dataset_name,
            dataset.uid AS dataset_uid,
            dataset.periodtypeid,
            datasetsource.sourceid,
            dataelement.dataelementid,
            dataelement.uid AS dataelement_uid,
            dataelement.name AS datalement_name,
            categoryoptioncombo.categoryoptioncomboid,
            categoryoptioncombo.name AS categoryoptioncombo_name
                
    FROM dataset
    JOIN datasetsource ON datasetsource.datasetid = dataset.datasetid
    JOIN datasetelement ON datasetelement.datasetid = dataset.datasetid
    JOIN dataelement ON dataelement.dataelementid = datasetelement.dataelementid
    JOIN categorycombos_optioncombos ON categorycombos_optioncombos.categorycomboid = dataelement.categorycomboid
    JOIN categoryoptioncombo ON categoryoptioncombo.categoryoptioncomboid = categorycombos_optioncombos.categoryoptioncomboid
    
    {% if dataset_uid_conditions %}    
        WHERE {{dataset_uid_conditions}}
    {% endif %}        
    
    ),

WITH period_info_filtered AS (
        SELECT 
            period.periodid,
            period.periodtypeid,
            lower(periodtype.name) AS frequency
        FROM _periodstructure
        JOIN periodtype ON periodtype.periodtypeid = period.periodtypeid
        WHERE period.periodtypeid in (SELECT periodtypeid FROM dataset_info)
    
        {% if period_start %}
            and period.startdate >= '{{period_start}}'
        {% endif %}

        {% if period_end %}
            and period.enddate <= '{{period_end}}'
        {% endif %}
        ),
        
{% if organisation_uids_to_path_filter %}      
    WITH organisation_info_filtered AS (
            SELECT 
                organisationunitid
            FROM organisationunit
            WHERE {{organisation_uids_to_path_filter}}    
            ),
{% endif %}        

WITH dataset_structure AS(
        SELECT
            dataset_info.dataset_name,
            dataset_info.dataset_uid,
            dataset_info.sourceid,
            dataset_info.dataelementid,
            dataset_info.datalement_name,
            dataset_info.dataelement_uid,
            dataset_info.categoryoptioncomboid,
            dataset_info.categoryoptioncombo_name,
            period_info_filtered.perioid,
            period_info_filtered.frequency
FROM dataset_info
JOIN period_info_filtered
ON dataset_info.periodtypeid = period_info_filtered.periodtypeid  

{% if organisation_uids_to_path_filter %} 
    WHERE dataset_info.sourceid in (SELECT organisationunitid FROM organisation_info_filtered )
{% endif %}   
    ),
    
WITH data_value_info AS(
        SELECT
            datavalue.dataelementid,
            datavalue.periodid,
            datavalue.sourceid,
            datavalue.categoryoptioncomboid,
            datavalue.value
        
    FROM datavalue
        ),
        
WITH dataset_values AS(
        SELECT
            dataset_structure.dataset_name,
            dataset_structure.dataset_uid,
            dataset_structure.sourceid,
            dataset_structure.dataelement_uid,
            dataset_structure.datalement_name,
            dataset_structure.categoryoptioncombo_name,
            dataset_structure.perioid,
            dataset_structure.frequency       
            data_value_info.value

        FROM dataset_structure
        LEFT JOIN data_value_info ON  (
                    dataset_structure.dataelementid = data_value_info.dataelementid 
                AND dataset_structure.periodid = data_value_info.periodid 
                AND dataset_structure.sourceid = data_value_info.sourceid
                AND dataset_structure.categoryoptioncomboid = data_value_info.categoryoptioncomboid
                ), 

WITH period_tree AS(

        SELECT 
            periodid,weekly,mothly,quarterly,yearly
        FROM _periodstructure
        ),
        
WITH period_structure AS (
         SELECT
             period_tree.periodid,
             period_tree.weekly,
             period_tree.mothly,
             period_tree.quarterly,
             period_tree.yearly
             
        FROM period_info_filtered
        JOIN period_tree ON period_info_filtered.periodid = period_tree.periodid
        ),
        
WITH datasets_values_periods  (
    SELECT
            dataset_values.dataset_name,
            dataset_values.dataset_uid,
            dataset_values.sourceid,
            dataset_values.dataelement_uid,
            dataset_values.datalement_name,
            dataset_values.categoryoptioncombo_name,
            dataset_values.perioid,
            dataset_values.frequency       
            dataset_values.value
            period_structure.weekly,
            period_structure.mothly,
            period_structure.quarterly,
            period_structure.yearly
        
    FROM dataset_values
    JOIN period_structure ON dataset_values.periodid = period_structure.id
    
),
WITH pruned_orgunitstructured AS(
    SELECT
          organisationunitid,
          level,
          namelevel1,
          namelevel2,
          namelevel3,
          namelevel4,
          namelevel5,
          organisationunituid
          
    FROM _orgunitstructure

),
SELECT      

    dataset_values.dataset_name,
    dataset_values.dataset_uid,
    dataset_values.sourceid,
    dataset_values.dataelement_uid,
    dataset_values.datalement_name,
    dataset_values.categoryoptioncombo_name,
    dataset_values.perioid,
    dataset_values.frequency       
    dataset_values.value
    period_structure.weekly,
    period_structure.mothly,
    period_structure.quarterly,
    period_structure.yearly,
    pruned_orgunitstructured.level,
    pruned_orgunitstructured.namelevel1,
    pruned_orgunitstructured.namelevel2,
    pruned_orgunitstructured.namelevel3,
    pruned_orgunitstructured.namelevel4,
    pruned_orgunitstructured.namelevel5,
    pruned_orgunitstructured.organisationunituid

FROM
JOIN pruned_orgunitstructured ON datasets_values_periods.organisationunitid = pruned_orgunitstructured.organisationunitid