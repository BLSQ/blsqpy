WITH dataelement_reduced AS (
    SELECT
            dataelement.name,
            dataelement.uid,
            dataelement.name,
    FROM dataelement
    WHERE {{dataelement_conditions}}

),period_structure AS(      
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

dataelement_info AS(
        SELECT 
            dataelement.dataelementid,
            dataelement.uid AS dataelement_uid,
            dataelement.name AS datalement_name,
            categoryoptioncombo.categoryoptioncomboid,
            categoryoptioncombo.name AS categoryoptioncombo_name
                
    FROM dataelement_reduced
    JOIN categorycombos_optioncombos ON categorycombos_optioncombos.categorycomboid = dataelement_reduced.categorycomboid
    JOIN categoryoptioncombo ON categoryoptioncombo.categoryoptioncomboid = categorycombos_optioncombos.categoryoptioncomboid
    ),

period_info_filtered AS (
        SELECT 
            period_structure.periodid,
            period_structure.iso,
            period.periodtypeid,
            periodtype.name AS frequency
        FROM period_structure
        JOIN period ON period_structure.periodid = period.periodid
        JOIN periodtype ON periodtype.periodtypeid = period.periodtypeid
        
{% if periodicity_conditions %}  
        WHERE period.periodtypeid in {{periodicity_conditions}}        
{% endif %}

        ),
        
  
organisation_info_filtered AS (
            SELECT 
                organisationunitid
            FROM organisationunit
{% if organisation_uids_to_path_filter %}    
            WHERE {{organisation_uids_to_path_filter}}   
{% endif %}
            ),

dataelement_structure AS(
        SELECT
            dataelement_info.dataelementid,
            dataelement_info.datalement_name,
            dataelement_info.dataelement_uid,
            dataelement_info.categoryoptioncomboid,
            dataelement_info.categoryoptioncombo_name,
            period_info_filtered.periodid,
            period_info_filtered.frequency,
            period_info_filtered.iso
FROM dataelement_info
CROSS JOIN period_info_filtered
CROSS JOIN organisation_info_filtered

    )
    
        SELECT
            dataelement_structure.dataset_uid,
--            dataelement_structure.sourceid,
            dataelement_structure.dataelement_uid,
            dataelement_structure.categoryoptioncombo_name,
            dataelement_structure.periodid,
            dataelement_structure.iso,
            COUNT (*) AS values_expected,
            COUNT(datavalue.value) AS values_reported,
            {{ou_labeling}}      
            
        FROM dataset_structure
        LEFT JOIN datavalue ON  (
                    dataset_structure.dataelementid = datavalue.dataelementid 
                AND dataset_structure.periodid = datavalue.periodid 
                AND dataset_structure.sourceid = datavalue.sourceid
                AND dataset_structure.categoryoptioncomboid = datavalue.categoryoptioncomboid
                )
        JOIN _orgunitstructure ON dataset_structure.sourceid = _orgunitstructure.organisationunitid
        {% if oug_filtered_levels %}  
        WHERE _orgunitstructure.level in {{oug_filtered_levels}}        
        {% endif %}        
        
        GROUP BY
            dataset_structure.dataset_uid,
--            dataset_structure.sourceid,
            dataset_structure.dataelement_uid,
            dataset_structure.categoryoptioncombo_name,
            dataset_structure.periodid,
            dataset_structure.iso,
            {{ou_structure}}