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
            categoryoptioncombo.name AS categoryoptioncombo_name
                
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
--            dataset_info.dataelement_uid,
            dataset_info.categoryoptioncomboid,
--            dataset_info.categoryoptioncombo_name,
            period_info_filtered.periodid,
--            period_info_filtered.frequency,
            period_info_filtered.iso
FROM dataset_info
JOIN period_info_filtered
ON dataset_info.periodtypeid = period_info_filtered.periodtypeid  

{% if organisation_uids_to_path_filter %} 
    WHERE dataset_info.sourceid in (SELECT organisationunitid FROM organisation_info_filtered )
{% endif %}

    )
    
        SELECT
            dataset_structure.dataset_uid,
--            dataset_structure.sourceid,
--            dataset_structure.dataelement_uid,
--            dataset_structure.categoryoptioncombo_name,
--            dataset_structure.periodid,
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
--            dataset_structure.dataelement_uid,
--            dataset_structure.categoryoptioncombo_name,
--            dataset_structure.periodid,
            dataset_structure.iso,
            _orgunitstructure.organisationunituid