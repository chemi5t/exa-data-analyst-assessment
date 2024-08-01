-- Review entire patient table
SELECT *
FROM dim_patient;

-- Part 1: Using the patient data provided identify how many patients there are for each given 
-- postcode area to check which area would be best to use for the population you are looking for. 
-- Patient counts should be reviewed by gender to make sure there's enough distribution across genders.


-- Patient counts by postcode and gender
WITH patient_counts AS (
    SELECT 
        postcode,
        gender,
        COUNT(*) AS number_of_patients,
        SUM(COUNT(*)) OVER (PARTITION BY postcode) AS total_patients_per_postcode
    FROM 
        dim_patient
    GROUP BY 
        postcode, 
        gender
)
SELECT 
    postcode,
    gender,
    number_of_patients,
    total_patients_per_postcode
FROM 
    patient_counts
ORDER BY 
    total_patients_per_postcode DESC, postcode, gender;


-- Part 2: Using the information you have from part 1 identify the 2 most suitable postcode areas (i.e. largest patient count) 
-- and derive a list of patients that fit the following criteria so that you can invite them to take part in a local research study

WITH patient_counts AS (
	SELECT 
		postcode,
		gender,
		COUNT(*) AS number_of_patients,
		SUM(COUNT(*)) OVER (PARTITION BY postcode) AS total_patients_per_postcode
	FROM 
		dim_patient
	GROUP BY 
		postcode, 
		gender
),
ranked_postcodes AS (
    SELECT 
        postcode,
		gender,
        total_patients_per_postcode,
        DENSE_RANK() OVER (ORDER BY total_patients_per_postcode DESC) AS postcode_rank
    FROM 
        patient_counts
)
SELECT 
    postcode,
	gender,
    total_patients_per_postcode,
    postcode_rank
FROM 
    ranked_postcodes
WHERE 
    postcode_rank <= 20
ORDER BY 
    postcode_rank

-- Part 3:
-- Current diagnosis of asthma, i.e. have current observation in their medical record with relevant clinical codes 
-- from asthma refset (refsetid 999012891000230104 = dim_clinical_codes), and not resolved [see dim_medication[Fhir] : active or completed]

WITH patient_counts AS (
	SELECT 
		postcode,
		registration_guid,
		gender,
		COUNT(*) AS number_of_patients,
		SUM(COUNT(*)) OVER (PARTITION BY postcode) AS total_patients_per_postcode
	FROM 
		dim_patient
	GROUP BY 
		postcode, 
		gender, 
		registration_guid
),
ranked_postcodes AS (
    SELECT 
        postcode,
 		gender,
        total_patients_per_postcode,
        DENSE_RANK() OVER (ORDER BY total_patients_per_postcode DESC) AS postcode_rank
    FROM 
        patient_counts
)
	SELECT 
		postcode_rank
	FROM 
		ranked_postcodes
	WHERE 
		postcode_rank <= 3
	ORDER BY 
		postcode_rank
)
--organisation, registration id, patient id, full name, postcode, age, and gender.
-- current_asthma_patients AS (
    SELECT 
        p.registration_guid,
        p.patient_id,
	    p.patient_givenname,
        p.patient_surname,
        p.postcode,
	    p.age,
        p.gender
    FROM 
        dim_patient p
    JOIN 
		dim_observation AS o 
	ON 
		p.registration_guid = o.registration_guid
    JOIN 
		conditions AS c 
	ON 
		o.emis_code_id = c.code_id
    JOIN 
		top_postcode AS tp 
	ON 
		c.registration_guid = tp.registration_guid
    WHERE 
        c.refset_simple_id = 999012891000230104


-- Q. derive a list of patients that fit the following criteria so that you can invite them to take part in a local research study.

-- Current diagnosis of asthma, i.e. have current observation in their medical record with relevant clinical codes 
-- from asthma refset (refsetid 999012891000230104), and not resolved




--CTE
-- have current observation in medical record
-- refsetid 999012891000230104
-- not resolved
-- inclusion and exclusion


SELECT 
	*
FROM 
	dim_patient AS p
JOIN 
	dim_medication AS m
ON 
	p.registration_guid = m.registration_guid
JOIN 
	product AS prd
ON 
	m.emis_code_id = prd.code_id
JOIN 
	drugs AS d
ON 
	prd.parent_code_id = d.code_id
JOIN 
	dim_observation AS o
ON 
	p.registration_guid = o.registration_guid
JOIN 
	conditions as c
ON 
	o.emis_code_id = c.code_id
WHERE 
	-- patients should have:
	
    c.refset_simple_id = 999012891000230104 AND 			-- has asthma 
	c.emis_term NOT ILIKE '%Asthma resolved%' AND 			-- and asthma not resolved >>>>>>>>>>NOTE: need to confirm with manager
	(d.code_id, d.snomed_concept_id) IN (
		(591221000033116, 129490002), 						-- Formoterol Fumarate
        (717321000033118, 108606009), 						-- Salmeterol Xinafoate
        (1215621000033114, 702408004), 						-- Vilanterol
        (972021000033115, 702801003), 						-- Indacaterol
        (1223821000033118, 704459002) 						-- Olodaterol
    ) AND
	
	-- AND to exclude if:
	
	c.emis_term NOT ILIKE '%smoker%' AND 					-- not a smoker >>>>>>>>>>NOTE: Need to confirm with manager. No such refsetid 999004211000230104 found so filtered on term to be excluded --> Currently a smoker i.e.  have current observation with relevant clinical codes from smoker refset (refsetid 999004211000230104)
	c.snomed_concept_id != 27113001 AND 						-- currently weigh less than 40 kg 
	c.emis_term NOT ILIKE '%COPD%' AND 						-- and COPD not resolved >>>>>>>>>>NOTE: Need to confirm with manager. No such refsetid 999011571000230107 found so filtered on term to be excluded --> Should not currently have a COPD diagnosis i.e. have current observation in their medical record with relevant clinical codes from COPD refset (refsetid 999011571000230107), and not resolved.
								   							-- >>>>>>>>>>NOTE: need to address whether the COPD is resolved or not as this has not been defined
								   
	-- only patients that have not opted out of taking part in research or sharing their medical records
	
	o.emis_original_term NOT ILIKE '%Declined consent for researcher to access clinical record%' AND  			-- >>>>>>>>>>NOTE:Need to confirmed with mananger
    o.emis_original_term NOT ILIKE '%Declined consent to share patient data with specified third party%' AND 
    o.emis_original_term NOT ILIKE '%No consent for electronic record sharing%' AND 
    o.emis_original_term NOT ILIKE '%Refused consent for upload to local shared electronic record%'
	;
	