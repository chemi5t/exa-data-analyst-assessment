-- Review entire patient table
SELECT *
FROM dim_patient;



-- Part 1: Using the patient data provided identify how many patients there are for each given postcode 
-- area to check which area would be best to use for the population you are looking for. Patient counts 
-- should be reviewed by gender to make sure there's enough distribution across genders.


-- Patient counts by postcode and gender
WITH patient_counts AS (
    SELECT 
        postcode_area,
        gender,
        COUNT(*) AS number_of_patients,
        SUM(COUNT(*)) OVER (PARTITION BY postcode_area) AS total_patients_per_postcode
    FROM 
        dim_patient
    GROUP BY 
        postcode_area, 
        gender
)
SELECT 
    postcode_area,
    gender,
    number_of_patients,
    total_patients_per_postcode
FROM 
    patient_counts
ORDER BY 
    total_patients_per_postcode DESC, postcode_area, gender;


-- Part 2: Using the information you have from part 1 identify the 2 most suitable postcode areas (i.e. largest patient count) 
-- and derive a list of patients that fit the following criteria so that you can invite them to take part in a local research study.

WITH patient_counts AS (
	SELECT 
		p.postcode_area,
		p.gender,
		COUNT(*) AS number_of_patients,
		SUM(COUNT(*)) OVER (PARTITION BY p.postcode_area) AS total_patients_per_postcode
	FROM 
		dim_patient AS p
	GROUP BY 
		p.postcode_area, 
		p.gender
),
ranked_postcode_areas AS (
    SELECT 
        pc.postcode_area,
		pc.gender,
		pc.number_of_patients,
        pc.total_patients_per_postcode,
        DENSE_RANK() OVER (ORDER BY pc.total_patients_per_postcode DESC) AS postcode_rank
    FROM 
        patient_counts AS pc
)
SELECT 
    rp.postcode_area,
	rp.gender,
	rp.number_of_patients,
    rp.total_patients_per_postcode,
    rp.postcode_rank
FROM 
    ranked_postcode_areas AS rp
WHERE 
    rp.postcode_rank <= 2
ORDER BY 
    rp.postcode_rank


-- Part 3 or 3:
-- >>>>>>>>>>>>>>>>NOTE: refer to investigating.sql to see how the final solution was refined to reduce duplicate entries<<<<<<<<<<<<<<

-- Patients should have:

-- Current diagnosis of asthma, i.e. have current observation in their medical record with relevant clinical codes from 
-- asthma refset (refsetid 999012891000230104), and not resolved
-- Have been prescribed medication from the list below, or any medication containing these ingredients (i.e. need to find 
-- child clinical codes), in the last 30 years:
-- Formoterol Fumarate (codeid 591221000033116, SNOMED concept id 129490002)
-- Salmeterol Xinafoate (codeid 717321000033118, SNOMED concept id 108606009)
-- Vilanterol (codeid 1215621000033114, SNOMED concept id 702408004)
-- Indacaterol (codeid 972021000033115, SNOMED concept id 702801003)
-- Olodaterol (codeid 1223821000033118, SNOMED concept id 704459002)

-- AND should be excluded if:

-- Currently a smoker i.e.  have current observation with relevant clinical codes from smoker refset (refsetid 999004211000230104)
-- Currently weight less than 40kg (SNOMED concept id 27113001)
-- Currently have a COPD diagnosis i.e. have current observation in their medical record with relevant clinical codes from COPD refset (refsetid 999011571000230107), and not resolved.

-- Only patients that have not opted out of taking part in research or sharing their medical record should be invited to participate (type 1 opt out, connected care opt out)

-- You will need to use a combination of the patient, observation, medication and clinical code information for a complete picture of a medical record. You should aim for your code to return a list of eligible patients with information on their organisation, registration id, patient id, full name, postcode, age, and gender.

WITH patient_counts AS (
	SELECT 
		p.patient_id,
		p.postcode_area,
		p.gender,
		COUNT(*) AS number_of_patients,
		SUM(COUNT(*)) OVER (PARTITION BY p.postcode_area) AS total_patients_per_postcode
	FROM 
		dim_patient AS p
	GROUP BY 
		p.postcode_area, 
		p.gender,
		p.patient_id
),
ranked_postcode_areas AS (
    SELECT 
		pc.patient_id,
        pc.postcode_area,
		pc.gender,
		pc.number_of_patients,
        pc.total_patients_per_postcode,
        DENSE_RANK() OVER (ORDER BY pc.total_patients_per_postcode DESC) AS postcode_rank
    FROM 
        patient_counts AS pc
),
top_areas AS (
	SELECT 
		rpa.patient_id,
		rpa.postcode_area,
		rpa.gender,
		rpa.number_of_patients,
		rpa.total_patients_per_postcode,
		rpa.postcode_rank
	FROM 
		ranked_postcode_areas AS rpa
	WHERE 
		rpa.postcode_rank <= 2
	ORDER BY 
		rpa.postcode_rank
),
list_of_patient_criteria AS (
	SELECT 
		o.consultation_source_emis_original_term AS o_consultation_source_emis_original_term,		-- >>>>>>>>>>NOTE:Need to confirmed with mananger in terms of organisation
		p.patient_id,
		m.emis_code_id AS med_emis_code_id,
		m.authorisedissues_authorised_date AS m_authorisedissues_authorised_date,		
		prd.code_id AS prd_code_id,
		d.code_id AS d_code_id,
		d.snomed_concept_id AS d_snomed_concept_id,
		o.emis_original_term AS o_emis_original_term,
		c.code_id AS c_code_id,
		c.refset_simple_id AS c_refset_simple_id,
		c.emis_term AS c_emis_term,
		c.snomed_concept_id AS c_snomed_concept_id
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
		m.authorisedissues_authorised_date::date >= (CURRENT_DATE - INTERVAL '30 years') AND		-- in the last 30 years only >>>>>>>>>>NOTE:Need to confirmed with mananger. Is the correct column selected?
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
),
patient_with_non_null AS (
    SELECT DISTINCT
        lpc.patient_id
    FROM 
        list_of_patient_criteria AS lpc
    WHERE 
        lpc.o_consultation_source_emis_original_term IS NOT NULL
),
filtered_patient_criteria AS (
    SELECT DISTINCT
        lpc.patient_id,
        lpc.o_consultation_source_emis_original_term
    FROM 
        list_of_patient_criteria AS lpc
    LEFT JOIN 
        patient_with_non_null AS pnn 
    ON 
        lpc.patient_id = pnn.patient_id
    WHERE 
        pnn.patient_id IS NULL OR lpc.o_consultation_source_emis_original_term IS NOT NULL
)
-- organisation, registration id, patient id, full name, postcode, age, and gender.
SELECT DISTINCT
	fpc.o_consultation_source_emis_original_term AS o_fpc_organisation,
	p.registration_guid AS p_registration_id,
	p.patient_id AS p_patient_id,
	p.patient_givenname || ' ' || p.patient_surname AS p_full_name,
	p.postcode AS p_postcode,
	p.age AS p_age,
	p.gender AS p_gender
FROM 
	dim_patient as p
JOIN 
	list_of_patient_criteria as lpc
ON
	p.patient_id = lpc.patient_id
JOIN
	patient_counts as pc
ON 
	lpc.patient_id = pc.patient_id
JOIN
	ranked_postcode_areas as rpa
ON
	pc.patient_id = rpa.patient_id
JOIN
	top_areas as ta
ON
	rpa.patient_id = ta.patient_id
JOIN 
    filtered_patient_criteria AS fpc
ON
    p.patient_id = fpc.patient_id
ORDER BY
	p_postcode,
	p_full_name,
	p_patient_id,
	o_fpc_organisation,
	p_age,
	p_gender;
