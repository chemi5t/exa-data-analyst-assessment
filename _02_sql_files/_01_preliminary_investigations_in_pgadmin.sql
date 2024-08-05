SELECT *
FROM dim_patient;
-->>>>is linked to obervation and medication via the 'registration_guid' column respectively<<<<--

SELECT *
FROM dim_observation;
-->>>>is linked to patient and medication via the 'registration_guid' column respectively<<<<--

SELECT *
FROM dim_medication;
-->>>>is linked to patient and medication via the 'registration_guid' column respectively<<<<--

SELECT *
FROM dim_clinical_codes;

--------------------------------------------------------------------------
--### patient to observation ###
--##############################
SELECT *
FROM dim_patient;

--dim_patient has 4,543 rows
SELECT registration_guid
FROM dim_patient
WHERE registration_guid = '9A70C9CD-D0F7-43F3-A05C-34C8F192E00C';
-- dim_observation has 275,768 rows
SELECT registration_guid
FROM dim_observation
WHERE registration_guid = '9A70C9CD-D0F7-43F3-A05C-34C8F192E00C';

--dim_patient has 4,543 rows
SELECT registration_guid
FROM dim_patient
WHERE registration_guid = '9A70C9CD-D0F7-43F3-A05C-34C8F192E00C';
-- dim_observation has 214,016 rows
SELECT registration_guid
FROM dim_medication
WHERE registration_guid = '9A70C9CD-D0F7-43F3-A05C-34C8F192E00C';


SELECT *
FROM dim_observation;
--------------------------------------------------------------------------
--### observation to medication ###
--##############################
SELECT *
FROM dim_observation;

-- one to many (above and below queries). registration_guid (tex) = registration_guid (text)

--dim_patient has 275,768 rows
SELECT registration_guid
FROM dim_observation
WHERE registration_guid = 'D84C873E-C19E-4DC8-BA27-66A382FF8D6E';
-- dim_observation has 214,016 rows
SELECT registration_guid
FROM dim_medication
WHERE registration_guid = 'D84C873E-C19E-4DC8-BA27-66A382FF8D6E';

SELECT *
FROM dim_medication;
--------------------------------------------------------------------------
--### medication to clinical ###
--##############################
SELECT *
FROM dim_medication;

-- many to none and none to one (DOESNT WORK)

-- dim_observation has 214,016 rows
SELECT emis_code_id
FROM dim_medication
WHERE emis_code_id = 837841000033110;
-- dim_medication has 586 rows
SELECT code_id
FROM dim_clinical_codes
WHERE code_id = 837841000033110;

SELECT *
FROM dim_clinical_codes;
--------------------------------------------------------------------------
SELECT *
FROM dim_clinical_codes;


SELECT snomed_concept_id
FROM dim_observation
WHERE snomed_concept_id = 1.06759910001191e+16;

SELECT snomed_concept_id
FROM dim_medication
WHERE snomed_concept_id = 1.06759910001191e+16;


SELECT code_id
FROM dim_clinical_codes
WHERE code_id = 849641000033117;

------------------------------------
-- delete this if needed

SELECT *
FROM dim_medication;

-- many to none and none to one (DOESNT WORK)

-- dim_observation has 214,016 rows
SELECT emis_code_id, snomed_concept_id
FROM dim_medication
WHERE emis_code_id = 1.030341000033116e+15;
-- dim_medication has 586 rows
SELECT code_id
FROM dim_clinical_codes
WHERE code_id = 1.030341000033116e+15;

SELECT *
FROM dim_clinical_codes;




--------------------------------------------------------------------------
--------------------------------------------------------------------------
--------------------------------------------------------------------------
--------------------------------------------------------------------------

-- table: dim_observation column: PK emis_observation_id
--275,768
SELECT DISTINCT emis_observation_id
FROM dim_observation
WHERE emis_observation_id = 2806335;

-- dim_medication = PK medication_guid
--214,016
SELECT DISTINCT medication_guid
FROM dim_medication
WHERE medication_guid = 'C2595C9E-3B3E-43A7-BD16-2A5C4114FD20';


-- dim_observation = PK emis_observation_id
--214,016
SELECT DISTINCT medication_guid
FROM dim_medication
WHERE medication_guid = 'C2595C9E-3B3E-43A7-BD16-2A5C4114FD20';


--------------------------------------------------------------------------
-- working query!

SELECT 
	p.patient_id, 
	p.registration_guid, 
	m.registration_guid, 
	o.registration_guid, 
	m.emis_code_id, 
	m.snomed_concept_id, 
	o.snomed_concept_id, 
	c.snomed_concept_id, 
	prd.emis_term, 
	d.snomed_concept_id, 
	c.refset_simple_id
FROM dim_patient AS p
JOIN dim_medication AS m
ON p.registration_guid = m.registration_guid
JOIN product AS prd
ON m.emis_code_id = prd.code_id
JOIN drugs AS d
ON prd.parent_code_id = d.code_id
RIGHT JOIN dim_observation AS o
ON o.registration_guid = p.registration_guid
RIGHT JOIN conditions as c
ON c.code_id = o.emis_code_id;


-------------------------------
-------------------------------

-- normalisaion of clinical_codes tables 

SELECT *
FROM drugs -- has 5 rows

SELECT *
FROM product -- has 147 rows

SELECT *
FROM conditions -- has 434 rows

-- above combined has 586 rows. Same as in the original dim_clinical_codes

SELECT *
FROM dim_clinical_codes -- 586 rows


-------------------------------
-------------------------------

-- at first query had alot of duplicate rows
-- 515 rows
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
)
-- organisation, registration id, patient id, full name, postcode, age, and gender.
SELECT 
	lpc.o_consultation_source_emis_original_term AS o_lpc_organisation,
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
ORDER BY
	p_postcode,
	p_full_name,
	p_patient_id,
	o_lpc_organisation,
	p_age,
	p_gender;

-------------------------------
-------------------------------

-- adding distinct to final select query helped to reduce issue but not fully
-- 96 rows
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
)
-- organisation, registration id, patient id, full name, postcode, age, and gender.
SELECT DISTINCT
	lpc.o_consultation_source_emis_original_term AS o_lpc_organisation,
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
ORDER BY
	p_postcode,
	p_full_name,
	p_patient_id,
	o_lpc_organisation,
	p_age,
	p_gender;

-------------------------------
-------------------------------

-- sense check below shows there should be 56 rows in the final solution

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
distinct_patients AS (
    SELECT DISTINCT
        lpc.patient_id
    FROM 
        dim_patient AS p
    JOIN 
        list_of_patient_criteria AS lpc
    ON
        p.patient_id = lpc.patient_id
    JOIN
        patient_counts AS pc
    ON 
         pc.patient_id = lpc.patient_id
    JOIN
        ranked_postcode_areas AS rpa
    ON
        pc.patient_id = rpa.patient_id
    JOIN
        top_areas AS ta
    ON
        rpa.patient_id = ta.patient_id
)
SELECT 
    COUNT(DISTINCT patient_id) AS unique_patient_count
FROM 
    distinct_patients;

-------------------------------
-------------------------------

-- knowing from earlier there should only be 56 distinct rows on patient_id after filter; the following was run to give the final answer that confirms with the above output
-- 56 rows
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
