-- ========================================================================
-- QUERY: Patient Data Filtering and Analysis
-- ========================================================================
-- PURPOSE:
-- This query is designed to filter and analyse patient data based on specific criteria.
-- It focuses on patients with asthma, ensuring they meet various criteria related to
-- medication, smoking status, weight, COPD diagnosis, and consent to share data.

--CONSIDERATIONS: check against refset_simple_id column found originally in dim_clinical_codes


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
		o.consultation_source_emis_original_term AS o_consultation_source_emis_original_term,		-- >>>>>>>>>>NOTE:Need to confirm with mananger in terms of organisation
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
		c.snomed_concept_id AS c_snomed_concept_id,
		prd.emis_term AS prd_emis_term, 					-- prd_emis_term check
		d.emis_term AS d_emis_term,							-- d_emis_term check
		(m.authorisedissues_authorised_date::date >= (CURRENT_DATE - INTERVAL '30 years')) AS m_authorisedissues_authorised_date_lessthanequalto_30years, -- check authorisedissues_authorised_date
		d.parent_code_id AS d_parent_code_id				-- check 
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
		c.refset_simple_id != 999004211000230104 AND 				-- exclude if current smoker
		-- c.emis_term NOT ILIKE '%smoker%' AND 					-- not a smoker >>>>>>>>>>NOTE: Need to confirm with manager. No such refsetid 999004211000230104 found so filtered on term to be excluded --> Currently a smoker i.e.  have current observation with relevant clinical codes from smoker refset (refsetid 999004211000230104)
		c.snomed_concept_id != 27113001 AND 						-- currently weigh less than 40 kg 
		c.refset_simple_id != 999011571000230107 AND				-- exclude COPD diagnosis that shows unresolved >>>>>>>>>>NOTE: Need to confirm with manager. All filtered comments suggest unresolved. No comments about being 'resolved' found.
		-- c.emis_term NOT ILIKE '%COPD%' AND 						-- and COPD not resolved >>>>>>>>>>NOTE: Need to confirm with manager. No such refsetid 999011571000230107 found so filtered on term to be excluded --> Should not currently have a COPD diagnosis i.e. have current observation in their medical record with relevant clinical codes from COPD refset (refsetid 999011571000230107), and not resolved.
																-- >>>>>>>>>>NOTE: need to address whether the COPD is resolved or not as this has not been defined

		-- only patients that have not opted out of taking part in research or sharing their medical records (searched using DISTINCT on colomun to see possible entries)

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
	p.gender AS p_gender,
	p.postcode_area,																-- postcode area check
	(lpc.c_refset_simple_id::bigint) AS c_lpc_refset_simple_id,						-- refset check >>>>>>>>>>NOTE: Need to confirm with manager. The numbers seem all the same and doesn't make sense
	(cc.refset_simple_id::bigint) AS cc_refset_simple_id_bigint,					-- refset check >>>>>>>>>>NOTE: Need to confirm with manager.joined to dim_clinical_codes with refset_simple_id cast to bigint to cross ref with c_lpc_refset_simple_id
	cc.refset_simple_id AS cc_refset_simple_id_double_precision,					-- refset check >>>>>>>>>>NOTE: Need to confirm with manager.joined to dim_clinical_codes with refset_simple_id left as double precision to cross ref with c_lpc_refset_simple_id
	cc.parent_code_id AS cc_parent_code_id,											-- to check with code_id
	ta.postcode_rank,																-- area rank check
    CASE 
        WHEN lpc.c_refset_simple_id = 999012891000230104 THEN TRUE
        ELSE FALSE
    END AS c_lpc_refset_simple_id_999012891000230104_has_asthma, 					-- current asthma diagnosis check
    CASE
        WHEN lpc.c_emis_term NOT ILIKE '%Asthma resolved%' THEN TRUE
        ELSE FALSE
    END AS c_lpc_emis_term_unresolved_asthma, 										-- unresolved asthma check
    lpc.m_authorisedissues_authorised_date_lessthanequalto_30years,					-- medication prescribed within last 30 years check 
    lpc.d_code_id AS d_lpc_code_id,													-- code id check
	lpc.prd_code_id AS prd_lpc_code_id_child_clinical,								-- code id check
    lpc.d_snomed_concept_id AS d_lpc_snomed_concept_id,								-- code id check
    CASE
        WHEN (lpc.d_code_id, lpc.d_snomed_concept_id) IN (
            (591221000033116, 129490002), -- Formoterol Fumarate
            (717321000033118, 108606009), -- Salmeterol Xinafoate
            (1215621000033114, 702408004), -- Vilanterol
            (972021000033115, 702801003), -- Indacaterol
            (1223821000033118, 704459002) -- Olodaterol
        ) THEN TRUE
        ELSE FALSE
    END AS d_lpc_code_id_and_d_lpc_snomed_concept_id_filtered, 						-- specific medications check
	CASE 
		WHEN lpc.c_refset_simple_id != 999004211000230104 THEN TRUE
        ELSE FALSE
    END AS lpc_refset_999004211000230104_non_smoker, 								-- exclude if current smoker check
-- 	CASE 
--         WHEN lpc.c_emis_term NOT ILIKE '%smoker%' THEN TRUE 
--         ELSE FALSE 
--     END AS c_lpc_emis_term_not_smoker, -- not a smoker check. NOTE: refset missing 999004211000230104 issue flagged. Hence filtered on word.
    CASE
        WHEN lpc.c_snomed_concept_id != 27113001 THEN TRUE
        ELSE FALSE
    END AS c_lpc_snomed_concept_id_not_27113001_ismorethan40kg, 					-- check patient does not weigh less than 40 kg
	CASE 
		WHEN lpc.c_refset_simple_id != 999011571000230107 THEN TRUE
        ELSE FALSE
    END AS lpc_refset_999011571000230107_no_COPD, 									-- no COPD check
--     CASE
--         WHEN lpc.c_emis_term NOT ILIKE '%COPD%' THEN TRUE
--         ELSE FALSE
--     END AS c_lpc_emis_term_no_COPD, -- patient does not have COPD
    CASE
        WHEN lpc.o_emis_original_term NOT ILIKE '%Declined consent for researcher to access clinical record%' THEN TRUE
        ELSE FALSE
    END AS o_lpc_emis_original_term_not_opted_out1, 									-- the patient has not opted out check
    CASE
        WHEN lpc.o_emis_original_term NOT ILIKE '%Declined consent to share patient data with specified third party%' THEN TRUE
        ELSE FALSE
    END AS o_lpc_emis_original_term_not_opted_out2, 									-- the patient has not opted out check
    CASE
        WHEN lpc.o_emis_original_term NOT ILIKE '%No consent for electronic record sharing%' THEN TRUE
        ELSE FALSE
    END AS o_lpc_emis_original_term_not_opted_out3,										-- the patient has not opted out check
    CASE
        WHEN lpc.o_emis_original_term NOT ILIKE '%Refused consent for upload to local shared electronic record%' THEN TRUE
        ELSE FALSE
    END AS no_refused_consent_for_local_uploado_lpc_emis_original_term_not_opted_out4,	-- the patient has not opted out check
	lpc.c_emis_term AS lpc_c_emis_term,												-- validity check
	lpc.prd_emis_term AS lpc_prd_emis_term,											-- validity check
	lpc.d_emis_term AS lpc_d_emis_term												-- validity check
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
JOIN
	dim_clinical_codes AS cc
ON
	lpc.d_parent_code_id = cc.parent_code_id
ORDER BY
	p_postcode,
	p_full_name,
	p_patient_id,
	o_fpc_organisation,
	p_age,
	p_gender;
