-- Review entire patient table
SELECT *
FROM dim_patient;

-- Part 1: Using the patient data provided identify how many patients there are for each given 
-- postcode area to check which area would be best to use for the population you are looking for. 
-- Patient counts should be reviewed by gender to make sure there's enough distribution across genders.

SELECT 
	postcode,
	gender,
	COUNT(*) AS number_of_patients,
	SUM(COUNT(*)) OVER (PARTITION BY postcode) AS total_patients_per_postcode
FROM 
	dim_patient
GROUP BY 
	postcode, gender;

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
		postcode, gender
),
ranked_postcodes AS (
    SELECT 
        postcode,
		gender,
        total_patients_per_postcode,
        RANK() OVER (ORDER BY total_patients_per_postcode DESC) AS postcode_rank
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
    postcode_rank <= 2
ORDER BY 
    postcode_rank;


-- Current diagnosis of asthma, i.e. have current observation in their medical record with relevant clinical codes 
-- from asthma refset (refsetid 999012891000230104 = dim_clinical_codes), and not resolved [see dim_medication[Fhir] : active or completed]
SELECT *
FROM dim_clinical_codes;

SELECT *
FROM dim_medication;


SELECT 
	*
FROM 
	dim_clinical_codes AS cl
JOIN 
	dim_medication as me
ON
	cl.code_id = me.emis_code_id
WHERE 
	cl.refset_simple_id = 999012891000230104;


