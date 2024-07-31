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