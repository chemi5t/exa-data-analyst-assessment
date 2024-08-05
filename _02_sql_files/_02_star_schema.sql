SELECT *
FROM dim_patient;

SELECT *
FROM dim_observation;

SELECT *
FROM dim_medication;

SELECT *
FROM dim_clinical_codes;
--------------------------------------------------------------------------
CREATE TABLE product AS
SELECT 
	parent_code_id, 
	code_id, 
	emis_term 
FROM 
	dim_clinical_codes
WHERE 
	parent_code_id IS NOT null AND 
	code_id IS NOT null AND 
	snomed_concept_id IS NULL

SELECT *
FROM product;

ALTER TABLE product 
	ALTER COLUMN parent_code_id type bigint;

--------------------------------------------------------------------------
CREATE TABLE drugs AS
SELECT 
	parent_code_id, 
	code_id, 
	snomed_concept_id, 
	emis_term 
FROM 
	dim_clinical_codes
WHERE 
	parent_code_id IS NOT null AND 
	code_id IS NOT null AND 
	snomed_concept_id IS NOT null

SELECT *
FROM drugs;
--------------------------------------------------------------------------
CREATE TABLE conditions AS
SELECT 
	refset_simple_id, 
	code_id, 
	snomed_concept_id,
	emis_term
	 
FROM 
	dim_clinical_codes
WHERE 
	refset_simple_id IS NOT null AND 
	code_id IS NOT null AND 
	snomed_concept_id IS NOT null
	
SELECT *
FROM conditions;
--------------------------------------------------------------------------
-- to store post_code_area
ALTER TABLE dim_patient ADD COLUMN postcode_area VARCHAR(10);

UPDATE dim_patient
	SET postcode_area = CASE 
		WHEN postcode ~ '^[A-Za-z]+' THEN regexp_replace(postcode, '^([A-Za-z]+).*', '\1')
		ELSE NULL
	END;
--------------------------------------------------------------------------
-- STAR SCHEMA QUERY

ALTER TABLE drugs ADD PRIMARY KEY (code_id);

ALTER TABLE product 
	ADD CONSTRAINT fk_parent_code_id FOREIGN KEY (parent_code_id) REFERENCES drugs(code_id);
	

ALTER TABLE dim_patient ADD PRIMARY KEY (registration_guid);

ALTER TABLE dim_medication 
	ADD CONSTRAINT fk_registration_guid FOREIGN KEY (registration_guid) REFERENCES dim_patient(registration_guid);

ALTER TABLE dim_observation 
	ADD CONSTRAINT fk_registration_guid FOREIGN KEY (registration_guid) REFERENCES dim_patient(registration_guid);

