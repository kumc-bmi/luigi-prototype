CREATE OR REPLACE VIEW demographic_transform AS
  SELECT pat_id, race, hispanic, sex, birth_date
  FROM source_patient
;