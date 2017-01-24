CREATE OR REPLACE VIEW encounter_transform AS
  SELECT sv.enc_num, sf.pat_id, sv.admit_date, sv.discharge_date, sv.enc_type
  FROM source_visit AS sv
  JOIN source_facts AS sf
    ON sv.enc_num = sf.enc_num
;