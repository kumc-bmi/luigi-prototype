DROP TABLE IF EXISTS source_facts CASCADE;
CREATE TABLE source_facts AS (
  SELECT 1 pat_id, 100 enc_num UNION
  SELECT 2 pat_id, 101 enc_num UNION
  SELECT 3 pat_id, 102 enc_num UNION
  SELECT 4 pat_id, 103 enc_num UNION
  SELECT 2 pat_id, 104 enc_num UNION
  SELECT 5 pat_id, 105 enc_num UNION
  SELECT 1 pat_id, 106 enc_num UNION
  SELECT 6 pat_id, 107 enc_num UNION
  SELECT 7 pat_id, 108 enc_num UNION
  SELECT 1 pat_id, 109 enc_num
);

DROP TABLE IF EXISTS source_patient CASCADE;
CREATE TABLE source_patient AS (
  SELECT 1 pat_id, DATE '01-01-1980' birth_date, 'M' sex, 'Y' hispanic, '05' race UNION
  SELECT 2 pat_id, DATE '02-02-1981' birth_date, 'F' sex, 'Y' hispanic, '06' race UNION
  SELECT 3 pat_id, DATE '03-03-1982' birth_date, 'M' sex, 'Y' hispanic, '03' race UNION
  SELECT 4 pat_id, DATE '04-04-1983' birth_date, 'F' sex, 'N' hispanic, '03' race UNION
  SELECT 5 pat_id, DATE '05-05-1984' birth_date, 'M' sex, 'N' hispanic, '05' race UNION
  SELECT 6 pat_id, DATE '06-06-1985' birth_date, 'F' sex, 'N' hispanic, '05' race UNION
  SELECT 7 pat_id, DATE '07-07-1986' birth_date, 'M' sex, 'N' hispanic, '02' race
);

DROP TABLE IF EXISTS source_visit CASCADE;
CREATE TABLE source_visit AS (
  SELECT 100 enc_num, DATE '01-01-2016' admit_date, DATE '01-02-2016' discharge_date, 'ED' enc_type UNION
  SELECT 101 enc_num, DATE '02-02-2016' admit_date, DATE '02-04-2016' discharge_date, 'AV' enc_type UNION
  SELECT 102 enc_num, DATE '03-03-2016' admit_date, DATE '03-06-2016' discharge_date, 'EI' enc_type UNION
  SELECT 103 enc_num, DATE '04-04-2016' admit_date, DATE '04-08-2016' discharge_date, 'IP' enc_type UNION
  SELECT 104 enc_num, DATE '05-05-2016' admit_date, DATE '05-10-2016' discharge_date, 'ED' enc_type UNION
  SELECT 105 enc_num, DATE '06-06-2016' admit_date, DATE '06-12-2016' discharge_date, 'AV' enc_type UNION
  SELECT 106 enc_num, DATE '07-07-2016' admit_date, DATE '07-14-2016' discharge_date, 'AV' enc_type UNION
  SELECT 107 enc_num, DATE '08-08-2016' admit_date, DATE '08-16-2016' discharge_date, 'IP' enc_type UNION
  SELECT 108 enc_num, DATE '09-09-2016' admit_date, DATE '09-18-2016' discharge_date, 'AV' enc_type UNION
  SELECT 109 enc_num, DATE '10-10-2016' admit_date, DATE '10-20-2016' discharge_date, 'ED' enc_type
);

DROP TABLE IF EXISTS demographic CASCADE;
CREATE TABLE demographic (
    pat_id integer,
    race text,
    hispanic text,
    sex text,
    birth_date date
);

DROP TABLE IF EXISTS encounter CASCADE;
CREATE TABLE encounter (
    enc_num integer,
    pat_id integer,
    admit_date date,
    discharge_date date,
    enc_type text
);