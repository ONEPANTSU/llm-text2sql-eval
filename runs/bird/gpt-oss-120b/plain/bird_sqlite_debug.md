# bird_sqlite debug

## Top mismatches
- question: What is the highest eligible free rate for K-12 students in the schools in Alameda County?
  gold_sql: `SELECT `Free Meal Count (K-12)` / `Enrollment (K-12)` FROM frpm WHERE `County Name` = 'Alameda' ORDER BY (CAST(`Free Meal Count (K-12)` AS REAL) / `Enrollment (K-12)`) DESC LIMIT 1`
  pred_sql: ``
  reason: pred_generation_fail
- question: Please list the lowest three eligible free rates for students aged 5-17 in continuation schools.
  gold_sql: `SELECT `Free Meal Count (Ages 5-17)` / `Enrollment (Ages 5-17)` FROM frpm WHERE `Educational Option Type` = 'Continuation School' AND `Free Meal Count (Ages 5-17)` / `Enrollment (Ages 5-17)` IS NOT NULL ORDER BY `Free Meal Count (Ages 5-17)` / `Enrollment (Ages 5-17)` ASC LIMIT 3`
  pred_sql: ``
  reason: pred_generation_fail
- question: Please list the zip code of all the charter schools in Fresno County Office of Education.
  gold_sql: `SELECT T2.Zip FROM frpm AS T1 INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode WHERE T1.`District Name` = 'Fresno County Office of Education' AND T1.`Charter School (Y/N)` = 1`
  pred_sql: ``
  reason: pred_generation_fail
- question: What is the unabbreviated mailing street address of the school with the highest FRPM count for K-12 students?
  gold_sql: `SELECT T2.MailStreet FROM frpm AS T1 INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode ORDER BY T1.`FRPM Count (K-12)` DESC LIMIT 1`
  pred_sql: `SELECT s.unabbreviated_mailing_street_address
FROM schools s
JOIN (
    SELECT school_id
    FROM frpm
    WHERE student_level = 'K-12'
    ORDER BY frpm_count DESC
    LIMIT 1
) f ON s.school_id = f.school_id;`
  reason: pred_exec_fail
  gold_rows: [['14429 South Downey Avenue']]
- question: Please list the phone numbers of the direct charter-funded schools that are opened after 2000/1/1.
  gold_sql: `SELECT T2.Phone FROM frpm AS T1 INNER JOIN schools AS T2 ON T1.CDSCode = T2.CDSCode WHERE T1.`Charter Funding Type` = 'Directly funded' AND T1.`Charter School (Y/N)` = 1 AND T2.OpenDate > '2000-01-01'`
  pred_sql: ``
  reason: pred_generation_fail

## Error types
- pred_generation_fail: 966
- pred_exec_fail: 237
- row_mismatch_0: 120
- row_count_mismatch: 85
- task_timeout: 3