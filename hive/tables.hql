CREATE TABLE employee
(
eid INT,
name STRING,
address STRING,
phone_num STRING,
row_opern STRING,
rec_eff_dt STRING
)
STORED AS PARQUET;

CREATE TABLE employee_history
(
eid INT,
name STRING,
address STRING,
phone_num STRING,
row_opern STRING
)
PARTITIONED BY
(
rec_eff_dt STRING
)
STORED AS PARQUET;