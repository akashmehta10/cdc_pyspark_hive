export PYSPARK_PYTHON='/usr/bin/python3.6'
export PYSPARK_DRIVER_PYTHON='/usr/bin/python3.6'

spark-submit scd_driver_code.py --py-files cdc-library/scd_lib.py

Day 0 (1st Run)

>>> spark.sql("select * from scd_hive_db.employee").show(10, False)
+---+----------+------------------------+----------+---------+----------+       
|eid|name      |address                 |phone_num |row_opern|rec_eff_dt|
+---+----------+------------------------+----------+---------+----------+
|5  |Sean J.   |1312 MacStreet Blvd., WA|2339794455|I        |2021-08-07|
|6  |Bethany S.|5354 Britain Rd., OR    |5559875643|I        |2021-08-07|
|3  |Linda S.  |1104 Olympus Blvd., NY  |5526631276|I        |2021-08-07|
|2  |Kristy P. |4432 Preston Rd., MA    |3243454112|I        |2021-08-07|
|1  |John D.   |1200 Flagstaff Rd., TX  |9984467825|I        |2021-08-07|
|4  |Michael W.|2227 Ricks Rd., AR      |2456548766|I        |2021-08-07|
+---+----------+------------------------+----------+---------+----------+


>>> spark.sql("select * from scd_hive_db.employee_history").show(10, False)
+---+----------+------------------------+----------+---------+----------+
|eid|name      |address                 |phone_num |row_opern|rec_eff_dt|
+---+----------+------------------------+----------+---------+----------+
|5  |Sean J.   |1312 MacStreet Blvd., WA|2339794455|I        |2021-08-07|
|6  |Bethany S.|5354 Britain Rd., OR    |5559875643|I        |2021-08-07|
|3  |Linda S.  |1104 Olympus Blvd., NY  |5526631276|I        |2021-08-07|
|2  |Kristy P. |4432 Preston Rd., MA    |3243454112|I        |2021-08-07|
|1  |John D.   |1200 Flagstaff Rd., TX  |9984467825|I        |2021-08-07|
|4  |Michael W.|2227 Ricks Rd., AR      |2456548766|I        |2021-08-07|
+---+----------+------------------------+----------+---------+----------+


>>> spark.sql("show partitions scd_hive_db.employee_history").show(10, False)
+---------------------+
|partition            |
+---------------------+
|rec_eff_dt=2021-08-07|
+---------------------+

Day 1 (2nd Run)

>>> spark.sql("select * from scd_hive_db.employee").show(10, False)
+---+----------+------------------------+----------+---------+----------+       
|eid|name      |address                 |phone_num |row_opern|rec_eff_dt|
+---+----------+------------------------+----------+---------+----------+
|5  |Sean J.   |1312 MacStreet Blvd., WA|2339794455|N        |2021-08-07|
|6  |Bethany S.|5354 Britain Rd., OR    |5559875643|N        |2021-08-07|
|7  |Christy L.|3321 Fountain Blvd., OR |6642346545|I        |2021-08-08|
|3  |Linda S.  |3323 Rivera Blvd., NY   |5526631276|U        |2021-08-08|
|1  |John D.   |1200 Flagstaff Rd., TX  |9984467826|U        |2021-08-08|
|4  |Michael W.|2227 Ricks Rd., AR      |2456548766|N        |2021-08-07|
+---+----------+------------------------+----------+---------+----------+

>>> spark.sql("select * from scd_hive_db.employee_history where rec_eff_dt='2021-08-08'").show(10, False)
21/08/09 05:55:44 WARN CorruptStatistics: Ignoring statistics because this file was created prior to 1.8.0, see PARQUET-251
+---+----------+-----------------------+----------+---------+----------+
|eid|name      |address                |phone_num |row_opern|rec_eff_dt|
+---+----------+-----------------------+----------+---------+----------+
|7  |Christy L.|3321 Fountain Blvd., OR|6642346545|I        |2021-08-08|
|2  |Kristy P. |4432 Preston Rd., MA   |3243454112|D        |2021-08-08|
|3  |Linda S.  |3323 Rivera Blvd., NY  |5526631276|U        |2021-08-08|
|1  |John D.   |1200 Flagstaff Rd., TX |9984467826|U        |2021-08-08|
+---+----------+-----------------------+----------+---------+----------+

>>> spark.sql("select * from scd_hive_db.employee_history where eid in (1)").show(10, False)
+---+-------+----------------------+----------+---------+----------+
|eid|name   |address               |phone_num |row_opern|rec_eff_dt|
+---+-------+----------------------+----------+---------+----------+
|1  |John D.|1200 Flagstaff Rd., TX|9984467825|I        |2021-08-07|
|1  |John D.|1200 Flagstaff Rd., TX|9984467826|U        |2021-08-08|
+---+-------+----------------------+----------+---------+----------+

>>> spark.sql("select * from scd_hive_db.employee_history where eid in (3)").show(10, False)
+---+--------+----------------------+----------+---------+----------+
|eid|name    |address               |phone_num |row_opern|rec_eff_dt|
+---+--------+----------------------+----------+---------+----------+
|3  |Linda S.|1104 Olympus Blvd., NY|5526631276|I        |2021-08-07|
|3  |Linda S.|3323 Rivera Blvd., NY |5526631276|U        |2021-08-08|
+---+--------+----------------------+----------+---------+----------+

>>> spark.sql("show partitions scd_hive_db.employee_history").show(10, False)
+---------------------+
|partition            |
+---------------------+
|rec_eff_dt=2021-08-07|
|rec_eff_dt=2021-08-08|
+---------------------+
