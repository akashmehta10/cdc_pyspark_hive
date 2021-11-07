# Change Data Capture (Pyspark and Hive)
Change Data Capture (CDC) using Pyspark and Hive provides a capability to determine and track changes in data over time. It performs a type-4 implementation of Slowly Changing Dimension (SCD) by maintaining a snapshot table and a history table. The identified records are marked with the following notation:

| Record Type | row_opern |
| ----------- | --------- |
| No Change   | N         |
| Updated     | U         |
| Inserted    | I         |
| Deleted     | D         |

Full Description: https://medium.com/@akashmehta10/change-data-capture-cdc-using-spark-hive-big-data-beca5afd669f

 **Snapshot Table**
  
|eid|name      |address                 |phone_num |row_opern|rec_eff_dt|
|---|----------|------------------------|----------|---------|----------|
|1  |John D.   |1200 Flagstaff Rd., TX  |9984467826|U        |2021-08-08|
|3  |Linda S.  |3323 Rivera Blvd., NY   |5526631276|U        |2021-08-08|
|4  |Michael W.|2227 Ricks Rd., AR      |2456548766|N        |2021-08-07|
|5  |Sean J.   |1312 MacStreet Blvd., WA|2339794455|N        |2021-08-07|
|6  |Bethany S.|5354 Britain Rd., OR    |5559875643|N        |2021-08-07|
|7  |Christy L.|3321 Fountain Blvd., OR |6642346545|I        |2021-08-08|


 **History Table**

|eid|name      |address                 |phone_num |row_opern|rec_eff_dt|
|---|----------|------------------------|----------|---------|----------|
|1  |John D.   |1200 Flagstaff Rd., TX  |9984467825|I        |2021-08-07|
|1  |John D.   |1200 Flagstaff Rd., TX  |9984467826|U        |2021-08-08|
|2  |Kristy P. |4432 Preston Rd., MA    |3243454112|I        |2021-08-07|
|2  |Kristy P. |4432 Preston Rd., MA    |3243454112|D        |2021-08-08|
|3  |Linda S.  |1104 Olympus Blvd., NY  |5526631276|I        |2021-08-07|
|3  |Linda S.  |3323 Rivera Blvd., NY   |5526631276|U        |2021-08-08|
|4  |Michael W.|2227 Ricks Rd., AR      |2456548766|I        |2021-08-07|
|5  |Sean J.   |1312 MacStreet Blvd., WA|2339794455|I        |2021-08-07|
|6  |Bethany S.|5354 Britain Rd., OR    |5559875643|I        |2021-08-07|
|7  |Christy L.|3321 Fountain Blvd., OR |6642346545|I        |2021-08-08|

## Configuration Steps

### Step 1
Create Hive Tables. Samples attached under hive directory.

### Step 2
Include scd_lib.py library and adjust scd_driver_code.py based on requirements.

### Step 3
spark-submit scd_driver_code.py --py-files cdc-library/scd_lib.py