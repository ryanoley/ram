Background
----------
Mapping to the Compustat Database has proven to be a difficult task
through the Securities Master Tables (SecMstrX/SecMapX) due to their
atemporal nature. Therefore, we are working through the IDC reference
tables that including historical mappings to CUSIPs.

Now, under most conditions this is a straight forward process. However,
there are some edge cases caused by odd corporate actions and/or bad
data that necessitate manually filtering the instances where multiple
GVKeys (essentially a company ID) are mapped to a single IDC Code
(a security ID).

It is also worth noting that we are functioning from a Security-first
perspective, meaning the correct question is:
```
Given this Security and this Date, which GVKey/Company is it associated with?
```

It has not been verified whether a GVKey-first mapping works properly.


Daily Routine
--------------
1. Run `get_map_data.bat`

If there are values that have to be manually adjusted

1. Run `mapping_verification.py -m` if there are mappings
2. Run `mapping_verification.py -a` after to add those new mappings and clean
the directory
3. Run `make_mapping_table.py` to re-write to SQL Server Database


How to use mapping_verification.py
----------------------------------
This script iterates through each IDC Code that has duplicate GVKeys,
and the purpose is to correctly specify which GVKeys belong to the
IDC Code, and when they changed from one to the next.

The User will be prompted to specify the number of GVKeys that are
appropriate, then sequentially how the GVKeys should be ordered and what
their applicable dates should be.

There are a few things to note:
* The GVKey data frame is sorted by GVKey, but this may not represent
the correct order. Be sure to reference the Report Dates and the Change
dates. Sometimes the larger GVKeys may come first.
