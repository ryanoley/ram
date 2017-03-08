Background
----------
Mapping to the Compustat Database has proven to be a difficult task
through the Securities Master Tables (SecMstrX/SecMapX) due to their
atemporal nature. Therefore, we are working through the IDC reference
tables that including historical mappings to CUSIPs.

Now, undermost conditions this is a straight forward process. However,
there are some edge cases caused by odd corporate actions and/or bad
data that necessitate manually filtering the instances where multiple
GVKeys (essentially a company ID) are mapped to a single IDC Code
(a security ID).

It is also worth noting that we are functioning from a Security-first
perspective, meaning the correct question is:
```
"Given this Security and this Date, which GVKey/Company is it associated
with"
```

It has not been verified whether a GVKey-first mapping works properly.
(I suspect it doesn't work properly because a GVKey will map to multiple
IDC Codes).


Initialization
--------------
1. Run `init_map_data.bat`
2. Run `init_filter_bad_mappings.py` from the Command Line


How to use init_filter_bad_mappings.py
--------------------------------------
This script iterates through each IDC Code that has duplicate GVKeys,
and the purpose is to correctly specify which GVKeys belong to the
IDC Code, and when they changed from one to the next.

The User will be prompted to specify the number of GVKeys that are
appropriate, then sequentially how the GVKeys should be ordered and what
their min and max applicable dates should be.

There are a few things to note:
* The GVKey data frame is sorted by GVKey, but this may not represent
the correct order. Be sure to reference the Report Dates and the Change
dates. Sometimes the larger GVKeys may come first.
* An improvement on the routine could be to infer start dates for GVKeys
after the first.
