# GVKey to IdcCode Mapping

The Compustat PIT tables map GVKeys (Companies) to CUSIPs and the CSVSecurity table maps SecIntoCodes (Securities) to CUSIPs. With this information, we can map IDC Codes to Cusips to GVKeys.

Because we no longer have the PIT database, we took a copy of it and will use this as the basis for our point in time mapping. Subsequent changes to Compustat mappings will come through in a CSVSecurity table diff, and appended to our final mapping table.


## Initial Setup

### `make_raw_tables.sql`

Creates the following tables:

1. `ram.dbo.ram_compustat_pit_map_raw` - A raw copy of the entire PIT mapping tables (US and Canada)

2. `ram_compustat_csvsecurity_map_raw` - A raw copy of the entire CSVSecurity table on date of creation

3. `ram.dbo.ram_compustat_pit_map_us` - A merge of PIT GVKeys/Cusips with current SecIntCodes from CSVSecurity.

4. `ram_compustat_csvsecurity_map_diffs` - An empty table that will collect the daily diffs of CSVSecurity table.

## Daily Pull

Run via: `daily_maping.bat`

Every day the main mapping table is wiped and recreated. Below are the scripts that comprise that process.


### `daily_id_diff.sql`

This script will take the original table (`ram.dbo.ram_compustat_pit_map_raw` from May 2018) and stack on top of it `ram_compustat_csvsecurity_map_diffs`, which this script will add to. The logic is that these two tables contain the SecIntCodes, and then time stamps for changes to the GVKeys and CUSIPs for those SecIntCodes.

At this point, there is no creation of Start and EndDates and no error handling.


### `process_raw_tables.sql`

This table will separate the problematic cases from everything else. The non-problematic cases will be written to the mapping database, while the problematic ones will be written to file for handling in the next process.

### `handle_mappings.py`

This process will check the problematic cases against a file of manually handled cases. If there is a case that has not been handled, an email will be sent to `notifications@roundaboutam.com`


## Notes

* From Compustat, `SecIntCodes` are individual securities, and a GVKey can have multiple SecIntCodes when it has multiple Securities.

* Original PIT table does not map to multiple Securities/CUSIPS, just a single; CSVSecurity maps to multiple

* In the case where the tables have been wiped, the files that exist in DATA/ram/data/gvkey_mapping can be used to recreate the tables. The original_csvsecurity_20180521 is the original raw table, and the diff files can be loaded into the diff table.
