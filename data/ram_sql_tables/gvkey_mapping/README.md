# GVKey to IdcCode Mapping

The Compustat PIT tables map GVKeys (Companies) to CUSIPs and the CSVSecurity table maps SecIntoCodes (Securities) to CUSIPs. With this information, we can map IDC Codes to Cusips to GVKeys.

Because we no longer have the PIT database, we took a copy of it and will use this as the basis for our point in time mapping. Subsequent changes to Compustat mappings will come through in a CSVSecurity table diff, and appended to our final mapping table.


## Initial Setup

### `make_raw_tables.sql`

Creates the following tables:

1. `ram.dbo.ram_compustat_pit_map_raw` - A raw copy of the entire PIT mapping tables (US and Canada)

2. `ram_compustat_csvsecurity_map_raw` - A raw copy of the entire CSVSecurity table on date of creation

3. `ram.dbo.ram_compustat_pit_map_us` - A merge of PIT GVKeys/Cusips with current SecIntCodes from CSVSecurity. Merged on Cusips

4. `ram_compustat_csvsecurity_map_diffs` - An empty table that will collect the daily diffs of CSVSecurity table.

### `process_raw_tables.sql`



## Daily Pull

### `daily_map_diff.bat` -> `daily_id_diff.sql`


## Notes

* From Compustat, `SecIntCodes` are individual securities, and a GVKey can have multiple SecIntCodes when it has multiple Securities.

* Original PIT table does not map to multiple Securities/CUSIPS, just a single; CSVSecurity maps to multiple

* Three tables will need to be stacked on top of eachother: `ram_compustat_pit_map_raw`, `ram_compustat_csvsecurity_map_raw`, `ram_compustat_csvsecurity_map_diffs`
