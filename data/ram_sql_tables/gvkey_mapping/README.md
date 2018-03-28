## Notes

* From Compustat, `SecIntCodes` are individual securities, and a GVKey can have multiple SecIntCodes when it has multiple Securities.

* Original PIT table does not map to multiple Securities/CUSIPS, just a single; CSVSecurity maps to multiple

* Three tables will need to be stacked on top of eachother: `ram_compustat_pit_map_raw`, `ram_compustat_csvsecurity_map_raw`, `ram_compustat_csvsecurity_map_diffs`
