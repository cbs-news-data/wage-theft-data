1. Add status/disposition field
    It's also called "action taken" in KY data
2. Add NAICS field if any have it? 
3. Add fields for appeals and appeal outcomes where available
4. Remove records for states like MA which have violations I don't care about in them
5. Remove child labor and other stuff from states that provided that data. Those are not wage theft complaints. 
    1. child labor
    2. retaliation
    3. recordkeeping
6. Add field for interest assessed and also fines
7. Assign unique case ID for cases with multiple violation categories
8. Drop "amount claimed" and "amount assessed" for "amount owed"
    - for records with assessed amounts, use that
    - for records with only claimed amounts, use it but use a disclaimer
    - DO NOT ALLOW ANYTHING WITH NULL OWED AMOUNTS
9. add warnings column to final data
    - e.g. "this state didn't provide any amounts paid"
10. either fully support employer addresses or don't
11. fix uuid field names in args for normalize_data.py
11. Enforce no duplicate rows in individual files
12. Go through each dataset after all steps are done to make sure no fields were missed