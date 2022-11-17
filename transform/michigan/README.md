# Notes on Michigan data

The field "OPEN_OR_CLOSED" appears to contain case statuses. However, the "open" cases sometimes contain paid amounts, which does not conform to how pretty much every other state has handled closed vs. open cases. I am therefore converting the "O" cases to null and will let determine_case_outcome to figure out what their dispo was.  