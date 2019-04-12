# medical-insight
User driven application with options to pull multi-dimension information related to health care using window function

*******************************************************************************************************************
This module provide information about Providers, their services, and the incurred cost for a given treatment.
It can also provide the cheapest and the costliest provider, and various insights on Inpatient data.
*******************************************************************************************************************
Here are the KEYWORDS to get the desired information:

--PROVIDER zip/city/state value
    Return Providers in a given ZIP/CITY/STATE. If provider not found for the ZIP then return information for 10 nearby 
    ZIP.
    
    ** USAGE **
    --PROVIDER ZIP 12345 
    --PROVIDER CITY 'NEW YORK'
    --PROVIDER STATE 'NY' 

-- DIAG zip/city/state value diagName 
    Return Providers in a given ZIP/CITY/STATE which treats the given medical condition. 
    
    ** USAGE **
    --DIAG ZIP 12345 'HEART'
    --DIAG CITY 'MADISON' 'KIDNEY'
    --DIAG STATE 'CA' 'BLOOD'
          
-- TOP H/L zip/city/state value
    H - Return TOP 10 providers with highest treatment cost in a given ZIP/CITY/STATE
    L - Return TOP 10 providers with cheapest treatment cost in a given ZIP/CITY/STATE
    
    ** USAGE **
    --TOP H zip 12345
    --TOP L city 'MADISON'
    
-- TOP-PAT H/L zip/city/state value
    H - Return Top 10 providers with highest Patient incurred cost in a given ZIP/CITY/STATE
    L - Return Top 10 providers with cheapest Patient incurred cost in a given ZIP/CITY/STATE
    
    ** USAGE **
    --TAP-PAT H zip 12345
    --TAP-PAT L city 'MADISON'
    
-- TOP-MED H/L zip/city/state value
    H - Return Top 10 providers with highest Total Medical cost in a given ZIP/CITY/STATE
    L - Return Top 10 providers with cheapest Total Medical cost in a given ZIP/CITY/STATE  
    
    ** USAGE **
    --TAP-MED H zip 12345
    --TAP-MED L city 'MADISON'    
    
-- TOP-DIA-PAT H/L zip/city/state value
    H - Return Top 10 Treatments with highest Patient cost in a given ZIP/CITY/STATE
    L - Return Top 10 Treatments with cheapest Patient cost in a given ZIP/CITY/STATE
    
    ** USAGE **
    --TAP-DIA-PAT H zip 12345
    --TAP-DIA-PAT L city 'MADISON'    
    
-- TOP-DIA-MED H/L zip/city/state value
    H - Return Top 10 Treatments with highest Total Medical cost in a given ZIP/CITY/STATE
    L - Return Top 10 Treatments with cheapest Total Medical cost in a given ZIP/CITY/STATE    
    
    ** USAGE **
    --TAP-DIA-MED H zip 12345
    --TAP-DIA-MED L city 'MADISON'
    
-- TOP-DIA H/L 
    H - Return Top 10 Treatments with highest Total Medical cost in Country
    L - Return Top 10 Treatments with cheapest Total Medical cost in Country    
    
    ** USAGE **
    --TAP-DIA H zip 12345
    --TAP-DIA L city 'MADISON' 
