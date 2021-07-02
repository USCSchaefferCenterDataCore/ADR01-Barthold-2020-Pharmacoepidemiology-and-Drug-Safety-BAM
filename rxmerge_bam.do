clear all
set more off
capture log close

/*
•	Input: XXXXX_YYYY.dta, 06-13, for oxybu tolte flavo hyosc darif trosp solif fesot propa mirab 
•	Output: bam_YYYY.dta, 06-13
•	In each year, merge all the ten molecules into a single file. Make year variable, fill in zeros, make date variables.
•	Merge the years 06-13. In each year, require that ptd==1, and then merge into 0613 file. Make composite min date variables.
*/

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
//																			  //
//					Merge the 10 molecules within each year 				  //
//																			  //
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
//darif fesot flavo oxybu solif tolte trosp     

////////////////////////	2006 		////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2006.dta", replace
keep bene_id darif_pdays_2006 darif_clms_2006 darif_minfilldt_2006 darif_totdd_2006 darif_fdays_2006 darif_tdd_2006
tempfile 2006
save `2006', replace

//no 2006 fesot

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2006.dta", replace
keep bene_id flavo_pdays_2006 flavo_clms_2006 flavo_minfilldt_2006 flavo_totdd_2006 flavo_fdays_2006 flavo_tdd_2006
merge 1:1 bene_id using `2006'
drop _m
save `2006', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2006.dta", replace
keep bene_id oxybu_pdays_2006 oxybu_clms_2006 oxybu_minfilldt_2006 oxybu_totdd_2006 oxybu_fdays_2006 oxybu_tdd_2006
merge 1:1 bene_id using `2006'
drop _m
save `2006', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2006.dta", replace
keep bene_id solif_pdays_2006 solif_clms_2006 solif_minfilldt_2006 solif_totdd_2006 solif_fdays_2006 solif_tdd_2006
merge 1:1 bene_id using `2006'
drop _m
save `2006', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2006.dta", replace
keep bene_id tolte_pdays_2006 tolte_clms_2006 tolte_minfilldt_2006 tolte_totdd_2006 tolte_fdays_2006 tolte_tdd_2006
merge 1:1 bene_id using `2006'
drop _m
save `2006', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2006.dta", replace
keep bene_id trosp_pdays_2006 trosp_clms_2006 trosp_minfilldt_2006 trosp_totdd_2006 trosp_fdays_2006 trosp_tdd_2006
merge 1:1 bene_id using `2006'
drop _m
save `2006', replace

// Organize 2006
gen bam2006 = 1

local ldrugs "fesot"
local vars "pdays clms totdd fdays tdd"
local drugs "darif fesot flavo oxybu solif tolte trosp"
foreach j in `ldrugs'{
	gen `j'_minfilldt_2006 = .
	foreach i in `vars'{
		gen `j'_`i'_2006 = 0
	}
}
foreach d in `drugs'{
	rename `d'_minfilldt_2006 `d'_d_2006
	foreach i in `vars'{
		replace `d'_`i'_2006 = 0 if `d'_`i'_2006==.
	}
}

//	Nonselective antimuscarinics (oxybutynin, tolterodine, flavoxate, hyoscyamine, trospium, fesoterodine, propantheline) 
//	M3 selective antimuscarinics (darifenacin or solifenacin)
egen nsl_d_2006 = rowmin(fesot_d_2006 flavo_d_2006 oxybu_d_2006 tolte_d_2006 trosp_d_2006)
egen nsl2_d_2006 = rowmin(fesot_d_2006 flavo_d_2006 oxybu_d_2006 tolte_d_2006)
egen m3s_d_2006 = rowmin(darif_d_2006 solif_d_2006)
egen bam_d_2006 = rowmin(fesot_d_2006 flavo_d_2006 oxybu_d_2006 tolte_d_2006 trosp_d_2006 darif_d_2006 solif_d_2006) 

foreach i in `vars'{
	gen nsl_`i'_2006 = 0
	gen nsl2_`i'_2006 = 0
	gen m3s_`i'_2006 = 0
	gen bam_`i'_2006 = 0
	replace nsl_`i'_2006 = fesot_`i'_2006 + flavo_`i'_2006 + oxybu_`i'_2006 + tolte_`i'_2006 + trosp_`i'_2006
	replace nsl2_`i'_2006 = fesot_`i'_2006 + flavo_`i'_2006 + oxybu_`i'_2006 + tolte_`i'_2006 
	replace m3s_`i'_2006 = darif_`i'_2006 + solif_`i'_2006 
	replace bam_`i'_2006 = fesot_`i'_2006 + flavo_`i'_2006 + oxybu_`i'_2006 + tolte_`i'_2006 + trosp_`i'_2006 + darif_`i'_2006 + solif_`i'_2006 
}
replace nsl_pdays_2006 = 365 if nsl_pdays_2006>365
replace nsl2_pdays_2006 = 365 if nsl2_pdays_2006>365
replace m3s_pdays_2006 = 365 if m3s_pdays_2006>365
replace bam_pdays_2006 = 365 if bam_pdays_2006>365

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_2006.dta", replace

////////////////////////	2007 		////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2007.dta", replace
keep bene_id darif_pdays_2007 darif_clms_2007 darif_minfilldt_2007 darif_totdd_2007 darif_fdays_2007 darif_tdd_2007
tempfile 2007
save `2007', replace

//no 2007 fesot

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2007.dta", replace
keep bene_id flavo_pdays_2007 flavo_clms_2007 flavo_minfilldt_2007 flavo_totdd_2007 flavo_fdays_2007 flavo_tdd_2007
merge 1:1 bene_id using `2007'
drop _m
save `2007', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2007.dta", replace
keep bene_id oxybu_pdays_2007 oxybu_clms_2007 oxybu_minfilldt_2007 oxybu_totdd_2007 oxybu_fdays_2007 oxybu_tdd_2007
merge 1:1 bene_id using `2007'
drop _m
save `2007', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2007.dta", replace
keep bene_id solif_pdays_2007 solif_clms_2007 solif_minfilldt_2007 solif_totdd_2007 solif_fdays_2007 solif_tdd_2007
merge 1:1 bene_id using `2007'
drop _m
save `2007', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2007.dta", replace
keep bene_id tolte_pdays_2007 tolte_clms_2007 tolte_minfilldt_2007 tolte_totdd_2007 tolte_fdays_2007 tolte_tdd_2007
merge 1:1 bene_id using `2007'
drop _m
save `2007', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2007.dta", replace
keep bene_id trosp_pdays_2007 trosp_clms_2007 trosp_minfilldt_2007 trosp_totdd_2007 trosp_fdays_2007 trosp_tdd_2007
merge 1:1 bene_id using `2007'
drop _m
save `2007', replace

// Organize 2007
gen bam2007 = 1

local ldrugs "fesot"
local vars "pdays clms totdd fdays tdd"
local drugs "darif fesot flavo oxybu solif tolte trosp"
foreach j in `ldrugs'{
	gen `j'_minfilldt_2007 = .
	foreach i in `vars'{
		gen `j'_`i'_2007 = 0
	}
}
foreach d in `drugs'{
	rename `d'_minfilldt_2007 `d'_d_2007
	foreach i in `vars'{
		replace `d'_`i'_2007 = 0 if `d'_`i'_2007==.
	}
}

//	Nonselective antimuscarinics (oxybutynin, tolterodine, flavoxate, hyoscyamine, trospium, fesoterodine, propantheline) 
//	M3 selective antimuscarinics (darifenacin or solifenacin)
egen nsl_d_2007 = rowmin(fesot_d_2007 flavo_d_2007 oxybu_d_2007 tolte_d_2007 trosp_d_2007)
egen nsl2_d_2007 = rowmin(fesot_d_2007 flavo_d_2007 oxybu_d_2007 tolte_d_2007)
egen m3s_d_2007 = rowmin(darif_d_2007 solif_d_2007)
egen bam_d_2007 = rowmin(fesot_d_2007 flavo_d_2007 oxybu_d_2007 tolte_d_2007 trosp_d_2007 darif_d_2007 solif_d_2007) 

foreach i in `vars'{
	gen nsl_`i'_2007 = 0
	gen nsl2_`i'_2007 = 0
	gen m3s_`i'_2007 = 0
	gen bam_`i'_2007 = 0
	replace nsl_`i'_2007 = fesot_`i'_2007 + flavo_`i'_2007 + oxybu_`i'_2007 + tolte_`i'_2007 + trosp_`i'_2007
	replace nsl2_`i'_2007 = fesot_`i'_2007 + flavo_`i'_2007 + oxybu_`i'_2007 + tolte_`i'_2007
	replace m3s_`i'_2007 = darif_`i'_2007 + solif_`i'_2007 
	replace bam_`i'_2007 = fesot_`i'_2007 + flavo_`i'_2007 + oxybu_`i'_2007 + tolte_`i'_2007 + trosp_`i'_2007 + darif_`i'_2007 + solif_`i'_2007 
}
replace nsl_pdays_2007 = 365 if nsl_pdays_2007>365
replace nsl2_pdays_2007 = 365 if nsl2_pdays_2007>365
replace m3s_pdays_2007 = 365 if m3s_pdays_2007>365
replace bam_pdays_2007 = 365 if bam_pdays_2007>365

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_2007.dta", replace

////////////////////////	2008 		////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2008.dta", replace
keep bene_id darif_pdays_2008 darif_clms_2008 darif_minfilldt_2008 darif_totdd_2008 darif_fdays_2008 darif_tdd_2008
tempfile 2008
save `2008', replace

//no 2008 fesot

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2008.dta", replace
keep bene_id flavo_pdays_2008 flavo_clms_2008 flavo_minfilldt_2008 flavo_totdd_2008 flavo_fdays_2008 flavo_tdd_2008
merge 1:1 bene_id using `2008'
drop _m
save `2008', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2008.dta", replace
keep bene_id oxybu_pdays_2008 oxybu_clms_2008 oxybu_minfilldt_2008 oxybu_totdd_2008 oxybu_fdays_2008 oxybu_tdd_2008
merge 1:1 bene_id using `2008'
drop _m
save `2008', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2008.dta", replace
keep bene_id solif_pdays_2008 solif_clms_2008 solif_minfilldt_2008 solif_totdd_2008 solif_fdays_2008 solif_tdd_2008
merge 1:1 bene_id using `2008'
drop _m
save `2008', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2008.dta", replace
keep bene_id tolte_pdays_2008 tolte_clms_2008 tolte_minfilldt_2008 tolte_totdd_2008 tolte_fdays_2008 tolte_tdd_2008
merge 1:1 bene_id using `2008'
drop _m
save `2008', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2008.dta", replace
keep bene_id trosp_pdays_2008 trosp_clms_2008 trosp_minfilldt_2008 trosp_totdd_2008 trosp_fdays_2008 trosp_tdd_2008
merge 1:1 bene_id using `2008'
drop _m
save `2008', replace

// Organize 2008
gen bam2008 = 1

local ldrugs "fesot"
local vars "pdays clms totdd fdays tdd"
local drugs "darif fesot flavo oxybu solif tolte trosp"
foreach j in `ldrugs'{
	gen `j'_minfilldt_2008 = .
	foreach i in `vars'{
		gen `j'_`i'_2008 = 0
	}
}
foreach d in `drugs'{
	rename `d'_minfilldt_2008 `d'_d_2008
	foreach i in `vars'{
		replace `d'_`i'_2008 = 0 if `d'_`i'_2008==.
	}
}

//	Nonselective antimuscarinics (oxybutynin, tolterodine, flavoxate, hyoscyamine, trospium, fesoterodine, propantheline) 
//	M3 selective antimuscarinics (darifenacin or solifenacin)
egen nsl_d_2008 = rowmin(fesot_d_2008 flavo_d_2008 oxybu_d_2008 tolte_d_2008 trosp_d_2008)
egen nsl2_d_2008 = rowmin(fesot_d_2008 flavo_d_2008 oxybu_d_2008 tolte_d_2008)
egen m3s_d_2008 = rowmin(darif_d_2008 solif_d_2008)
egen bam_d_2008 = rowmin(fesot_d_2008 flavo_d_2008 oxybu_d_2008 tolte_d_2008 trosp_d_2008 darif_d_2008 solif_d_2008) 

foreach i in `vars'{
	gen nsl_`i'_2008 = 0
	gen nsl2_`i'_2008 = 0
	gen m3s_`i'_2008 = 0
	gen bam_`i'_2008 = 0
	replace nsl_`i'_2008 = fesot_`i'_2008 + flavo_`i'_2008 + oxybu_`i'_2008 + tolte_`i'_2008 + trosp_`i'_2008
	replace nsl2_`i'_2008 = fesot_`i'_2008 + flavo_`i'_2008 + oxybu_`i'_2008 + tolte_`i'_2008 
	replace m3s_`i'_2008 = darif_`i'_2008 + solif_`i'_2008 
	replace bam_`i'_2008 = fesot_`i'_2008 + flavo_`i'_2008 + oxybu_`i'_2008 + tolte_`i'_2008 + trosp_`i'_2008 + darif_`i'_2008 + solif_`i'_2008 
}
replace nsl_pdays_2008 = 365 if nsl_pdays_2008>365
replace nsl2_pdays_2008 = 365 if nsl2_pdays_2008>365
replace m3s_pdays_2008 = 365 if m3s_pdays_2008>365
replace bam_pdays_2008 = 365 if bam_pdays_2008>365

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_2008.dta", replace

////////////////////////	2009 		////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2009.dta", replace
keep bene_id darif_pdays_2009 darif_clms_2009 darif_minfilldt_2009 darif_totdd_2009 darif_fdays_2009 darif_tdd_2009
tempfile 2009
save `2009', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/fesot_2009.dta", replace
keep bene_id fesot_pdays_2009 fesot_clms_2009 fesot_minfilldt_2009 fesot_totdd_2009 fesot_fdays_2009 fesot_tdd_2009
merge 1:1 bene_id using `2009'
drop _m
save `2009', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2009.dta", replace
keep bene_id flavo_pdays_2009 flavo_clms_2009 flavo_minfilldt_2009 flavo_totdd_2009 flavo_fdays_2009 flavo_tdd_2009
merge 1:1 bene_id using `2009'
drop _m
save `2009', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2009.dta", replace
keep bene_id oxybu_pdays_2009 oxybu_clms_2009 oxybu_minfilldt_2009 oxybu_totdd_2009 oxybu_fdays_2009 oxybu_tdd_2009
merge 1:1 bene_id using `2009'
drop _m
save `2009', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2009.dta", replace
keep bene_id solif_pdays_2009 solif_clms_2009 solif_minfilldt_2009 solif_totdd_2009 solif_fdays_2009 solif_tdd_2009
merge 1:1 bene_id using `2009'
drop _m
save `2009', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2009.dta", replace
keep bene_id tolte_pdays_2009 tolte_clms_2009 tolte_minfilldt_2009 tolte_totdd_2009 tolte_fdays_2009 tolte_tdd_2009
merge 1:1 bene_id using `2009'
drop _m
save `2009', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2009.dta", replace
keep bene_id trosp_pdays_2009 trosp_clms_2009 trosp_minfilldt_2009 trosp_totdd_2009 trosp_fdays_2009 trosp_tdd_2009
merge 1:1 bene_id using `2009'
drop _m
save `2009', replace

// Organize 2009
gen bam2009 = 1

local vars "pdays clms totdd fdays tdd"
local drugs "darif fesot flavo oxybu solif tolte trosp"
foreach d in `drugs'{
	rename `d'_minfilldt_2009 `d'_d_2009
	foreach i in `vars'{
		replace `d'_`i'_2009 = 0 if `d'_`i'_2009==.
	}
}

//	Nonselective antimuscarinics (oxybutynin, tolterodine, flavoxate, hyoscyamine, trospium, fesoterodine, propantheline) 
//	M3 selective antimuscarinics (darifenacin or solifenacin)
egen nsl_d_2009 = rowmin(fesot_d_2009 flavo_d_2009 oxybu_d_2009 tolte_d_2009 trosp_d_2009)
egen nsl2_d_2009 = rowmin(fesot_d_2009 flavo_d_2009 oxybu_d_2009 tolte_d_2009)
egen m3s_d_2009 = rowmin(darif_d_2009 solif_d_2009)
egen bam_d_2009 = rowmin(fesot_d_2009 flavo_d_2009 oxybu_d_2009 tolte_d_2009 trosp_d_2009 darif_d_2009 solif_d_2009) 

foreach i in `vars'{
	gen nsl_`i'_2009 = 0
	gen nsl2_`i'_2009 = 0
	gen m3s_`i'_2009 = 0
	gen bam_`i'_2009 = 0
	replace nsl_`i'_2009 = fesot_`i'_2009 + flavo_`i'_2009 + oxybu_`i'_2009 + tolte_`i'_2009 + trosp_`i'_2009
	replace nsl2_`i'_2009 = fesot_`i'_2009 + flavo_`i'_2009 + oxybu_`i'_2009 + tolte_`i'_2009 
	replace m3s_`i'_2009 = darif_`i'_2009 + solif_`i'_2009 
	replace bam_`i'_2009 = fesot_`i'_2009 + flavo_`i'_2009 + oxybu_`i'_2009 + tolte_`i'_2009 + trosp_`i'_2009 + darif_`i'_2009 + solif_`i'_2009 
}
replace nsl_pdays_2009 = 365 if nsl_pdays_2009>365
replace nsl2_pdays_2009 = 365 if nsl2_pdays_2009>365
replace m3s_pdays_2009 = 365 if m3s_pdays_2009>365
replace bam_pdays_2009 = 365 if bam_pdays_2009>365

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_2009.dta", replace

////////////////////////	2010 		////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2010.dta", replace
keep bene_id darif_pdays_2010 darif_clms_2010 darif_minfilldt_2010 darif_totdd_2010 darif_fdays_2010 darif_tdd_2010
tempfile 2010
save `2010', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/fesot_2010.dta", replace
keep bene_id fesot_pdays_2010 fesot_clms_2010 fesot_minfilldt_2010 fesot_totdd_2010 fesot_fdays_2010 fesot_tdd_2010
merge 1:1 bene_id using `2010'
drop _m
save `2010', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2010.dta", replace
keep bene_id flavo_pdays_2010 flavo_clms_2010 flavo_minfilldt_2010 flavo_totdd_2010 flavo_fdays_2010 flavo_tdd_2010
merge 1:1 bene_id using `2010'
drop _m
save `2010', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2010.dta", replace
keep bene_id oxybu_pdays_2010 oxybu_clms_2010 oxybu_minfilldt_2010 oxybu_totdd_2010 oxybu_fdays_2010 oxybu_tdd_2010
merge 1:1 bene_id using `2010'
drop _m
save `2010', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2010.dta", replace
keep bene_id solif_pdays_2010 solif_clms_2010 solif_minfilldt_2010 solif_totdd_2010 solif_fdays_2010 solif_tdd_2010
merge 1:1 bene_id using `2010'
drop _m
save `2010', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2010.dta", replace
keep bene_id tolte_pdays_2010 tolte_clms_2010 tolte_minfilldt_2010 tolte_totdd_2010 tolte_fdays_2010 tolte_tdd_2010
merge 1:1 bene_id using `2010'
drop _m
save `2010', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2010.dta", replace
keep bene_id trosp_pdays_2010 trosp_clms_2010 trosp_minfilldt_2010 trosp_totdd_2010 trosp_fdays_2010 trosp_tdd_2010
merge 1:1 bene_id using `2010'
drop _m
save `2010', replace

// Organize 2010
gen bam2010 = 1

local vars "pdays clms totdd fdays tdd"
local drugs "darif fesot flavo oxybu solif tolte trosp"
foreach d in `drugs'{
	rename `d'_minfilldt_2010 `d'_d_2010
	foreach i in `vars'{
		replace `d'_`i'_2010 = 0 if `d'_`i'_2010==.
	}
}

//	Nonselective antimuscarinics (oxybutynin, tolterodine, flavoxate, hyoscyamine, trospium, fesoterodine, propantheline) 
//	M3 selective antimuscarinics (darifenacin or solifenacin)
egen nsl_d_2010 = rowmin(fesot_d_2010 flavo_d_2010 oxybu_d_2010 tolte_d_2010 trosp_d_2010)
egen nsl2_d_2010 = rowmin(fesot_d_2010 flavo_d_2010 oxybu_d_2010 tolte_d_2010)
egen m3s_d_2010 = rowmin(darif_d_2010 solif_d_2010)
egen bam_d_2010 = rowmin(fesot_d_2010 flavo_d_2010 oxybu_d_2010 tolte_d_2010 trosp_d_2010 darif_d_2010 solif_d_2010) 

foreach i in `vars'{
	gen nsl_`i'_2010 = 0
	gen nsl2_`i'_2010 = 0
	gen m3s_`i'_2010 = 0
	gen bam_`i'_2010 = 0
	replace nsl_`i'_2010 = fesot_`i'_2010 + flavo_`i'_2010 + oxybu_`i'_2010 + tolte_`i'_2010 + trosp_`i'_2010
	replace nsl2_`i'_2010 = fesot_`i'_2010 + flavo_`i'_2010 + oxybu_`i'_2010 + tolte_`i'_2010 
	replace m3s_`i'_2010 = darif_`i'_2010 + solif_`i'_2010 
	replace bam_`i'_2010 = fesot_`i'_2010 + flavo_`i'_2010 + oxybu_`i'_2010 + tolte_`i'_2010 + trosp_`i'_2010 + darif_`i'_2010 + solif_`i'_2010 
}
replace nsl_pdays_2010 = 365 if nsl_pdays_2010>365
replace nsl2_pdays_2010 = 365 if nsl2_pdays_2010>365
replace m3s_pdays_2010 = 365 if m3s_pdays_2010>365
replace bam_pdays_2010 = 365 if bam_pdays_2010>365

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_2010.dta", replace

////////////////////////	2011 		////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2011.dta", replace
keep bene_id darif_pdays_2011 darif_clms_2011 darif_minfilldt_2011 darif_totdd_2011 darif_fdays_2011 darif_tdd_2011
tempfile 2011
save `2011', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/fesot_2011.dta", replace
keep bene_id fesot_pdays_2011 fesot_clms_2011 fesot_minfilldt_2011 fesot_totdd_2011 fesot_fdays_2011 fesot_tdd_2011
merge 1:1 bene_id using `2011'
drop _m
save `2011', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2011.dta", replace
keep bene_id flavo_pdays_2011 flavo_clms_2011 flavo_minfilldt_2011 flavo_totdd_2011 flavo_fdays_2011 flavo_tdd_2011
merge 1:1 bene_id using `2011'
drop _m
save `2011', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2011.dta", replace
keep bene_id oxybu_pdays_2011 oxybu_clms_2011 oxybu_minfilldt_2011 oxybu_totdd_2011 oxybu_fdays_2011 oxybu_tdd_2011
merge 1:1 bene_id using `2011'
drop _m
save `2011', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2011.dta", replace
keep bene_id solif_pdays_2011 solif_clms_2011 solif_minfilldt_2011 solif_totdd_2011 solif_fdays_2011 solif_tdd_2011
merge 1:1 bene_id using `2011'
drop _m
save `2011', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2011.dta", replace
keep bene_id tolte_pdays_2011 tolte_clms_2011 tolte_minfilldt_2011 tolte_totdd_2011 tolte_fdays_2011 tolte_tdd_2011
merge 1:1 bene_id using `2011'
drop _m
save `2011', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2011.dta", replace
keep bene_id trosp_pdays_2011 trosp_clms_2011 trosp_minfilldt_2011 trosp_totdd_2011 trosp_fdays_2011 trosp_tdd_2011
merge 1:1 bene_id using `2011'
drop _m
save `2011', replace

// Organize 2011
gen bam2011 = 1

local vars "pdays clms totdd fdays tdd"
local drugs "darif fesot flavo oxybu solif tolte trosp"
foreach d in `drugs'{
	rename `d'_minfilldt_2011 `d'_d_2011
	foreach i in `vars'{
		replace `d'_`i'_2011 = 0 if `d'_`i'_2011==.
	}
}

//	Nonselective antimuscarinics (oxybutynin, tolterodine, flavoxate, hyoscyamine, trospium, fesoterodine, propantheline) 
//	M3 selective antimuscarinics (darifenacin or solifenacin)
egen nsl_d_2011 = rowmin(fesot_d_2011 flavo_d_2011 oxybu_d_2011 tolte_d_2011 trosp_d_2011)
egen nsl2_d_2011 = rowmin(fesot_d_2011 flavo_d_2011 oxybu_d_2011 tolte_d_2011)
egen m3s_d_2011 = rowmin(darif_d_2011 solif_d_2011)
egen bam_d_2011 = rowmin(fesot_d_2011 flavo_d_2011 oxybu_d_2011 tolte_d_2011 trosp_d_2011 darif_d_2011 solif_d_2011) 

foreach i in `vars'{
	gen nsl_`i'_2011 = 0
	gen nsl2_`i'_2011 = 0
	gen m3s_`i'_2011 = 0
	gen bam_`i'_2011 = 0
	replace nsl_`i'_2011 = fesot_`i'_2011 + flavo_`i'_2011 + oxybu_`i'_2011 + tolte_`i'_2011 + trosp_`i'_2011
	replace nsl2_`i'_2011 = fesot_`i'_2011 + flavo_`i'_2011 + oxybu_`i'_2011 + tolte_`i'_2011 
	replace m3s_`i'_2011 = darif_`i'_2011 + solif_`i'_2011 
	replace bam_`i'_2011 = fesot_`i'_2011 + flavo_`i'_2011 + oxybu_`i'_2011 + tolte_`i'_2011 + trosp_`i'_2011 + darif_`i'_2011 + solif_`i'_2011 
}
replace nsl_pdays_2011 = 365 if nsl_pdays_2011>365
replace nsl2_pdays_2011 = 365 if nsl2_pdays_2011>365
replace m3s_pdays_2011 = 365 if m3s_pdays_2011>365
replace bam_pdays_2011 = 365 if bam_pdays_2011>365

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_2011.dta", replace

////////////////////////	2012 		////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2012.dta", replace
keep bene_id darif_pdays_2012 darif_clms_2012 darif_minfilldt_2012 darif_totdd_2012 darif_fdays_2012 darif_tdd_2012
tempfile 2012
save `2012', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/fesot_2012.dta", replace
keep bene_id fesot_pdays_2012 fesot_clms_2012 fesot_minfilldt_2012 fesot_totdd_2012 fesot_fdays_2012 fesot_tdd_2012
merge 1:1 bene_id using `2012'
drop _m
save `2012', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2012.dta", replace
keep bene_id flavo_pdays_2012 flavo_clms_2012 flavo_minfilldt_2012 flavo_totdd_2012 flavo_fdays_2012 flavo_tdd_2012
merge 1:1 bene_id using `2012'
drop _m
save `2012', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2012.dta", replace
keep bene_id oxybu_pdays_2012 oxybu_clms_2012 oxybu_minfilldt_2012 oxybu_totdd_2012 oxybu_fdays_2012 oxybu_tdd_2012
merge 1:1 bene_id using `2012'
drop _m
save `2012', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2012.dta", replace
keep bene_id solif_pdays_2012 solif_clms_2012 solif_minfilldt_2012 solif_totdd_2012 solif_fdays_2012 solif_tdd_2012
merge 1:1 bene_id using `2012'
drop _m
save `2012', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2012.dta", replace
keep bene_id tolte_pdays_2012 tolte_clms_2012 tolte_minfilldt_2012 tolte_totdd_2012 tolte_fdays_2012 tolte_tdd_2012
merge 1:1 bene_id using `2012'
drop _m
save `2012', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2012.dta", replace
keep bene_id trosp_pdays_2012 trosp_clms_2012 trosp_minfilldt_2012 trosp_totdd_2012 trosp_fdays_2012 trosp_tdd_2012
merge 1:1 bene_id using `2012'
drop _m
save `2012', replace

// Organize 2012
gen bam2012 = 1

local vars "pdays clms totdd fdays tdd"
local drugs "darif fesot flavo oxybu solif tolte trosp"
foreach d in `drugs'{
	rename `d'_minfilldt_2012 `d'_d_2012
	foreach i in `vars'{
		replace `d'_`i'_2012 = 0 if `d'_`i'_2012==.
	}
}

//	Nonselective antimuscarinics (oxybutynin, tolterodine, flavoxate, hyoscyamine, trospium, fesoterodine, propantheline) 
//	M3 selective antimuscarinics (darifenacin or solifenacin)
egen nsl_d_2012 = rowmin(fesot_d_2012 flavo_d_2012 oxybu_d_2012 tolte_d_2012 trosp_d_2012)
egen nsl2_d_2012 = rowmin(fesot_d_2012 flavo_d_2012 oxybu_d_2012 tolte_d_2012)
egen m3s_d_2012 = rowmin(darif_d_2012 solif_d_2012)
egen bam_d_2012 = rowmin(fesot_d_2012 flavo_d_2012 oxybu_d_2012 tolte_d_2012 trosp_d_2012 darif_d_2012 solif_d_2012) 

foreach i in `vars'{
	gen nsl_`i'_2012 = 0
	gen nsl2_`i'_2012 = 0
	gen m3s_`i'_2012 = 0
	gen bam_`i'_2012 = 0
	replace nsl_`i'_2012 = fesot_`i'_2012 + flavo_`i'_2012 + oxybu_`i'_2012 + tolte_`i'_2012 + trosp_`i'_2012
	replace nsl2_`i'_2012 = fesot_`i'_2012 + flavo_`i'_2012 + oxybu_`i'_2012 + tolte_`i'_2012 
	replace m3s_`i'_2012 = darif_`i'_2012 + solif_`i'_2012 
	replace bam_`i'_2012 = fesot_`i'_2012 + flavo_`i'_2012 + oxybu_`i'_2012 + tolte_`i'_2012 + trosp_`i'_2012 + darif_`i'_2012 + solif_`i'_2012 
}
replace nsl_pdays_2012 = 365 if nsl_pdays_2012>365
replace nsl2_pdays_2012 = 365 if nsl2_pdays_2012>365
replace m3s_pdays_2012 = 365 if m3s_pdays_2012>365
replace bam_pdays_2012 = 365 if bam_pdays_2012>365

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_2012.dta", replace

////////////////////////	2013 		////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2013.dta", replace
keep bene_id darif_pdays_2013 darif_clms_2013 darif_minfilldt_2013 darif_totdd_2013 darif_fdays_2013 darif_tdd_2013
tempfile 2013
save `2013', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/fesot_2013.dta", replace
keep bene_id fesot_pdays_2013 fesot_clms_2013 fesot_minfilldt_2013 fesot_totdd_2013 fesot_fdays_2013 fesot_tdd_2013
merge 1:1 bene_id using `2013'
drop _m
save `2013', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2013.dta", replace
keep bene_id flavo_pdays_2013 flavo_clms_2013 flavo_minfilldt_2013 flavo_totdd_2013 flavo_fdays_2013 flavo_tdd_2013
merge 1:1 bene_id using `2013'
drop _m
save `2013', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2013.dta", replace
keep bene_id oxybu_pdays_2013 oxybu_clms_2013 oxybu_minfilldt_2013 oxybu_totdd_2013 oxybu_fdays_2013 oxybu_tdd_2013
merge 1:1 bene_id using `2013'
drop _m
save `2013', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2013.dta", replace
keep bene_id solif_pdays_2013 solif_clms_2013 solif_minfilldt_2013 solif_totdd_2013 solif_fdays_2013 solif_tdd_2013
merge 1:1 bene_id using `2013'
drop _m
save `2013', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2013.dta", replace
keep bene_id tolte_pdays_2013 tolte_clms_2013 tolte_minfilldt_2013 tolte_totdd_2013 tolte_fdays_2013 tolte_tdd_2013
merge 1:1 bene_id using `2013'
drop _m
save `2013', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2013.dta", replace
keep bene_id trosp_pdays_2013 trosp_clms_2013 trosp_minfilldt_2013 trosp_totdd_2013 trosp_fdays_2013 trosp_tdd_2013
merge 1:1 bene_id using `2013'
drop _m
save `2013', replace

// Organize 2013
gen bam2013 = 1

local vars "pdays clms totdd fdays tdd"
local drugs "darif fesot flavo oxybu solif tolte trosp"
foreach d in `drugs'{
	rename `d'_minfilldt_2013 `d'_d_2013
	foreach i in `vars'{
		replace `d'_`i'_2013 = 0 if `d'_`i'_2013==.
	}
}

//	Nonselective antimuscarinics (oxybutynin, tolterodine, flavoxate, hyoscyamine, trospium, fesoterodine, propantheline) 
//	M3 selective antimuscarinics (darifenacin or solifenacin)
egen nsl_d_2013 = rowmin(fesot_d_2013 flavo_d_2013 oxybu_d_2013 tolte_d_2013 trosp_d_2013)
egen nsl2_d_2013 = rowmin(fesot_d_2013 flavo_d_2013 oxybu_d_2013 tolte_d_2013)
egen m3s_d_2013 = rowmin(darif_d_2013 solif_d_2013)
egen bam_d_2013 = rowmin(fesot_d_2013 flavo_d_2013 oxybu_d_2013 tolte_d_2013 trosp_d_2013 darif_d_2013 solif_d_2013) 

foreach i in `vars'{
	gen nsl_`i'_2013 = 0
	gen nsl2_`i'_2013 = 0
	gen m3s_`i'_2013 = 0
	gen bam_`i'_2013 = 0
	replace nsl_`i'_2013 = fesot_`i'_2013 + flavo_`i'_2013 + oxybu_`i'_2013 + tolte_`i'_2013 + trosp_`i'_2013
	replace nsl2_`i'_2013 = fesot_`i'_2013 + flavo_`i'_2013 + oxybu_`i'_2013 + tolte_`i'_2013 
	replace m3s_`i'_2013 = darif_`i'_2013 + solif_`i'_2013 
	replace bam_`i'_2013 = fesot_`i'_2013 + flavo_`i'_2013 + oxybu_`i'_2013 + tolte_`i'_2013 + trosp_`i'_2013 + darif_`i'_2013 + solif_`i'_2013 
}
replace nsl_pdays_2013 = 365 if nsl_pdays_2013>365
replace nsl2_pdays_2013 = 365 if nsl2_pdays_2013>365
replace m3s_pdays_2013 = 365 if m3s_pdays_2013>365
replace bam_pdays_2013 = 365 if bam_pdays_2013>365

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_2013.dta", replace

////////////////////////	2014 		////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2014.dta", replace
keep bene_id darif_pdays_2014 darif_clms_2014 darif_minfilldt_2014 darif_totdd_2014 darif_fdays_2014 darif_tdd_2014
tempfile 2014
save `2014', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/fesot_2014.dta", replace
keep bene_id fesot_pdays_2014 fesot_clms_2014 fesot_minfilldt_2014 fesot_totdd_2014 fesot_fdays_2014 fesot_tdd_2014
merge 1:1 bene_id using `2014'
drop _m
save `2014', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2014.dta", replace
keep bene_id flavo_pdays_2014 flavo_clms_2014 flavo_minfilldt_2014 flavo_totdd_2014 flavo_fdays_2014 flavo_tdd_2014
merge 1:1 bene_id using `2014'
drop _m
save `2014', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2014.dta", replace
keep bene_id oxybu_pdays_2014 oxybu_clms_2014 oxybu_minfilldt_2014 oxybu_totdd_2014 oxybu_fdays_2014 oxybu_tdd_2014
merge 1:1 bene_id using `2014'
drop _m
save `2014', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2014.dta", replace
keep bene_id solif_pdays_2014 solif_clms_2014 solif_minfilldt_2014 solif_totdd_2014 solif_fdays_2014 solif_tdd_2014
merge 1:1 bene_id using `2014'
drop _m
save `2014', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2014.dta", replace
keep bene_id tolte_pdays_2014 tolte_clms_2014 tolte_minfilldt_2014 tolte_totdd_2014 tolte_fdays_2014 tolte_tdd_2014
merge 1:1 bene_id using `2014'
drop _m
save `2014', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2014.dta", replace
keep bene_id trosp_pdays_2014 trosp_clms_2014 trosp_minfilldt_2014 trosp_totdd_2014 trosp_fdays_2014 trosp_tdd_2014
merge 1:1 bene_id using `2014'
drop _m
save `2014', replace

// Organize 2014
gen bam2014 = 1

local vars "pdays clms totdd fdays tdd"
local drugs "darif fesot flavo oxybu solif tolte trosp"
foreach d in `drugs'{
	rename `d'_minfilldt_2014 `d'_d_2014
	foreach i in `vars'{
		replace `d'_`i'_2014 = 0 if `d'_`i'_2014==.
	}
}

//	Nonselective antimuscarinics (oxybutynin, tolterodine, flavoxate, hyoscyamine, trospium, fesoterodine, propantheline) 
//	M3 selective antimuscarinics (darifenacin or solifenacin)
egen nsl_d_2014 = rowmin(fesot_d_2014 flavo_d_2014 oxybu_d_2014 tolte_d_2014 trosp_d_2014)
egen nsl2_d_2014 = rowmin(fesot_d_2014 flavo_d_2014 oxybu_d_2014 tolte_d_2014)
egen m3s_d_2014 = rowmin(darif_d_2014 solif_d_2014)
egen bam_d_2014 = rowmin(fesot_d_2014 flavo_d_2014 oxybu_d_2014 tolte_d_2014 trosp_d_2014 darif_d_2014 solif_d_2014) 

foreach i in `vars'{
	gen nsl_`i'_2014 = 0
	gen nsl2_`i'_2014 = 0
	gen m3s_`i'_2014 = 0
	gen bam_`i'_2014 = 0
	replace nsl_`i'_2014 = fesot_`i'_2014 + flavo_`i'_2014 + oxybu_`i'_2014 + tolte_`i'_2014 + trosp_`i'_2014
	replace nsl2_`i'_2014 = fesot_`i'_2014 + flavo_`i'_2014 + oxybu_`i'_2014 + tolte_`i'_2014 
	replace m3s_`i'_2014 = darif_`i'_2014 + solif_`i'_2014 
	replace bam_`i'_2014 = fesot_`i'_2014 + flavo_`i'_2014 + oxybu_`i'_2014 + tolte_`i'_2014 + trosp_`i'_2014 + darif_`i'_2014 + solif_`i'_2014 
}
replace nsl_pdays_2014 = 365 if nsl_pdays_2014>365
replace nsl2_pdays_2014 = 365 if nsl2_pdays_2014>365
replace m3s_pdays_2014 = 365 if m3s_pdays_2014>365
replace bam_pdays_2014 = 365 if bam_pdays_2014>365

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_2014.dta", replace

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
//																			  //
//			Merge the bam files from 06-14 to get wide file				  //
//																			  //
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
//2006
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2006.dta", replace
keep if ptD_allyr=="Y"
keep bene_id
tempfile samp
save `samp', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_2006.dta", replace

count
merge 1:1 bene_id using `samp'
keep if _m==3
drop _m

tempfile bam
save `bam', replace

//2007
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2007.dta", replace
keep if ptD_allyr=="Y"
keep bene_id
tempfile samp
save `samp', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_2007.dta", replace
count
merge 1:1 bene_id using `samp'
keep if _m==3
drop _m

merge 1:1 bene_id using `bam'
drop _m
save `bam', replace

//2008
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2008.dta", replace
keep if ptD_allyr=="Y"
keep bene_id
tempfile samp
save `samp', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_2008.dta", replace
count
merge 1:1 bene_id using `samp'
keep if _m==3
drop _m

merge 1:1 bene_id using `bam'
drop _m
save `bam', replace

//2009
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2009.dta", replace
keep if ptD_allyr=="Y"
keep bene_id
tempfile samp
save `samp', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_2009.dta", replace
count
merge 1:1 bene_id using `samp'
keep if _m==3
drop _m

merge 1:1 bene_id using `bam'
drop _m
save `bam', replace

//2010
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2010.dta", replace
keep if ptD_allyr=="Y"
keep bene_id
tempfile samp
save `samp', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_2010.dta", replace
count
merge 1:1 bene_id using `samp'
keep if _m==3
drop _m

merge 1:1 bene_id using `bam'
drop _m
save `bam', replace

//2011
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2011.dta", replace
keep if ptD_allyr=="Y"
keep bene_id
tempfile samp
save `samp', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_2011.dta", replace
count
merge 1:1 bene_id using `samp'
keep if _m==3
drop _m

merge 1:1 bene_id using `bam'
drop _m
save `bam', replace

//2012
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2012.dta", replace
keep if ptD_allyr=="Y"
keep bene_id
tempfile samp
save `samp', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_2012.dta", replace
count
merge 1:1 bene_id using `samp'
keep if _m==3
drop _m

merge 1:1 bene_id using `bam'
drop _m
save `bam', replace

//2013
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2013.dta", replace
keep if ptD_allyr=="Y"
keep bene_id
tempfile samp
save `samp', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_2013.dta", replace
count
merge 1:1 bene_id using `samp'
keep if _m==3
drop _m

merge 1:1 bene_id using `bam'
drop _m
save `bam', replace

//2014
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2014.dta", replace
keep if ptD_allyr=="Y"
keep bene_id
tempfile samp
save `samp', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_2014.dta", replace
count
merge 1:1 bene_id using `samp'
keep if _m==3
drop _m

merge 1:1 bene_id using `bam'
drop _m

//summary variables
gen year_bammax=.
gen year_bammin=.
forvalues i=2006(1)2014{
	replace year_bammax = `i' if bam`i'==1
}
forvalues i=2014(-1)2006{
	replace year_bammin = `i' if bam`i'==1
}

//don't fill in zeros - if someone has a missing, it's because they weren't observed in that year

//First dates
local vars "pdays clms totdd"
local drugs "bam nsl nsl2 m3s darif fesot flavo oxybu solif tolte trosp"

foreach d in `drugs'{
	egen `d'_d = rowmin(`d'_d_2006 `d'_d_2007 `d'_d_2008 `d'_d_2009 `d'_d_2010 `d'_d_2011 `d'_d_2012 `d'_d_2013 `d'_d_2014) 
	gen `d'_y = year(`d'_d)
}

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_0614.dta", replace
desc 
sum
