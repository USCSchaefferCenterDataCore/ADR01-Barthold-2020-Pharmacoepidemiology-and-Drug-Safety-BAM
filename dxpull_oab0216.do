clear all
set more off
capture log close

/*
•	Input: ipcYYYY.dta, snfcYYYY.dta, opcYYYY.dta, hhacYYYY.dta, carlYYYY.dta, carcYYYY.dta, hoscYYYY.dta, dmecYYYY.dta
•	Output: oabYYYY.dta, 04-13
•	In each year, goes through claims from IP, SNF, OP, HHA, CARL, CARC, HOS, DMEC,
•	Makes bene-level file showing the earliest dx date in that year for:
o		oab (596.51), neurogenic bladder (596.54), other disorder (596.59)
o		dysuria (788.1), UI (788.3), urinary frequency (788.41), urgency of urination (788.63)

RESDAC: https://www.resdac.org/cms-data/file-family/Medicare-Claims

*/
/*
////////////////////////////////////////////////////////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
/////////				2002						////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////
/////////			IP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/ip/2002/ipc2002.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/ipc2002_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile ip
save `ip', replace

////////////////////////////////////////////////////////////////////////////////
/////////			SNF								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/snf/2002/snfc2002.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/snfc2002_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile snf
save `snf', replace

////////////////////////////////////////////////////////////////////////////////
/////////			OP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/op/2002/opc2002.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/opc2002_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile op
save `op', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HHA								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/hha/2002/hhac2002.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/hhac2002_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile hha
save `hha', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Line					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// linedgns
use "/disk/agedisk2/medicare/data/20pct/car/2002/carl2002.dta", replace
keep ehic linedgns sexpndt1
egen idn = group(ehic)

//Dummies
gen icd59651 = 1 if linedgns=="59651"
gen icd59654 = 1 if linedgns=="59654"
gen icd59659 = 1 if linedgns=="59659"
gen icd7881 = 1 if substr(linedgns, 1, 4)=="7881"
gen icd7883 = 1 if substr(linedgns, 1, 4)=="7883"
gen icd7884 = 1 if substr(linedgns, 1, 4)=="7884"
gen icd78863 = 1 if linedgns=="78863"

drop if linedgns=="78832" | linedgns=="78835" | linedgns=="78837" | linedgns=="78838"
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sexpndt1 if icd59651==1
gen cdate_icd59654 = sexpndt1 if icd59654==1
gen cdate_icd59659 = sexpndt1 if icd59659==1
gen cdate_icd7881 = sexpndt1 if icd7881==1
gen cdate_icd7883 = sexpndt1 if icd7883==1
gen cdate_icd7884 = sexpndt1 if icd7884==1
gen cdate_icd78863 = sexpndt1 if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sexpndt1
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/carl2002_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile carl
save `carl', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Claim					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-4] pdgns_cd
use "/disk/agedisk2/medicare/data/20pct/car/2002/carc2002.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/4{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/4{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/carc2002_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile carc
save `carc', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HOS								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/hos/2002/hosc2002.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/hosc2002_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile hos
save `hos', replace

////////////////////////////////////////////////////////////////////////////////
/////////			DME claim						////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-4] pdgns_cd
use "/disk/agedisk2/medicare/data/20pct/dme/2002/dmec2002.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/4{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/4{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/dmec2002_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile dmec
save `dmec', replace

////////////////////////////////////////////////////////////////////////////////
///////////////////////			MERGE AND SAVE			////////////////////////
////////////////////////////////////////////////////////////////////////////////
use `ip', replace
append using `snf'
append using `op'
append using `hha'
append using `carl'
append using `carc'
append using `hos'
append using `dmec'

egen idn = group(bene_id)

egen icd59651_d1 = min(d1_icd59651), by(idn)
egen icd59654_d1 = min(d1_icd59654), by(idn)
egen icd59659_d1 = min(d1_icd59659), by(idn)
egen icd7881_d1 = min(d1_icd7881), by(idn)
egen icd7883_d1 = min(d1_icd7883), by(idn)
egen icd7884_d1 = min(d1_icd7884), by(idn)
egen icd78863_d1 = min(d1_icd78863), by(idn)
drop d1* 
 
xfill icd*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]

desc
sum

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2002.dta", replace

////////////////////////////////////////////////////////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
/////////				2003						////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////
/////////			IP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/ip/2003/ipc2003.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/ipc2003_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile ip
save `ip', replace

////////////////////////////////////////////////////////////////////////////////
/////////			SNF								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/snf/2003/snfc2003.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/snfc2003_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile snf
save `snf', replace

////////////////////////////////////////////////////////////////////////////////
/////////			OP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/op/2003/opc2003.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/opc2003_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile op
save `op', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HHA								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/hha/2003/hhac2003.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/hhac2003_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile hha
save `hha', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Line					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// linedgns
use "/disk/agedisk2/medicare/data/20pct/car/2003/carl2003.dta", replace
keep ehic linedgns sexpndt1
egen idn = group(ehic)

//Dummies
gen icd59651 = 1 if linedgns=="59651"
gen icd59654 = 1 if linedgns=="59654"
gen icd59659 = 1 if linedgns=="59659"
gen icd7881 = 1 if substr(linedgns, 1, 4)=="7881"
gen icd7883 = 1 if substr(linedgns, 1, 4)=="7883"
gen icd7884 = 1 if substr(linedgns, 1, 4)=="7884"
gen icd78863 = 1 if linedgns=="78863"

drop if linedgns=="78832" | linedgns=="78835" | linedgns=="78837" | linedgns=="78838"
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sexpndt1 if icd59651==1
gen cdate_icd59654 = sexpndt1 if icd59654==1
gen cdate_icd59659 = sexpndt1 if icd59659==1
gen cdate_icd7881 = sexpndt1 if icd7881==1
gen cdate_icd7883 = sexpndt1 if icd7883==1
gen cdate_icd7884 = sexpndt1 if icd7884==1
gen cdate_icd78863 = sexpndt1 if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sexpndt1
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/carl2003_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile carl
save `carl', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Claim					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-4] pdgns_cd
use "/disk/agedisk2/medicare/data/20pct/car/2003/carc2003.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/4{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/4{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/carc2003_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile carc
save `carc', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HOS								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/hos/2003/hosc2003.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/hosc2003_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile hos
save `hos', replace

////////////////////////////////////////////////////////////////////////////////
/////////			DME claim						////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-4] pdgns_cd
use "/disk/agedisk2/medicare/data/20pct/dme/2003/dmec2003.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/4{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/4{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/dmec2003_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile dmec
save `dmec', replace

////////////////////////////////////////////////////////////////////////////////
///////////////////////			MERGE AND SAVE			////////////////////////
////////////////////////////////////////////////////////////////////////////////
use `ip', replace
append using `snf'
append using `op'
append using `hha'
append using `carl'
append using `carc'
append using `hos'
append using `dmec'

egen idn = group(bene_id)

egen icd59651_d1 = min(d1_icd59651), by(idn)
egen icd59654_d1 = min(d1_icd59654), by(idn)
egen icd59659_d1 = min(d1_icd59659), by(idn)
egen icd7881_d1 = min(d1_icd7881), by(idn)
egen icd7883_d1 = min(d1_icd7883), by(idn)
egen icd7884_d1 = min(d1_icd7884), by(idn)
egen icd78863_d1 = min(d1_icd78863), by(idn)
drop d1* 
 
xfill icd*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]

desc
sum

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2003.dta", replace


////////////////////////////////////////////////////////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
/////////				2004						////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////
/////////			IP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/ip/2004/ipc2004.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/ipc2004_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile ip
save `ip', replace

////////////////////////////////////////////////////////////////////////////////
/////////			SNF								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/snf/2004/snfc2004.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/snfc2004_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile snf
save `snf', replace

////////////////////////////////////////////////////////////////////////////////
/////////			OP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/op/2004/opc2004.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/opc2004_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile op
save `op', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HHA								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/hha/2004/hhac2004.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/hhac2004_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile hha
save `hha', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Line					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// linedgns
use "/disk/agedisk2/medicare/data/20pct/car/2004/carl2004.dta", replace
keep ehic linedgns sexpndt1
egen idn = group(ehic)

//Dummies
gen icd59651 = 1 if linedgns=="59651"
gen icd59654 = 1 if linedgns=="59654"
gen icd59659 = 1 if linedgns=="59659"
gen icd7881 = 1 if substr(linedgns, 1, 4)=="7881"
gen icd7883 = 1 if substr(linedgns, 1, 4)=="7883"
gen icd7884 = 1 if substr(linedgns, 1, 4)=="7884"
gen icd78863 = 1 if linedgns=="78863"

drop if linedgns=="78832" | linedgns=="78835" | linedgns=="78837" | linedgns=="78838"
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sexpndt1 if icd59651==1
gen cdate_icd59654 = sexpndt1 if icd59654==1
gen cdate_icd59659 = sexpndt1 if icd59659==1
gen cdate_icd7881 = sexpndt1 if icd7881==1
gen cdate_icd7883 = sexpndt1 if icd7883==1
gen cdate_icd7884 = sexpndt1 if icd7884==1
gen cdate_icd78863 = sexpndt1 if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sexpndt1
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/carl2004_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile carl
save `carl', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Claim					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-4] pdgns_cd
use "/disk/agedisk2/medicare/data/20pct/car/2004/carc2004.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/4{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/4{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/carc2004_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile carc
save `carc', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HOS								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/hos/2004/hosc2004.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/hosc2004_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile hos
save `hos', replace

////////////////////////////////////////////////////////////////////////////////
/////////			DME claim						////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-4] pdgns_cd
use "/disk/agedisk2/medicare/data/20pct/dme/2004/dmec2004.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/4{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/4{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/dmec2004_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile dmec
save `dmec', replace

////////////////////////////////////////////////////////////////////////////////
///////////////////////			MERGE AND SAVE			////////////////////////
////////////////////////////////////////////////////////////////////////////////
use `ip', replace
append using `snf'
append using `op'
append using `hha'
append using `carl'
append using `carc'
append using `hos'
append using `dmec'

egen idn = group(bene_id)

egen icd59651_d1 = min(d1_icd59651), by(idn)
egen icd59654_d1 = min(d1_icd59654), by(idn)
egen icd59659_d1 = min(d1_icd59659), by(idn)
egen icd7881_d1 = min(d1_icd7881), by(idn)
egen icd7883_d1 = min(d1_icd7883), by(idn)
egen icd7884_d1 = min(d1_icd7884), by(idn)
egen icd78863_d1 = min(d1_icd78863), by(idn)
drop d1* 
 
xfill icd*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]

desc
sum

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2004.dta", replace


////////////////////////////////////////////////////////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
/////////				2005						////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
////////////////////////////////////////////////////////////////////////////////


////////////////////////////////////////////////////////////////////////////////
/////////			IP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/ip/2005/ipc2005.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/ipc2005_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile ip
save `ip', replace

////////////////////////////////////////////////////////////////////////////////
/////////			SNF								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/snf/2005/snfc2005.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/snfc2005_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile snf
save `snf', replace

////////////////////////////////////////////////////////////////////////////////
/////////			OP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/op/2005/opc2005.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/opc2005_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile op
save `op', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HHA								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/hha/2005/hhac2005.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/hhac2005_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile hha
save `hha', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Line					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// linedgns
use "/disk/agedisk2/medicare/data/20pct/car/2005/carl2005.dta", replace
keep ehic linedgns sexpndt1
egen idn = group(ehic)

//Dummies
gen icd59651 = 1 if linedgns=="59651"
gen icd59654 = 1 if linedgns=="59654"
gen icd59659 = 1 if linedgns=="59659"
gen icd7881 = 1 if substr(linedgns, 1, 4)=="7881"
gen icd7883 = 1 if substr(linedgns, 1, 4)=="7883"
gen icd7884 = 1 if substr(linedgns, 1, 4)=="7884"
gen icd78863 = 1 if linedgns=="78863"

drop if linedgns=="78832" | linedgns=="78835" | linedgns=="78837" | linedgns=="78838"
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sexpndt1 if icd59651==1
gen cdate_icd59654 = sexpndt1 if icd59654==1
gen cdate_icd59659 = sexpndt1 if icd59659==1
gen cdate_icd7881 = sexpndt1 if icd7881==1
gen cdate_icd7883 = sexpndt1 if icd7883==1
gen cdate_icd7884 = sexpndt1 if icd7884==1
gen cdate_icd78863 = sexpndt1 if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sexpndt1
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/carl2005_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile carl
save `carl', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Claim					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-4] pdgns_cd
use "/disk/agedisk2/medicare/data/20pct/car/2005/carc2005.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/4{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/4{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/carc2005_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile carc
save `carc', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HOS								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-10]
// pdgns_cd 

use "/disk/agedisk2/medicare/data/20pct/hos/2005/hosc2005.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/10{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/hosc2005_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile hos
save `hos', replace

////////////////////////////////////////////////////////////////////////////////
/////////			DME claim						////////////////////////////
////////////////////////////////////////////////////////////////////////////////
// dgns_cd[1-4] pdgns_cd
use "/disk/agedisk2/medicare/data/20pct/dme/2005/dmec2005.dta", replace
keep ehic dgns_cd* pdgns_cd sfromdt
egen idn = group(ehic)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/4{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/4{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = sfromdt if icd59651==1
gen cdate_icd59654 = sfromdt if icd59654==1
gen cdate_icd59659 = sfromdt if icd59659==1
gen cdate_icd7881 = sfromdt if icd7881==1
gen cdate_icd7883 = sfromdt if icd7883==1
gen cdate_icd7884 = sfromdt if icd7884==1
gen cdate_icd78863 = sfromdt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort ehic sfromdt
drop if ehic==ehic[_n-1]

keep ehic icd* d1_*
count
sum icd* 

tempfile ehic
save `ehic', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Xwalk2002_2005/dmec2005_xw.dta", replace
keep ehic bene_id
sort ehic
drop if ehic==ehic[_n-1]
merge 1:1 ehic using `ehic'
keep if _m==3
drop _m

tempfile dmec
save `dmec', replace

////////////////////////////////////////////////////////////////////////////////
///////////////////////			MERGE AND SAVE			////////////////////////
////////////////////////////////////////////////////////////////////////////////
use `ip', replace
append using `snf'
append using `op'
append using `hha'
append using `carl'
append using `carc'
append using `hos'
append using `dmec'

egen idn = group(bene_id)

egen icd59651_d1 = min(d1_icd59651), by(idn)
egen icd59654_d1 = min(d1_icd59654), by(idn)
egen icd59659_d1 = min(d1_icd59659), by(idn)
egen icd7881_d1 = min(d1_icd7881), by(idn)
egen icd7883_d1 = min(d1_icd7883), by(idn)
egen icd7884_d1 = min(d1_icd7884), by(idn)
egen icd78863_d1 = min(d1_icd78863), by(idn)
drop d1* 
 
xfill icd*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]

desc
sum

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2005.dta", replace

////////////////////////////////////////////////////////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
/////////				2006						////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
/////////			IP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/ip/2006/ipc2006.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile ip
save `ip', replace

////////////////////////////////////////////////////////////////////////////////
/////////			SNF								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/snf/2006/snfc2006.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile snf
save `snf', replace

////////////////////////////////////////////////////////////////////////////////
/////////			OP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/op/2006/opc2006.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile op
save `op', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HHA								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/hha/2006/hhac2006.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile hha
save `hha', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Line					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/car/2006/carl2006.dta", replace
keep bene_id linedgns expnsdt1
egen idn = group(bene_id)

//Dummies
gen icd59651 = 1 if linedgns=="59651"
gen icd59654 = 1 if linedgns=="59654"
gen icd59659 = 1 if linedgns=="59659"
gen icd7881 = 1 if substr(linedgns, 1, 4)=="7881"
gen icd7883 = 1 if substr(linedgns, 1, 4)=="7883"
gen icd7884 = 1 if substr(linedgns, 1, 4)=="7884"
gen icd78863 = 1 if linedgns=="78863"

drop if linedgns=="78832" | linedgns=="78835" | linedgns=="78837" | linedgns=="78838"
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = expnsdt1 if icd59651==1
gen cdate_icd59654 = expnsdt1 if icd59654==1
gen cdate_icd59659 = expnsdt1 if icd59659==1
gen cdate_icd7881 = expnsdt1 if icd7881==1
gen cdate_icd7883 = expnsdt1 if icd7883==1
gen cdate_icd7884 = expnsdt1 if icd7884==1
gen cdate_icd78863 = expnsdt1 if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id expnsdt1
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile carl
save `carl', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Claim					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/car/2006/carc2006.dta", replace
keep bene_id dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/8{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/8{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile carc
save `carc', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HOS								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/hos/2006/hosc2006.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile hos
save `hos', replace

////////////////////////////////////////////////////////////////////////////////
/////////			DME claim						////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/dme/2006/dmec2006.dta", replace
keep bene_id dgns_cd* from_dt thru_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/8{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/8{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
replace from_dt = thru_dt if from_dt==.		//from_dt has missing values - but only where thru_dt==the year of the file.
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile dmec
save `dmec', replace

////////////////////////////////////////////////////////////////////////////////
///////////////////////			MERGE AND SAVE			////////////////////////
////////////////////////////////////////////////////////////////////////////////
use `ip', replace
append using `snf'
append using `op'
append using `hha'
append using `carl'
append using `carc'
append using `hos'
append using `dmec'

egen idn = group(bene_id)

egen icd59651_d1 = min(d1_icd59651), by(idn)
egen icd59654_d1 = min(d1_icd59654), by(idn)
egen icd59659_d1 = min(d1_icd59659), by(idn)
egen icd7881_d1 = min(d1_icd7881), by(idn)
egen icd7883_d1 = min(d1_icd7883), by(idn)
egen icd7884_d1 = min(d1_icd7884), by(idn)
egen icd78863_d1 = min(d1_icd78863), by(idn)
drop d1* 
 
xfill icd*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]

desc
sum

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2006.dta", replace

////////////////////////////////////////////////////////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
/////////				2007						////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
/////////			IP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/ip/2007/ipc2007.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile ip
save `ip', replace

////////////////////////////////////////////////////////////////////////////////
/////////			SNF								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/snf/2007/snfc2007.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile snf
save `snf', replace

////////////////////////////////////////////////////////////////////////////////
/////////			OP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/op/2007/opc2007.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile op
save `op', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HHA								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/hha/2007/hhac2007.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile hha
save `hha', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Line					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/car/2007/carl2007.dta", replace
keep bene_id linedgns expnsdt1
egen idn = group(bene_id)

//Dummies
gen icd59651 = 1 if linedgns=="59651"
gen icd59654 = 1 if linedgns=="59654"
gen icd59659 = 1 if linedgns=="59659"
gen icd7881 = 1 if substr(linedgns, 1, 4)=="7881"
gen icd7883 = 1 if substr(linedgns, 1, 4)=="7883"
gen icd7884 = 1 if substr(linedgns, 1, 4)=="7884"
gen icd78863 = 1 if linedgns=="78863"

drop if linedgns=="78832" | linedgns=="78835" | linedgns=="78837" | linedgns=="78838"
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = expnsdt1 if icd59651==1
gen cdate_icd59654 = expnsdt1 if icd59654==1
gen cdate_icd59659 = expnsdt1 if icd59659==1
gen cdate_icd7881 = expnsdt1 if icd7881==1
gen cdate_icd7883 = expnsdt1 if icd7883==1
gen cdate_icd7884 = expnsdt1 if icd7884==1
gen cdate_icd78863 = expnsdt1 if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id expnsdt1
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile carl
save `carl', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Claim					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/car/2007/carc2007.dta", replace
keep bene_id dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/8{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/8{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile carc
save `carc', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HOS								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/hos/2007/hosc2007.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile hos
save `hos', replace

////////////////////////////////////////////////////////////////////////////////
/////////			DME claim						////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/dme/2007/dmec2007.dta", replace
keep bene_id dgns_cd* from_dt thru_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/8{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/8{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
replace from_dt = thru_dt if from_dt==.		//from_dt has missing values - but only where thru_dt==the year of the file.
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile dmec
save `dmec', replace

////////////////////////////////////////////////////////////////////////////////
///////////////////////			MERGE AND SAVE			////////////////////////
////////////////////////////////////////////////////////////////////////////////
use `ip', replace
append using `snf'
append using `op'
append using `hha'
append using `carl'
append using `carc'
append using `hos'
append using `dmec'

egen idn = group(bene_id)

egen icd59651_d1 = min(d1_icd59651), by(idn)
egen icd59654_d1 = min(d1_icd59654), by(idn)
egen icd59659_d1 = min(d1_icd59659), by(idn)
egen icd7881_d1 = min(d1_icd7881), by(idn)
egen icd7883_d1 = min(d1_icd7883), by(idn)
egen icd7884_d1 = min(d1_icd7884), by(idn)
egen icd78863_d1 = min(d1_icd78863), by(idn)
drop d1* 
 
xfill icd*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]

desc
sum

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2007.dta", replace

////////////////////////////////////////////////////////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
/////////				2008						////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
/////////			IP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/ip/2008/ipc2008.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile ip
save `ip', replace

////////////////////////////////////////////////////////////////////////////////
/////////			SNF								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/snf/2008/snfc2008.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile snf
save `snf', replace

////////////////////////////////////////////////////////////////////////////////
/////////			OP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/op/2008/opc2008.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile op
save `op', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HHA								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/hha/2008/hhac2008.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile hha
save `hha', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Line					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/car/2008/carl2008.dta", replace
keep bene_id linedgns expnsdt1
egen idn = group(bene_id)

//Dummies
gen icd59651 = 1 if linedgns=="59651"
gen icd59654 = 1 if linedgns=="59654"
gen icd59659 = 1 if linedgns=="59659"
gen icd7881 = 1 if substr(linedgns, 1, 4)=="7881"
gen icd7883 = 1 if substr(linedgns, 1, 4)=="7883"
gen icd7884 = 1 if substr(linedgns, 1, 4)=="7884"
gen icd78863 = 1 if linedgns=="78863"

drop if linedgns=="78832" | linedgns=="78835" | linedgns=="78837" | linedgns=="78838"
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = expnsdt1 if icd59651==1
gen cdate_icd59654 = expnsdt1 if icd59654==1
gen cdate_icd59659 = expnsdt1 if icd59659==1
gen cdate_icd7881 = expnsdt1 if icd7881==1
gen cdate_icd7883 = expnsdt1 if icd7883==1
gen cdate_icd7884 = expnsdt1 if icd7884==1
gen cdate_icd78863 = expnsdt1 if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id expnsdt1
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile carl
save `carl', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Claim					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/car/2008/carc2008.dta", replace
keep bene_id dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/8{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/8{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile carc
save `carc', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HOS								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/hos/2008/hosc2008.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile hos
save `hos', replace

////////////////////////////////////////////////////////////////////////////////
/////////			DME claim						////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/dme/2008/dmec2008.dta", replace
keep bene_id dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/8{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/8{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile dmec
save `dmec', replace

////////////////////////////////////////////////////////////////////////////////
///////////////////////			MERGE AND SAVE			////////////////////////
////////////////////////////////////////////////////////////////////////////////
use `ip', replace
append using `snf'
append using `op'
append using `hha'
append using `carl'
append using `carc'
append using `hos'
append using `dmec'

egen idn = group(bene_id)

egen icd59651_d1 = min(d1_icd59651), by(idn)
egen icd59654_d1 = min(d1_icd59654), by(idn)
egen icd59659_d1 = min(d1_icd59659), by(idn)
egen icd7881_d1 = min(d1_icd7881), by(idn)
egen icd7883_d1 = min(d1_icd7883), by(idn)
egen icd7884_d1 = min(d1_icd7884), by(idn)
egen icd78863_d1 = min(d1_icd78863), by(idn)
drop d1* 
 
xfill icd*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]

desc
sum

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2008.dta", replace

////////////////////////////////////////////////////////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
/////////				2009						////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
/////////			IP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/ip/2009/ipc2009.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile ip
save `ip', replace

////////////////////////////////////////////////////////////////////////////////
/////////			SNF								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/snf/2009/snfc2009.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile snf
save `snf', replace

////////////////////////////////////////////////////////////////////////////////
/////////			OP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/op/2009/opc2009.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile op
save `op', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HHA								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/hha/2009/hhac2009.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile hha
save `hha', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Line					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/car/2009/carl2009.dta", replace
keep bene_id linedgns expnsdt1
egen idn = group(bene_id)

//Dummies
gen icd59651 = 1 if linedgns=="59651"
gen icd59654 = 1 if linedgns=="59654"
gen icd59659 = 1 if linedgns=="59659"
gen icd7881 = 1 if substr(linedgns, 1, 4)=="7881"
gen icd7883 = 1 if substr(linedgns, 1, 4)=="7883"
gen icd7884 = 1 if substr(linedgns, 1, 4)=="7884"
gen icd78863 = 1 if linedgns=="78863"

drop if linedgns=="78832" | linedgns=="78835" | linedgns=="78837" | linedgns=="78838"
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = expnsdt1 if icd59651==1
gen cdate_icd59654 = expnsdt1 if icd59654==1
gen cdate_icd59659 = expnsdt1 if icd59659==1
gen cdate_icd7881 = expnsdt1 if icd7881==1
gen cdate_icd7883 = expnsdt1 if icd7883==1
gen cdate_icd7884 = expnsdt1 if icd7884==1
gen cdate_icd78863 = expnsdt1 if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id expnsdt1
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile carl
save `carl', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Claim					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/car/2009/carc2009.dta", replace
keep bene_id dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/8{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/8{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile carc
save `carc', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HOS								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/hos/2009/hosc2009.dta", replace
keep bene_id dgnscd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/10{
	replace icd59651 = 1 if dgnscd`i'=="59651"
	replace icd59654 = 1 if dgnscd`i'=="59654"
	replace icd59659 = 1 if dgnscd`i'=="59659"
	replace icd7881 = 1 if substr(dgnscd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgnscd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgnscd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgnscd`i'=="78863"
}
forvalues i=1/10{
	drop if dgnscd`i'=="78832" | dgnscd`i'=="78835" | dgnscd`i'=="78837" | dgnscd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile hos
save `hos', replace

////////////////////////////////////////////////////////////////////////////////
/////////			DME claim						////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/dme/2009/dmec2009.dta", replace
keep bene_id dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/8{
	replace icd59651 = 1 if dgns_cd`i'=="59651"
	replace icd59654 = 1 if dgns_cd`i'=="59654"
	replace icd59659 = 1 if dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if dgns_cd`i'=="78863"
}
forvalues i=1/8{
	drop if dgns_cd`i'=="78832" | dgns_cd`i'=="78835" | dgns_cd`i'=="78837" | dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd* d1_*
count
sum icd* 

tempfile dmec
save `dmec', replace

////////////////////////////////////////////////////////////////////////////////
///////////////////////			MERGE AND SAVE			////////////////////////
////////////////////////////////////////////////////////////////////////////////
use `ip', replace
append using `snf'
append using `op'
append using `hha'
append using `carl'
append using `carc'
append using `hos'
append using `dmec'

egen idn = group(bene_id)

egen icd59651_d1 = min(d1_icd59651), by(idn)
egen icd59654_d1 = min(d1_icd59654), by(idn)
egen icd59659_d1 = min(d1_icd59659), by(idn)
egen icd7881_d1 = min(d1_icd7881), by(idn)
egen icd7883_d1 = min(d1_icd7883), by(idn)
egen icd7884_d1 = min(d1_icd7884), by(idn)
egen icd78863_d1 = min(d1_icd78863), by(idn)
drop d1* 
 
xfill icd*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]

desc
sum

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2009.dta", replace

////////////////////////////////////////////////////////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
/////////				2010						////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
/////////			IP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/ip/2010/ipc2010.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile ip
save `ip', replace

////////////////////////////////////////////////////////////////////////////////
/////////			SNF								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/snf/2010/snfc2010.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile snf
save `snf', replace

////////////////////////////////////////////////////////////////////////////////
/////////			OP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/op/2010/opc2010.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile op
save `op', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HHA								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/hha/2010/hhac2010.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile hha
save `hha', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Line					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/car/2010/carl2010.dta", replace
keep bene_id line_icd_dgns_cd expnsdt1
egen idn = group(bene_id)

//Dummies
gen icd59651 = 1 if line_icd_dgns_cd=="59651"
gen icd59654 = 1 if line_icd_dgns_cd=="59654"
gen icd59659 = 1 if line_icd_dgns_cd=="59659"
gen icd7881 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7881"
gen icd7883 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7883"
gen icd7884 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7884"
gen icd78863 = 1 if line_icd_dgns_cd=="78863"

drop if line_icd_dgns_cd=="78832" | line_icd_dgns_cd=="78835" | line_icd_dgns_cd=="78837" | line_icd_dgns_cd=="78838"
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = expnsdt1 if icd59651==1
gen cdate_icd59654 = expnsdt1 if icd59654==1
gen cdate_icd59659 = expnsdt1 if icd59659==1
gen cdate_icd7881 = expnsdt1 if icd7881==1
gen cdate_icd7883 = expnsdt1 if icd7883==1
gen cdate_icd7884 = expnsdt1 if icd7884==1
gen cdate_icd78863 = expnsdt1 if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id expnsdt1
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile carl
save `carl', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Claim					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/car/2010/carc2010.dta", replace
keep bene_id icd_dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/12{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/12{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile carc
save `carc', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HOS								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/hos/2010/hosc2010.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile hos
save `hos', replace

////////////////////////////////////////////////////////////////////////////////
/////////			DME claim						////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/dme/2010/dmec2010.dta", replace
keep bene_id icd_dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/12{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/12{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile dmec
save `dmec', replace

////////////////////////////////////////////////////////////////////////////////
///////////////////////			MERGE AND SAVE			////////////////////////
////////////////////////////////////////////////////////////////////////////////
use `ip', replace
append using `snf'
append using `op'
append using `hha'
append using `carl'
append using `carc'
append using `hos'
append using `dmec'

egen idn = group(bene_id)

egen icd59651_d1 = min(d1_icd59651), by(idn)
egen icd59654_d1 = min(d1_icd59654), by(idn)
egen icd59659_d1 = min(d1_icd59659), by(idn)
egen icd7881_d1 = min(d1_icd7881), by(idn)
egen icd7883_d1 = min(d1_icd7883), by(idn)
egen icd7884_d1 = min(d1_icd7884), by(idn)
egen icd78863_d1 = min(d1_icd78863), by(idn)
drop d1* 
 
xfill icd5* icd7*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]

desc
sum

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2010.dta", replace

////////////////////////////////////////////////////////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
/////////				2011						////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
/////////			IP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/ip/2011/ipc2011.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile ip
save `ip', replace

////////////////////////////////////////////////////////////////////////////////
/////////			SNF								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/snf/2011/snfc2011.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile snf
save `snf', replace

////////////////////////////////////////////////////////////////////////////////
/////////			OP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/op/2011/opc2011.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile op
save `op', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HHA								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/hha/2011/hhac2011.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile hha
save `hha', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Line					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/car/2011/carl2011.dta", replace
keep bene_id line_icd_dgns_cd expnsdt1
egen idn = group(bene_id)

//Dummies
gen icd59651 = 1 if line_icd_dgns_cd=="59651"
gen icd59654 = 1 if line_icd_dgns_cd=="59654"
gen icd59659 = 1 if line_icd_dgns_cd=="59659"
gen icd7881 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7881"
gen icd7883 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7883"
gen icd7884 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7884"
gen icd78863 = 1 if line_icd_dgns_cd=="78863"

drop if line_icd_dgns_cd=="78832" | line_icd_dgns_cd=="78835" | line_icd_dgns_cd=="78837" | line_icd_dgns_cd=="78838"
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = expnsdt1 if icd59651==1
gen cdate_icd59654 = expnsdt1 if icd59654==1
gen cdate_icd59659 = expnsdt1 if icd59659==1
gen cdate_icd7881 = expnsdt1 if icd7881==1
gen cdate_icd7883 = expnsdt1 if icd7883==1
gen cdate_icd7884 = expnsdt1 if icd7884==1
gen cdate_icd78863 = expnsdt1 if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id expnsdt1
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile carl
save `carl', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Claim					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/car/2011/carc2011.dta", replace
keep bene_id icd_dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/12{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/12{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile carc
save `carc', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HOS								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/hos/2011/hosc2011.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile hos
save `hos', replace

////////////////////////////////////////////////////////////////////////////////
/////////			DME claim						////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/dme/2011/dmec2011.dta", replace
keep bene_id icd_dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/12{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/12{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile dmec
save `dmec', replace

////////////////////////////////////////////////////////////////////////////////
///////////////////////			MERGE AND SAVE			////////////////////////
////////////////////////////////////////////////////////////////////////////////
use `ip', replace
append using `snf'
append using `op'
append using `hha'
append using `carl'
append using `carc'
append using `hos'
append using `dmec'

egen idn = group(bene_id)

egen icd59651_d1 = min(d1_icd59651), by(idn)
egen icd59654_d1 = min(d1_icd59654), by(idn)
egen icd59659_d1 = min(d1_icd59659), by(idn)
egen icd7881_d1 = min(d1_icd7881), by(idn)
egen icd7883_d1 = min(d1_icd7883), by(idn)
egen icd7884_d1 = min(d1_icd7884), by(idn)
egen icd78863_d1 = min(d1_icd78863), by(idn)
drop d1* 
 
xfill icd5* icd7*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]

desc
sum

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2011.dta", replace

////////////////////////////////////////////////////////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
/////////				2012						////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
/////////			IP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/ip/2012/ipc2012.dta", clear
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile ip
save `ip', replace

////////////////////////////////////////////////////////////////////////////////
/////////			SNF								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/snf/2012/snfc2012.dta", clear
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile snf
save `snf', replace

////////////////////////////////////////////////////////////////////////////////
/////////			OP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/op/2012/opc2012.dta", clear
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile op
save `op', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HHA								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/hha/2012/hhac2012.dta", clear
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile hha
save `hha', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Line					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/car/2012/carl2012.dta", clear
keep bene_id line_icd_dgns_cd expnsdt1
egen idn = group(bene_id)

//Dummies
gen icd59651 = 1 if line_icd_dgns_cd=="59651"
gen icd59654 = 1 if line_icd_dgns_cd=="59654"
gen icd59659 = 1 if line_icd_dgns_cd=="59659"
gen icd7881 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7881"
gen icd7883 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7883"
gen icd7884 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7884"
gen icd78863 = 1 if line_icd_dgns_cd=="78863"

drop if line_icd_dgns_cd=="78832" | line_icd_dgns_cd=="78835" | line_icd_dgns_cd=="78837" | line_icd_dgns_cd=="78838"
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = expnsdt1 if icd59651==1
gen cdate_icd59654 = expnsdt1 if icd59654==1
gen cdate_icd59659 = expnsdt1 if icd59659==1
gen cdate_icd7881 = expnsdt1 if icd7881==1
gen cdate_icd7883 = expnsdt1 if icd7883==1
gen cdate_icd7884 = expnsdt1 if icd7884==1
gen cdate_icd78863 = expnsdt1 if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id expnsdt1
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile carl
save `carl', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Claim					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/car/2012/carc2012.dta", clear
keep bene_id icd_dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/12{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/12{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile carc
save `carc', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HOS								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/hos/2012/hosc2012.dta", clear
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile hos
save `hos', replace

////////////////////////////////////////////////////////////////////////////////
/////////			DME claim						////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/dme/2012/dmec2012.dta", clear
keep bene_id icd_dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/12{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/12{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile dmec
save `dmec', replace

////////////////////////////////////////////////////////////////////////////////
///////////////////////			MERGE AND SAVE			////////////////////////
////////////////////////////////////////////////////////////////////////////////
use `ip', replace
append using `snf'
append using `op'
append using `hha'
append using `carl'
append using `carc'
append using `hos'
append using `dmec'

egen idn = group(bene_id)

egen icd59651_d1 = min(d1_icd59651), by(idn)
egen icd59654_d1 = min(d1_icd59654), by(idn)
egen icd59659_d1 = min(d1_icd59659), by(idn)
egen icd7881_d1 = min(d1_icd7881), by(idn)
egen icd7883_d1 = min(d1_icd7883), by(idn)
egen icd7884_d1 = min(d1_icd7884), by(idn)
egen icd78863_d1 = min(d1_icd78863), by(idn)
drop d1* 
 
xfill icd5* icd7*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]

desc
sum

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2012.dta", replace

////////////////////////////////////////////////////////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
/////////				2013						////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
/////////			IP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare/data/20pct/ip/2013/ipc2013.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile ip
save `ip', replace

////////////////////////////////////////////////////////////////////////////////
/////////			SNF								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare/data/20pct/snf/2013/snfc2013.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile snf
save `snf', replace

////////////////////////////////////////////////////////////////////////////////
/////////			OP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare/data/20pct/op/2013/opc2013.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile op
save `op', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HHA								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare/data/20pct/hha/2013/hhac2013.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile hha
save `hha', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Line					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare/data/20pct/car/2013/carl2013.dta", replace
keep bene_id line_icd_dgns_cd expnsdt1
egen idn = group(bene_id)

//Dummies
gen icd59651 = 1 if line_icd_dgns_cd=="59651"
gen icd59654 = 1 if line_icd_dgns_cd=="59654"
gen icd59659 = 1 if line_icd_dgns_cd=="59659"
gen icd7881 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7881"
gen icd7883 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7883"
gen icd7884 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7884"
gen icd78863 = 1 if line_icd_dgns_cd=="78863"

drop if line_icd_dgns_cd=="78832" | line_icd_dgns_cd=="78835" | line_icd_dgns_cd=="78837" | line_icd_dgns_cd=="78838"
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = expnsdt1 if icd59651==1
gen cdate_icd59654 = expnsdt1 if icd59654==1
gen cdate_icd59659 = expnsdt1 if icd59659==1
gen cdate_icd7881 = expnsdt1 if icd7881==1
gen cdate_icd7883 = expnsdt1 if icd7883==1
gen cdate_icd7884 = expnsdt1 if icd7884==1
gen cdate_icd78863 = expnsdt1 if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id expnsdt1
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile carl
save `carl', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Claim					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare/data/20pct/car/2013/carc2013.dta", replace
keep bene_id icd_dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/12{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/12{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile carc
save `carc', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HOS								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare/data/20pct/hos/2013/hosc2013.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile hos
save `hos', replace

////////////////////////////////////////////////////////////////////////////////
/////////			DME claim						////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare/data/20pct/dme/2013/dmec2013.dta", replace
keep bene_id icd_dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/12{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/12{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile dmec
save `dmec', replace

////////////////////////////////////////////////////////////////////////////////
///////////////////////			MERGE AND SAVE			////////////////////////
////////////////////////////////////////////////////////////////////////////////
use `ip', replace
append using `snf'
append using `op'
append using `hha'
append using `carl'
append using `carc'
append using `hos'
append using `dmec'

egen idn = group(bene_id)

egen icd59651_d1 = min(d1_icd59651), by(idn)
egen icd59654_d1 = min(d1_icd59654), by(idn)
egen icd59659_d1 = min(d1_icd59659), by(idn)
egen icd7881_d1 = min(d1_icd7881), by(idn)
egen icd7883_d1 = min(d1_icd7883), by(idn)
egen icd7884_d1 = min(d1_icd7884), by(idn)
egen icd78863_d1 = min(d1_icd78863), by(idn)
drop d1* 
 
xfill icd5* icd7*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]

desc
sum

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2013.dta", replace

////////////////////////////////////////////////////////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
/////////				2014						////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
/////////			IP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/ip/2014/ipc2014.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile ip
save `ip', replace

////////////////////////////////////////////////////////////////////////////////
/////////			SNF								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/snf/2014/snfc2014.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile snf
save `snf', replace

////////////////////////////////////////////////////////////////////////////////
/////////			OP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/op/2014/opc2014.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile op
save `op', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HHA								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/hha/2014/hhac2014.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile hha
save `hha', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Line					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/car/2014/carl2014.dta", replace
keep bene_id line_icd_dgns_cd expnsdt1
egen idn = group(bene_id)

//Dummies
gen icd59651 = 1 if line_icd_dgns_cd=="59651"
gen icd59654 = 1 if line_icd_dgns_cd=="59654"
gen icd59659 = 1 if line_icd_dgns_cd=="59659"
gen icd7881 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7881"
gen icd7883 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7883"
gen icd7884 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7884"
gen icd78863 = 1 if line_icd_dgns_cd=="78863"

drop if line_icd_dgns_cd=="78832" | line_icd_dgns_cd=="78835" | line_icd_dgns_cd=="78837" | line_icd_dgns_cd=="78838"
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = expnsdt1 if icd59651==1
gen cdate_icd59654 = expnsdt1 if icd59654==1
gen cdate_icd59659 = expnsdt1 if icd59659==1
gen cdate_icd7881 = expnsdt1 if icd7881==1
gen cdate_icd7883 = expnsdt1 if icd7883==1
gen cdate_icd7884 = expnsdt1 if icd7884==1
gen cdate_icd78863 = expnsdt1 if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id expnsdt1
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile carl
save `carl', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Claim					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/car/2014/carc2014.dta", replace
keep bene_id icd_dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/12{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/12{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile carc
save `carc', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HOS								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/hos/2014/hosc2014.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile hos
save `hos', replace

////////////////////////////////////////////////////////////////////////////////
/////////			DME claim						////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/dme/2014/dmec2014.dta", replace
keep bene_id icd_dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/12{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651"
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654"
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659"
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881"
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
}
forvalues i=1/12{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile dmec
save `dmec', replace

////////////////////////////////////////////////////////////////////////////////
///////////////////////			MERGE AND SAVE			////////////////////////
////////////////////////////////////////////////////////////////////////////////
use `ip', replace
append using `snf'
append using `op'
append using `hha'
append using `carl'
append using `carc'
append using `hos'
append using `dmec'

egen idn = group(bene_id)

egen icd59651_d1 = min(d1_icd59651), by(idn)
egen icd59654_d1 = min(d1_icd59654), by(idn)
egen icd59659_d1 = min(d1_icd59659), by(idn)
egen icd7881_d1 = min(d1_icd7881), by(idn)
egen icd7883_d1 = min(d1_icd7883), by(idn)
egen icd7884_d1 = min(d1_icd7884), by(idn)
egen icd78863_d1 = min(d1_icd78863), by(idn)
drop d1* 
 
xfill icd5* icd7*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]

desc
sum

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2014.dta", replace


////////////////////////////////////////////////////////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
/////////				2015						////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
/////////			IP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/ip/2015/ipc2015.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651" 
		replace icd59651 = 1 if icd_dgns_cd`i'=="N3281"  
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654" 
		replace icd59654 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659" 
		replace icd59659 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881" 
		replace icd7881 = 1 if icd_dgns_cd`i'=="R300" | icd_dgns_cd`i'=="R309"  
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
		replace icd7883 = 1 if icd_dgns_cd`i'=="R32" | icd_dgns_cd`i'=="N3941" | icd_dgns_cd`i'=="N3946" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3944" | icd_dgns_cd`i'=="N39491" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3948"                    
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
		replace icd7884 = 1 if icd_dgns_cd`i'=="R350" | icd_dgns_cd`i'=="R358" | icd_dgns_cd`i'=="R352" 
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
		replace icd78863 = 1 if icd_dgns_cd`i'=="R3915"  
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile ip
save `ip', replace

////////////////////////////////////////////////////////////////////////////////
/////////			SNF								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/snf/2015/snfc2015.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651" 
		replace icd59651 = 1 if icd_dgns_cd`i'=="N3281"  
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654" 
		replace icd59654 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659" 
		replace icd59659 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881" 
		replace icd7881 = 1 if icd_dgns_cd`i'=="R300" | icd_dgns_cd`i'=="R309"  
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
		replace icd7883 = 1 if icd_dgns_cd`i'=="R32" | icd_dgns_cd`i'=="N3941" | icd_dgns_cd`i'=="N3946" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3944" | icd_dgns_cd`i'=="N39491" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3948"                    
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
		replace icd7884 = 1 if icd_dgns_cd`i'=="R350" | icd_dgns_cd`i'=="R358" | icd_dgns_cd`i'=="R352" 
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
		replace icd78863 = 1 if icd_dgns_cd`i'=="R3915"  
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile snf
save `snf', replace

////////////////////////////////////////////////////////////////////////////////
/////////			OP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/op/2015/opc2015.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651" 
		replace icd59651 = 1 if icd_dgns_cd`i'=="N3281"  
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654" 
		replace icd59654 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659" 
		replace icd59659 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881" 
		replace icd7881 = 1 if icd_dgns_cd`i'=="R300" | icd_dgns_cd`i'=="R309"  
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
		replace icd7883 = 1 if icd_dgns_cd`i'=="R32" | icd_dgns_cd`i'=="N3941" | icd_dgns_cd`i'=="N3946" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3944" | icd_dgns_cd`i'=="N39491" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3948"                    
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
		replace icd7884 = 1 if icd_dgns_cd`i'=="R350" | icd_dgns_cd`i'=="R358" | icd_dgns_cd`i'=="R352" 
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
		replace icd78863 = 1 if icd_dgns_cd`i'=="R3915"  
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile op
save `op', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HHA								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/hha/2015/hhac2015.dta", replace
keep bene_id icd_dgns_* clm_from_dt
rename clm_from_dt from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651" 
		replace icd59651 = 1 if icd_dgns_cd`i'=="N3281"  
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654" 
		replace icd59654 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659" 
		replace icd59659 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881" 
		replace icd7881 = 1 if icd_dgns_cd`i'=="R300" | icd_dgns_cd`i'=="R309"  
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
		replace icd7883 = 1 if icd_dgns_cd`i'=="R32" | icd_dgns_cd`i'=="N3941" | icd_dgns_cd`i'=="N3946" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3944" | icd_dgns_cd`i'=="N39491" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3948"                    
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
		replace icd7884 = 1 if icd_dgns_cd`i'=="R350" | icd_dgns_cd`i'=="R358" | icd_dgns_cd`i'=="R352" 
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
		replace icd78863 = 1 if icd_dgns_cd`i'=="R3915"  
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile hha
save `hha', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Line					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/car/2015/carl2015.dta", replace
keep bene_id line_icd_dgns_cd expnsdt1
egen idn = group(bene_id)

//Dummies
	gen icd59651 = 1 if line_icd_dgns_cd=="59651" 
		replace icd59651 = 1 if line_icd_dgns_cd=="N3281"  
	gen icd59654 = 1 if line_icd_dgns_cd=="59654" 
		replace icd59654 = 1 if line_icd_dgns_cd=="N319"  
	gen icd59659 = 1 if line_icd_dgns_cd=="59659" 
		replace icd59659 = 1 if line_icd_dgns_cd=="N319"  
	gen icd7881 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7881" 
		replace icd7881 = 1 if line_icd_dgns_cd=="R300" | line_icd_dgns_cd=="R309"  
	gen icd7883 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7883"
		replace icd7883 = 1 if line_icd_dgns_cd=="R32" | line_icd_dgns_cd=="N3941" | line_icd_dgns_cd=="N3946" | line_icd_dgns_cd=="N3942" | line_icd_dgns_cd=="N3944" | line_icd_dgns_cd=="N39491" | line_icd_dgns_cd=="N3942" | line_icd_dgns_cd=="N3948"                    
	gen icd7884 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7884"
		replace icd7884 = 1 if line_icd_dgns_cd=="R350" | line_icd_dgns_cd=="R358" | line_icd_dgns_cd=="R352" 
	gen icd78863 = 1 if line_icd_dgns_cd=="78863"
		replace icd78863 = 1 if line_icd_dgns_cd=="R3915"  

drop if line_icd_dgns_cd=="78832" | line_icd_dgns_cd=="78835" | line_icd_dgns_cd=="78837" | line_icd_dgns_cd=="78838"
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = expnsdt1 if icd59651==1
gen cdate_icd59654 = expnsdt1 if icd59654==1
gen cdate_icd59659 = expnsdt1 if icd59659==1
gen cdate_icd7881 = expnsdt1 if icd7881==1
gen cdate_icd7883 = expnsdt1 if icd7883==1
gen cdate_icd7884 = expnsdt1 if icd7884==1
gen cdate_icd78863 = expnsdt1 if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id expnsdt1
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile carl
save `carl', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Claim					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/car/2015/carc2015.dta", replace
keep bene_id icd_dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/12{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651" 
		replace icd59651 = 1 if icd_dgns_cd`i'=="N3281"  
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654" 
		replace icd59654 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659" 
		replace icd59659 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881" 
		replace icd7881 = 1 if icd_dgns_cd`i'=="R300" | icd_dgns_cd`i'=="R309"  
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
		replace icd7883 = 1 if icd_dgns_cd`i'=="R32" | icd_dgns_cd`i'=="N3941" | icd_dgns_cd`i'=="N3946" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3944" | icd_dgns_cd`i'=="N39491" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3948"                    
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
		replace icd7884 = 1 if icd_dgns_cd`i'=="R350" | icd_dgns_cd`i'=="R358" | icd_dgns_cd`i'=="R352" 
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
		replace icd78863 = 1 if icd_dgns_cd`i'=="R3915"  
}
forvalues i=1/12{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile carc
save `carc', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HOS								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/hos/2015/hosc2015.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651" 
		replace icd59651 = 1 if icd_dgns_cd`i'=="N3281"  
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654" 
		replace icd59654 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659" 
		replace icd59659 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881" 
		replace icd7881 = 1 if icd_dgns_cd`i'=="R300" | icd_dgns_cd`i'=="R309"  
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
		replace icd7883 = 1 if icd_dgns_cd`i'=="R32" | icd_dgns_cd`i'=="N3941" | icd_dgns_cd`i'=="N3946" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3944" | icd_dgns_cd`i'=="N39491" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3948"                    
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
		replace icd7884 = 1 if icd_dgns_cd`i'=="R350" | icd_dgns_cd`i'=="R358" | icd_dgns_cd`i'=="R352" 
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
		replace icd78863 = 1 if icd_dgns_cd`i'=="R3915"  
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile hos
save `hos', replace

////////////////////////////////////////////////////////////////////////////////
/////////			DME claim						////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/dme/2015/dmec2015.dta", replace
keep bene_id icd_dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/12{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651" 
		replace icd59651 = 1 if icd_dgns_cd`i'=="N3281"  
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654" 
		replace icd59654 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659" 
		replace icd59659 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881" 
		replace icd7881 = 1 if icd_dgns_cd`i'=="R300" | icd_dgns_cd`i'=="R309"  
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
		replace icd7883 = 1 if icd_dgns_cd`i'=="R32" | icd_dgns_cd`i'=="N3941" | icd_dgns_cd`i'=="N3946" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3944" | icd_dgns_cd`i'=="N39491" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3948"                    
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
		replace icd7884 = 1 if icd_dgns_cd`i'=="R350" | icd_dgns_cd`i'=="R358" | icd_dgns_cd`i'=="R352" 
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
		replace icd78863 = 1 if icd_dgns_cd`i'=="R3915"  
}
forvalues i=1/12{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile dmec
save `dmec', replace

////////////////////////////////////////////////////////////////////////////////
///////////////////////			MERGE AND SAVE			////////////////////////
////////////////////////////////////////////////////////////////////////////////
use `ip', replace
append using `snf'
append using `op'
append using `hha'
append using `carl'
append using `carc'
append using `hos'
append using `dmec'

egen idn = group(bene_id)

egen icd59651_d1 = min(d1_icd59651), by(idn)
egen icd59654_d1 = min(d1_icd59654), by(idn)
egen icd59659_d1 = min(d1_icd59659), by(idn)
egen icd7881_d1 = min(d1_icd7881), by(idn)
egen icd7883_d1 = min(d1_icd7883), by(idn)
egen icd7884_d1 = min(d1_icd7884), by(idn)
egen icd78863_d1 = min(d1_icd78863), by(idn)
drop d1* 
 
xfill icd5* icd7*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]

desc
sum

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2015.dta", replace
*/
////////////////////////////////////////////////////////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
/////////				2016						////////////////////////////
/////////											////////////////////////////
/////////											////////////////////////////
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
/////////			IP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/ip/2016/ipc2016.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651" 
		replace icd59651 = 1 if icd_dgns_cd`i'=="N3281"  
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654" 
		replace icd59654 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659" 
		replace icd59659 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881" 
		replace icd7881 = 1 if icd_dgns_cd`i'=="R300" | icd_dgns_cd`i'=="R309"  
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
		replace icd7883 = 1 if icd_dgns_cd`i'=="R32" | icd_dgns_cd`i'=="N3941" | icd_dgns_cd`i'=="N3946" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3944" | icd_dgns_cd`i'=="N39491" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3948"                    
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
		replace icd7884 = 1 if icd_dgns_cd`i'=="R350" | icd_dgns_cd`i'=="R358" | icd_dgns_cd`i'=="R352" 
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
		replace icd78863 = 1 if icd_dgns_cd`i'=="R3915"  
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile ip
save `ip', replace

////////////////////////////////////////////////////////////////////////////////
/////////			SNF								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/snf/2016/snfc2016.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651" 
		replace icd59651 = 1 if icd_dgns_cd`i'=="N3281"  
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654" 
		replace icd59654 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659" 
		replace icd59659 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881" 
		replace icd7881 = 1 if icd_dgns_cd`i'=="R300" | icd_dgns_cd`i'=="R309"  
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
		replace icd7883 = 1 if icd_dgns_cd`i'=="R32" | icd_dgns_cd`i'=="N3941" | icd_dgns_cd`i'=="N3946" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3944" | icd_dgns_cd`i'=="N39491" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3948"                    
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
		replace icd7884 = 1 if icd_dgns_cd`i'=="R350" | icd_dgns_cd`i'=="R358" | icd_dgns_cd`i'=="R352" 
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
		replace icd78863 = 1 if icd_dgns_cd`i'=="R3915"  
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile snf
save `snf', replace

////////////////////////////////////////////////////////////////////////////////
/////////			OP								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/op/2016/opc2016.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651" 
		replace icd59651 = 1 if icd_dgns_cd`i'=="N3281"  
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654" 
		replace icd59654 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659" 
		replace icd59659 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881" 
		replace icd7881 = 1 if icd_dgns_cd`i'=="R300" | icd_dgns_cd`i'=="R309"  
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
		replace icd7883 = 1 if icd_dgns_cd`i'=="R32" | icd_dgns_cd`i'=="N3941" | icd_dgns_cd`i'=="N3946" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3944" | icd_dgns_cd`i'=="N39491" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3948"                    
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
		replace icd7884 = 1 if icd_dgns_cd`i'=="R350" | icd_dgns_cd`i'=="R358" | icd_dgns_cd`i'=="R352" 
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
		replace icd78863 = 1 if icd_dgns_cd`i'=="R3915"  
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile op
save `op', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HHA								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/hha/2016/hhac2016.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651" 
		replace icd59651 = 1 if icd_dgns_cd`i'=="N3281"  
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654" 
		replace icd59654 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659" 
		replace icd59659 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881" 
		replace icd7881 = 1 if icd_dgns_cd`i'=="R300" | icd_dgns_cd`i'=="R309"  
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
		replace icd7883 = 1 if icd_dgns_cd`i'=="R32" | icd_dgns_cd`i'=="N3941" | icd_dgns_cd`i'=="N3946" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3944" | icd_dgns_cd`i'=="N39491" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3948"                    
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
		replace icd7884 = 1 if icd_dgns_cd`i'=="R350" | icd_dgns_cd`i'=="R358" | icd_dgns_cd`i'=="R352" 
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
		replace icd78863 = 1 if icd_dgns_cd`i'=="R3915"  
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile hha
save `hha', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Line					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/car/2016/carl2016.dta", replace
keep bene_id line_icd_dgns_cd expnsdt1
egen idn = group(bene_id)

//Dummies
	gen icd59651 = 1 if line_icd_dgns_cd=="59651" 
		replace icd59651 = 1 if line_icd_dgns_cd=="N3281"  
	gen icd59654 = 1 if line_icd_dgns_cd=="59654" 
		replace icd59654 = 1 if line_icd_dgns_cd=="N319"  
	gen icd59659 = 1 if line_icd_dgns_cd=="59659" 
		replace icd59659 = 1 if line_icd_dgns_cd=="N319"  
	gen icd7881 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7881" 
		replace icd7881 = 1 if line_icd_dgns_cd=="R300" | line_icd_dgns_cd=="R309"  
	gen icd7883 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7883"
		replace icd7883 = 1 if line_icd_dgns_cd=="R32" | line_icd_dgns_cd=="N3941" | line_icd_dgns_cd=="N3946" | line_icd_dgns_cd=="N3942" | line_icd_dgns_cd=="N3944" | line_icd_dgns_cd=="N39491" | line_icd_dgns_cd=="N3942" | line_icd_dgns_cd=="N3948"                    
	gen icd7884 = 1 if substr(line_icd_dgns_cd, 1, 4)=="7884"
		replace icd7884 = 1 if line_icd_dgns_cd=="R350" | line_icd_dgns_cd=="R358" | line_icd_dgns_cd=="R352" 
	gen icd78863 = 1 if line_icd_dgns_cd=="78863"
		replace icd78863 = 1 if line_icd_dgns_cd=="R3915"  

drop if line_icd_dgns_cd=="78832" | line_icd_dgns_cd=="78835" | line_icd_dgns_cd=="78837" | line_icd_dgns_cd=="78838"
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = expnsdt1 if icd59651==1
gen cdate_icd59654 = expnsdt1 if icd59654==1
gen cdate_icd59659 = expnsdt1 if icd59659==1
gen cdate_icd7881 = expnsdt1 if icd7881==1
gen cdate_icd7883 = expnsdt1 if icd7883==1
gen cdate_icd7884 = expnsdt1 if icd7884==1
gen cdate_icd78863 = expnsdt1 if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id expnsdt1
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile carl
save `carl', replace

////////////////////////////////////////////////////////////////////////////////
/////////			Carrier	Claim					////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/car/2016/carc2016.dta", replace
keep bene_id icd_dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/12{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651" 
		replace icd59651 = 1 if icd_dgns_cd`i'=="N3281"  
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654" 
		replace icd59654 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659" 
		replace icd59659 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881" 
		replace icd7881 = 1 if icd_dgns_cd`i'=="R300" | icd_dgns_cd`i'=="R309"  
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
		replace icd7883 = 1 if icd_dgns_cd`i'=="R32" | icd_dgns_cd`i'=="N3941" | icd_dgns_cd`i'=="N3946" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3944" | icd_dgns_cd`i'=="N39491" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3948"                    
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
		replace icd7884 = 1 if icd_dgns_cd`i'=="R350" | icd_dgns_cd`i'=="R358" | icd_dgns_cd`i'=="R352" 
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
		replace icd78863 = 1 if icd_dgns_cd`i'=="R3915"  
}
forvalues i=1/12{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile carc
save `carc', replace

////////////////////////////////////////////////////////////////////////////////
/////////			HOS								////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/hos/2016/hosc2016.dta", replace
keep bene_id icd_dgns_* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/25{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651" 
		replace icd59651 = 1 if icd_dgns_cd`i'=="N3281"  
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654" 
		replace icd59654 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659" 
		replace icd59659 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881" 
		replace icd7881 = 1 if icd_dgns_cd`i'=="R300" | icd_dgns_cd`i'=="R309"  
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
		replace icd7883 = 1 if icd_dgns_cd`i'=="R32" | icd_dgns_cd`i'=="N3941" | icd_dgns_cd`i'=="N3946" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3944" | icd_dgns_cd`i'=="N39491" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3948"                    
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
		replace icd7884 = 1 if icd_dgns_cd`i'=="R350" | icd_dgns_cd`i'=="R358" | icd_dgns_cd`i'=="R352" 
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
		replace icd78863 = 1 if icd_dgns_cd`i'=="R3915"  
}
forvalues i=1/25{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile hos
save `hos', replace

////////////////////////////////////////////////////////////////////////////////
/////////			DME claim						////////////////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/dme/2016/dmec2016.dta", replace
keep bene_id icd_dgns_cd* from_dt
egen idn = group(bene_id)

//Dummies
gen icd59651 = .
gen icd59654 = .
gen icd59659 = .
gen icd7881 = .
gen icd7883 = .
gen icd7884 = .
gen icd78863 = .

forvalues i=1/12{
	replace icd59651 = 1 if icd_dgns_cd`i'=="59651" 
		replace icd59651 = 1 if icd_dgns_cd`i'=="N3281"  
	replace icd59654 = 1 if icd_dgns_cd`i'=="59654" 
		replace icd59654 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd59659 = 1 if icd_dgns_cd`i'=="59659" 
		replace icd59659 = 1 if icd_dgns_cd`i'=="N319"  
	replace icd7881 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7881" 
		replace icd7881 = 1 if icd_dgns_cd`i'=="R300" | icd_dgns_cd`i'=="R309"  
	replace icd7883 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7883"
		replace icd7883 = 1 if icd_dgns_cd`i'=="R32" | icd_dgns_cd`i'=="N3941" | icd_dgns_cd`i'=="N3946" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3944" | icd_dgns_cd`i'=="N39491" | icd_dgns_cd`i'=="N3942" | icd_dgns_cd`i'=="N3948"                    
	replace icd7884 = 1 if substr(icd_dgns_cd`i', 1, 4)=="7884"
		replace icd7884 = 1 if icd_dgns_cd`i'=="R350" | icd_dgns_cd`i'=="R358" | icd_dgns_cd`i'=="R352" 
	replace icd78863 = 1 if icd_dgns_cd`i'=="78863"
		replace icd78863 = 1 if icd_dgns_cd`i'=="R3915"  
}
forvalues i=1/12{
	drop if icd_dgns_cd`i'=="78832" | icd_dgns_cd`i'=="78835" | icd_dgns_cd`i'=="78837" | icd_dgns_cd`i'=="78838"
}
keep if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1

//Dates
gen cdate_icd59651 = from_dt if icd59651==1
gen cdate_icd59654 = from_dt if icd59654==1
gen cdate_icd59659 = from_dt if icd59659==1
gen cdate_icd7881 = from_dt if icd7881==1
gen cdate_icd7883 = from_dt if icd7883==1
gen cdate_icd7884 = from_dt if icd7884==1
gen cdate_icd78863 = from_dt if icd78863==1

xfill icd5* icd7*, i(idn)

egen d1_icd59651 = min(cdate_icd59651), by(idn) 
egen d1_icd59654 = min(cdate_icd59654), by(idn) 
egen d1_icd59659 = min(cdate_icd59659), by(idn) 
egen d1_icd7881 = min(cdate_icd7881), by(idn) 
egen d1_icd7883 = min(cdate_icd7883), by(idn) 
egen d1_icd7884 = min(cdate_icd7884), by(idn) 
egen d1_icd78863 = min(cdate_icd78863), by(idn) 

sort bene_id from_dt
drop if bene_id==bene_id[_n-1]

keep bene_id icd5* icd7* d1_*
count
sum icd5* icd7* 

tempfile dmec
save `dmec', replace

////////////////////////////////////////////////////////////////////////////////
///////////////////////			MERGE AND SAVE			////////////////////////
////////////////////////////////////////////////////////////////////////////////
use `ip', replace
append using `snf'
append using `op'
append using `hha'
append using `carl'
append using `carc'
append using `hos'
append using `dmec'

egen idn = group(bene_id)

egen icd59651_d1 = min(d1_icd59651), by(idn)
egen icd59654_d1 = min(d1_icd59654), by(idn)
egen icd59659_d1 = min(d1_icd59659), by(idn)
egen icd7881_d1 = min(d1_icd7881), by(idn)
egen icd7883_d1 = min(d1_icd7883), by(idn)
egen icd7884_d1 = min(d1_icd7884), by(idn)
egen icd78863_d1 = min(d1_icd78863), by(idn)
drop d1* 
 
xfill icd5* icd7*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]

desc
sum

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2016.dta", replace

////////////////////////////////////////////////////////////////////////////////
///////////////////////			2004-2016		////////////////////////
////////////////////////////////////////////////////////////////////////////////

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2002.dta", replace
gen oab2002 = 1
tempfile 2002
save `2002', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2003.dta", replace
gen oab2003 = 1
tempfile 2003
save `2003', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2004.dta", replace
gen oab2004 = 1
tempfile 2004
save `2004', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2005.dta", replace
gen oab2005 = 1
tempfile 2005
save `2005', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2006.dta", replace
gen oab2006 = 1
tempfile 2006
save `2006', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2007.dta", replace
gen oab2007 = 1
tempfile 2007
save `2007', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2008.dta", replace
gen oab2008 = 1
tempfile 2008
save `2008', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2009.dta", replace
gen oab2009 = 1
tempfile 2009
save `2009', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2010.dta", replace
gen oab2010 = 1
tempfile 2010
save `2010', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2011.dta", replace
gen oab2011 = 1
tempfile 2011
save `2011', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2012.dta", replace
gen oab2012 = 1
tempfile 2012
save `2012', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2013.dta", replace
gen oab2013 = 1
tempfile 2013
save `2013', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2014.dta", replace
gen oab2014 = 1
tempfile 2014
save `2014', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2015.dta", replace
gen oab2015 = 1
tempfile 2015
save `2015', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab2016.dta", replace
gen oab2016 = 1
tempfile 2016
save `2016', replace

//append the years
use `2002', replace
append using `2003'
append using `2004'
append using `2005'
append using `2006'
append using `2007'
append using `2008'
append using `2009'
append using `2010'
append using `2011'
append using `2012'
append using `2013'
append using `2014'
append using `2015'
append using `2016'

drop idn ehic
egen idn = group(bene_id)
codebook bene_id

egen icd59651_d = min(icd59651_d1), by(idn)
egen icd59654_d = min(icd59654_d1), by(idn)
egen icd59659_d = min(icd59659_d1), by(idn)
egen icd7881_d = min(icd7881_d1), by(idn)
egen icd7883_d = min(icd7883_d1), by(idn)
egen icd7884_d = min(icd7884_d1), by(idn)
egen icd78863_d = min(icd78863_d1), by(idn)

drop icd59651_d1 icd59654_d1 icd59659_d1 icd7881_d1 icd7883_d1 icd7884_d1 icd78863_d1

xfill oab20* icd5* icd7*, i(idn)
egen oab_d = rowmin(icd59651_d icd59654_d icd59659_d icd7881_d icd7883_d icd7884_d icd78863_d)

gen year_oabmax=.
gen year_oabmin=.
forvalues i=2002(1)2016{
	replace year_oabmax = `i' if oab`i'==1
}
forvalues i=2016(-1)2002{
	replace year_oabmin = `i' if oab`i'==1
}

//zeros for specific dx
local icddx "59651 59654 59659 7881 7883 7884 78863"
foreach i in `icddx'{
	replace icd`i' = 0 if icd`i'==.
}

sort bene_id
drop if bene_id==bene_id[_n-1]
count

gen oab_y = year(oab_d)
tab oab_y

drop idn
compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab_0216.dta", replace
desc 
sum
