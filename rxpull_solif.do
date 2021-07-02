clear all
set more off
capture log close

/*
•	Input: pdeYYYY.dta (06-13)
•	Output: XXXXX_dYYYY.dta (06-13)
•	Pulls drugs of interest from 06-13, by gname.
o	Gets NDCs associated with gnames from ndcbygname_bam.do
•	Makes arrays of use to characterize use during the year. Pushes forward extra pills from early fills, the last fill of the year, IP days, and SNF days. 
•	Makes variables for total daily doses of each drug in the year
*/
/*
Minimum daily doses:
Darifenacin - 7.5 mg
Fesoterodine - 4 mg
Flavoxate - 300 mg
Oxybutynin - patch 3.9 mg, oral 5 mg
Solifenacin - 5 mg
Tolterodine - 2 mg
Trospium - 20 mg
*/

////////////////////////////////////////////////////////////////////////////////
/////////			NDC level file with solif	////////////////////////////
////////////////////////////////////////////////////////////////////////////////
//keep the ndcs defined by gname, excluding the combo pills
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/DrugInfo/ndcall_tclass_all_2016_02_10.dta", replace
keep ndc gname

gen solif = 1 if strpos(gname, "SOLIFENACIN")

keep if solif==1 

tempfile solif
save `solif', replace

////////////////////////////////////////////////////////////////////////////////
///////////////////////			solif pull			////////////////////////
////////////////////////////////////////////////////////////////////////////////

////////////////		2006			////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/extracts/pde/20pct/2006/opt1pde2006.dta", replace
keep bene_id srvc_dt prdsrvid dayssply qtydspns gcdf gcdf_desc str
rename prdsrvid ndc

merge m:1 ndc using `solif'
keep if _m==3 
drop _m

sum qtydspns, detail
levelsof qtydspns, clean
tab str
tab gcdf_desc

//drop observations with crazy quantity dispensed
drop if qtydspns<1 & qtydspns!=.
drop if qtydspns>365 & qtydspns!=.

egen idn = group(bene_id)
sort bene_id srvc_dt

//Two claims for the same bene on the same day?
/* ~38 claims out of a 1000 (for 2006 loops) are by the same person on the same day
(i.e. 19 pairs of claims). We think those prescritpions are meant to be taken together,
meaning that if it's two 30 day fills, it is worth 30 possession days, not 60. 
So, we'll take the max of the two claims on the same day, and then drop one of the 
observations. 
*/
egen claim=group(bene_id srvc_dt)

gen repeat = 1 if srvc_dt==srvc_dt[_n-1] & bene_id==bene_id[_n-1]
replace repeat = 1 if srvc_dt==srvc_dt[_n+1] & bene_id==bene_id[_n+1]

egen repeatday = max(dayssply), by(claim)
replace dayssply = repeatday if repeat==1

drop repeat repeatday claim
drop if srvc_dt==srvc_dt[_n-1] & bene_id==bene_id[_n-1]

//////////////////  Early fills pushes
//for people that fill their prescription before emptying their last, carry the extra pills forward
//extrapill_push is the amount, from that fill date, that is superfluous for reaching the next fill date. Capped at 10. 
gen doy_srvc = doy(srvc_dt)
replace doy_srvc = 0 if year(srvc_dt)<2006
replace doy_srvc = 365 if year(srvc_dt)>2006

gen extrapill_push = 0
replace extrapill_push = extrapill_push + (doy_srvc+dayssply)-(doy_srvc[_n+1]) if bene_id==bene_id[_n+1]
replace extrapill_push = 0 if extrapill_push<0
replace extrapill_push = 10 if extrapill_push>10

//pushstock is the accumulated stock of extra pills. Capped at 10. 
gen pushstock = extrapill_push
replace extrapill_push = 10 if extrapill_push>10

gen need = 0
replace need = (doy_srvc[_n+1])-(doy_srvc+dayssply)
replace need = (365 - (doy_srvc)+dayssply) if bene_id!=bene_id[_n+1]
replace need = 0 if need<0

gen dayssply2 = dayssply

// Creating a loop that will loop through one observation at a time and do the following
/* 1. Add the previous pushstock to the current pushstock
   2. Add the needed extra pills to the dayssply which is the dayssply + the min of either the need or the pushstock since:
	a. If need < pushstock, then need gets added to dayssply
	b. If need >= pushstock, then pushstock gets added to dayssply
   3. Calculate the new pushstock, which is the pushstock minus the need with a bottom cap at 0 and a high cap at 10 */
	
local N=_N

forvalues i=1/`N' {
	quietly replace pushstock=pushstock[_n-1]+pushstock if pushstock[_n-1]<. & bene_id==bene_id[_n-1] in `i'
	quietly replace dayssply2=dayssply2+min(need,pushstock) if bene_id==bene_id[_n-1] in `i'
	quietly replace pushstock=min(max(pushstock-need,0),10) if bene_id==bene_id[_n-1] in `i'
}

//value of early fill push stock at the end of the year
gen earlyfill_push = pushstock if bene_id!=bene_id[_n+1]
replace earlyfill_push = 0 if earlyfill_push<0
replace earlyfill_push = 10 if earlyfill_push>10 & earlyfill_push!=.
//extra pills from last prescription at end of year, capped at 90
gen lastfill_push = (doy_srvc+dayssply-365) if bene_id!=bene_id[_n+1]
replace lastfill_push = 0 if lastfill_push<0 & lastfill_push!=.
replace lastfill_push = 90 if lastfill_push>90 & lastfill_push!=.

xfill earlyfill_push lastfill_push, i(idn)

drop pushstock need extrapill_push

///////////////// Array
forvalues d = 1/365{
	gen solif_a`d' = 1 if `d'>=doy_srvc & `d'<=(doy_srvc+dayssply2) & solif==1
}

drop doy_srvc
xfill solif_a*, i(idn)

/////////////////bene-class timing
egen solif_fdays_2006 = sum(dayssply), by(idn)
egen solif_clms_2006 = count(dayssply), by(idn)
egen solif_minfilldt_2006 = min(srvc_dt), by(idn)
egen solif_maxfilldt_2006 = max(srvc_dt), by(idn)
gen solif_fillperiod_2006 = solif_maxfilldt_2006 - solif_minfilldt_2006 + 1

//////////////// weighted average dose
//claim_mgs is calculated dependent on dose form (at the claim level). 
gen claim_mgs = 0
replace claim_mgs = 5*qtydspns if str=="5MG"
replace claim_mgs = 10*qtydspns if str=="10MG"
replace claim_mgs = 5*qtydspns if str=="5 MG"
replace claim_mgs = 10*qtydspns if str=="10 MG"
//sum mgs for all claims in year by bene
egen itot_mgs = sum(claim_mgs), by(idn)
//average mgs per day supplied
gen solif_dmg_2006 = itot_mgs/solif_fdays_2006
drop claim_mgs itot_mgs

//bene level
sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

replace earlyfill_push = 0 if earlyfill_push==.
replace lastfill_push = 0 if lastfill_push==.

tempfile classarray
save `classarray', replace

/////////////////		 Bring in SNF 		////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/snf/2006/snfc2006.dta", clear
keep bene_id from_dt thru_dt

merge m:1 bene_id using `classarray'
keep if _m==3 | _m==2
drop _m
codebook bene_id

gen doy_from = doy(from_dt)
replace doy_from = 0 if year(from_dt)<2006
replace doy_from = 365 if year(from_dt)>2006
gen doy_thru = doy(thru_dt)
replace doy_thru = 0 if year(thru_dt)<2006
replace doy_thru = 365 if year(thru_dt)>2006

////////////// Array
forvalues d = 1/365{
	gen snf_a`d' = 1 if `d'>=doy_from & `d'<=doy_thru 
}
drop from_dt thru_dt doy_from doy_thru
xfill snf_a*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

///////////   SNF push
//snf_push is the extra days added for SNF days concurrent with drug days. Capped at 10. 
gen snf_push = 0

forvalues d = 1/365{
	replace snf_push = snf_push + 1 if solif_a`d'==1 & snf_a`d'==1

	//if the solif_a`d' is 1, do nothing (already added it to the snf_push)
	//if that solif_a spot is empty:
	gen v1 = 1 if snf_push>0 & solif_a`d'==.
	replace solif_a`d' = 1 if v1==1
	replace snf_push = snf_push-1 if v1==1
	drop v1
	replace snf_push = 10 if snf_push>10
}
drop snf_a*

save `classarray', replace

/////////////////		 Bring in IP 		////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/ip/2006/ipc2006.dta", clear
keep bene_id from_dt thru_dt 

merge m:1 bene_id using `classarray'
keep if _m==3 | _m==2
drop _m
codebook bene_id

gen doy_from = doy(from_dt)
replace doy_from = 0 if year(from_dt)<2006
replace doy_from = 365 if year(from_dt)>2006
gen doy_thru = doy(thru_dt)
replace doy_thru = 0 if year(thru_dt)<2006
replace doy_thru = 365 if year(thru_dt)>2006

//////////////// Array
forvalues d = 1/365{
	gen ips_a`d' = 1 if `d'>=doy_from & `d'<=doy_thru
}
drop from_dt thru_dt doy_from doy_thru
xfill ips_a*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

///////////////  IP push
//ips_push is the extra days added for ip days concurrent with drug days
gen ips_push = 0

forvalues d = 1/365{
	replace ips_push = ips_push + 1 if solif_a`d'==1 & ips_a`d'==1

	//if the solif_a`d' is 1, do nothing (already added it to the ips_push)
	//if that solif_a spot is not full:
	gen v1 = 1 if ips_push>0 & solif_a`d'==.
	replace solif_a`d' = 1 if v1==1
	replace ips_push = ips_push-1 if v1==1
	drop v1
	replace ips_push = 10 if ips_push>10
}
drop ips_a*

////////////////  Final possession day calculations
gen solif_pdays_2006 = 0

/*The array is filled using dayssply2, which includes adjustments for early fills.
Then, the array is adjusted further for IPS and SNF days. 
So, the sum of the ones in the array is the total 2006 possession days
*/
forvalues d = 1/365{
	replace solif_pdays_2006 = solif_pdays_2006 + 1 if solif_a`d'==1
}
drop solif_a*
replace solif_pdays_2006 = 365 if solif_pdays_2006>365

//	mgs in year, for all the possession days
gen solif_totmg_2006 = solif_pdays_2006*solif_dmg_2006
//	total daily doses in the year
gen solif_totdd_2006 = solif_totmg_2006/5

//////////////// Calculate the number of extra push days that could go into next year. 
//inyear_push = extra push days from early fills throughout the year, IPS days, and SNF days. (each of which was capped at 10, but I will still cap whole thing at 30). 
//lastfill_push is the amount from the last fill that goes into next year (capped at 90). 
gen inyear_push = earlyfill_push + snf_push + ips_push
replace inyear_push = 30 if inyear_push>30 & inyear_push!=.
egen solif_push_2006 = rowmax(lastfill_push inyear_push) 
replace solif_push_2006 = 0 if solif_push_2006==.
replace solif_push_2006 = 0 if solif_push_2006<0

keep bene_id solif* 

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2006.dta", replace

keep bene_id solif_push_2006
tempfile push
save `push', replace

////////////////		2007			////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/extracts/pde/20pct/2007/opt1pde2007.dta", replace
keep bene_id srvc_dt prdsrvid dayssply qtydspns gcdf gcdf_desc str
rename prdsrvid ndc

merge m:1 ndc using `solif'
keep if _m==3 
drop _m

sum qtydspns, detail
levelsof qtydspns, clean
tab str
tab gcdf_desc

//drop observations with crazy quantity dispensed
drop if qtydspns<1 & qtydspns!=.
drop if qtydspns>365 & qtydspns!=.

merge m:1 bene_id using `push'
keep if _m==3 | _m==1
drop _m

egen idn = group(bene_id)
sort bene_id srvc_dt

//Two claims for the same bene on the same day?
/* ~38 claims out of a 1000 (for 2006 loops) are by the same person on the same day
(i.e. 19 pairs of claims). We think those prescritpions are meant to be taken together,
meaning that if it's two 30 day fills, it is worth 30 possession days, not 60. 
So, we'll take the max of the two claims on the same day, and then drop one of the 
observations. 
*/
egen claim=group(bene_id srvc_dt)

gen repeat = 1 if srvc_dt==srvc_dt[_n-1] & bene_id==bene_id[_n-1]
replace repeat = 1 if srvc_dt==srvc_dt[_n+1] & bene_id==bene_id[_n+1]

egen repeatday = max(dayssply), by(claim)
replace dayssply = repeatday if repeat==1

drop repeat repeatday claim
drop if srvc_dt==srvc_dt[_n-1] & bene_id==bene_id[_n-1]

//////////////////  Early fills pushes
//for people that fill their prescription before emptying their last, carry the extra pills forward
//extrapill_push is the amount, from that fill date, that is superfluous for reaching the next fill date. Capped at 10. 
gen doy_srvc = doy(srvc_dt)
replace doy_srvc = 0 if year(srvc_dt)<2007
replace doy_srvc = 365 if year(srvc_dt)>2007

gen extrapill_push = 0
replace extrapill_push = extrapill_push + (doy_srvc+dayssply)-(doy_srvc[_n+1]) if bene_id==bene_id[_n+1]
replace extrapill_push = 0 if extrapill_push<0
replace extrapill_push = 10 if extrapill_push>10

//pushstock is the accumulated stock of extra pills. Capped at 10. 
gen pushstock = extrapill_push
replace extrapill_push = 10 if extrapill_push>10

//add the pushstock from last year
replace pushstock = pushstock + solif_push_2006 if bene_id!=bene_id[_n-1] | _n==1

gen need = 0
replace need = (doy_srvc[_n+1])-(doy_srvc+dayssply)
replace need = (365 - (doy_srvc)+dayssply) if bene_id!=bene_id[_n+1]
replace need = 0 if need<0

gen dayssply2 = dayssply

// Creating a loop that will loop through one observation at a time and do the following
/* 1. Add the previous pushstock to the current pushstock
   2. Add the needed extra pills to the dayssply which is the dayssply + the min of either the need or the pushstock since:
	a. If need < pushstock, then need gets added to dayssply
	b. If need >= pushstock, then pushstock gets added to dayssply
   3. Calculate the new pushstock, which is the pushstock minus the need with a bottom cap at 0 and a high cap at 10 */
	
local N=_N

forvalues i=1/`N' {
	quietly replace pushstock=pushstock[_n-1]+pushstock if pushstock[_n-1]<. & bene_id==bene_id[_n-1] in `i'
	quietly replace dayssply2=dayssply2+min(need,pushstock) if bene_id==bene_id[_n-1] in `i'
	quietly replace pushstock=min(max(pushstock-need,0),10) if bene_id==bene_id[_n-1] in `i'
}

//value of early fill push stock at the end of the year
gen earlyfill_push = pushstock if bene_id!=bene_id[_n+1]
replace earlyfill_push = 0 if earlyfill_push<0
replace earlyfill_push = 10 if earlyfill_push>10 & earlyfill_push!=.
//extra pills from last prescription at end of year, capped at 90
gen lastfill_push = (doy_srvc+dayssply-365) if bene_id!=bene_id[_n+1]
replace lastfill_push = 0 if lastfill_push<0 & lastfill_push!=.
replace lastfill_push = 90 if lastfill_push>90 & lastfill_push!=.

xfill earlyfill_push lastfill_push, i(idn)

drop pushstock need extrapill_push

///////////////// Array
forvalues d = 1/365{
	gen solif_a`d' = 1 if `d'>=doy_srvc & `d'<=(doy_srvc+dayssply2) & solif==1
}

drop doy_srvc
xfill solif_a*, i(idn)

/////////////////bene-class timing
egen solif_fdays_2007 = sum(dayssply), by(idn)
egen solif_clms_2007 = count(dayssply), by(idn)
egen solif_minfilldt_2007 = min(srvc_dt), by(idn)
egen solif_maxfilldt_2007 = max(srvc_dt), by(idn)
gen solif_fillperiod_2007 = solif_maxfilldt_2007 - solif_minfilldt_2007 + 1

//////////////// weighted average dose
//claim_mgs is calculated dependent on dose form (at the claim level). 
gen claim_mgs = 0
replace claim_mgs = 5*qtydspns if str=="5MG"
replace claim_mgs = 10*qtydspns if str=="10MG"
replace claim_mgs = 5*qtydspns if str=="5 MG"
replace claim_mgs = 10*qtydspns if str=="10 MG"
//sum mgs for all claims in year by bene
egen itot_mgs = sum(claim_mgs), by(idn)
//average mgs per day supplied
gen solif_dmg_2007 = itot_mgs/solif_fdays_2007
drop claim_mgs itot_mgs

//bene level
sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

replace earlyfill_push = 0 if earlyfill_push==.
replace lastfill_push = 0 if lastfill_push==.

tempfile classarray
save `classarray', replace

/////////////////		 Bring in SNF 		////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/snf/2007/snfc2007.dta", clear
keep bene_id from_dt thru_dt

merge m:1 bene_id using `classarray'
keep if _m==3 | _m==2
drop _m
codebook bene_id

gen doy_from = doy(from_dt)
replace doy_from = 0 if year(from_dt)<2007
replace doy_from = 365 if year(from_dt)>2007
gen doy_thru = doy(thru_dt)
replace doy_thru = 0 if year(thru_dt)<2007
replace doy_thru = 365 if year(thru_dt)>2007

////////////// Array
forvalues d = 1/365{
	gen snf_a`d' = 1 if `d'>=doy_from & `d'<=doy_thru 
}
drop from_dt thru_dt doy_from doy_thru
xfill snf_a*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

///////////   SNF push
//snf_push is the extra days added for SNF days concurrent with drug days. Capped at 10. 
gen snf_push = 0

forvalues d = 1/365{
	replace snf_push = snf_push + 1 if solif_a`d'==1 & snf_a`d'==1

	//if the solif_a`d' is 1, do nothing (already added it to the snf_push)
	//if that solif_a spot is empty:
	gen v1 = 1 if snf_push>0 & solif_a`d'==.
	replace solif_a`d' = 1 if v1==1
	replace snf_push = snf_push-1 if v1==1
	drop v1
	replace snf_push = 10 if snf_push>10
}
drop snf_a*

save `classarray', replace

/////////////////		 Bring in IP 		////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/ip/2007/ipc2007.dta", clear
keep bene_id from_dt thru_dt 

merge m:1 bene_id using `classarray'
keep if _m==3 | _m==2
drop _m
codebook bene_id

gen doy_from = doy(from_dt)
replace doy_from = 0 if year(from_dt)<2007
replace doy_from = 365 if year(from_dt)>2007
gen doy_thru = doy(thru_dt)
replace doy_thru = 0 if year(thru_dt)<2007
replace doy_thru = 365 if year(thru_dt)>2007

//////////////// Array
forvalues d = 1/365{
	gen ips_a`d' = 1 if `d'>=doy_from & `d'<=doy_thru
}
drop from_dt thru_dt doy_from doy_thru
xfill ips_a*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

///////////////  IP push
//ips_push is the extra days added for ip days concurrent with drug days
gen ips_push = 0

forvalues d = 1/365{
	replace ips_push = ips_push + 1 if solif_a`d'==1 & ips_a`d'==1

	//if the solif_a`d' is 1, do nothing (already added it to the ips_push)
	//if that solif_a spot is not full:
	gen v1 = 1 if ips_push>0 & solif_a`d'==.
	replace solif_a`d' = 1 if v1==1
	replace ips_push = ips_push-1 if v1==1
	drop v1
	replace ips_push = 10 if ips_push>10
}
drop ips_a*

////////////////  Final possession day calculations
gen solif_pdays_2007 = 0

/*The array is filled using dayssply2, which includes adjustments for early fills.
Then, the array is adjusted further for IPS and SNF days. 
So, the sum of the ones in the array is the total 2007 possession days
*/
forvalues d = 1/365{
	replace solif_pdays_2007 = solif_pdays_2007 + 1 if solif_a`d'==1
}
drop solif_a*
replace solif_pdays_2007 = 365 if solif_pdays_2007>365

//	mgs in year, for all the possession days
gen solif_totmg_2007 = solif_pdays_2007*solif_dmg_2007
//	total daily doses in the year
gen solif_totdd_2007 = solif_totmg_2007/5

//////////////// Calculate the number of extra push days that could go into next year. 
//inyear_push = extra push days from early fills throughout the year, IPS days, and SNF days. (each of which was capped at 10, but I will still cap whole thing at 30). 
//lastfill_push is the amount from the last fill that goes into next year (capped at 90). 
gen inyear_push = earlyfill_push + snf_push + ips_push
replace inyear_push = 30 if inyear_push>30 & inyear_push!=.
egen solif_push_2007 = rowmax(lastfill_push inyear_push) 
replace solif_push_2007 = 0 if solif_push_2007==.
replace solif_push_2007 = 0 if solif_push_2007<0

keep bene_id solif* 

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2007.dta", replace

keep bene_id solif_push_2007
tempfile push
save `push', replace

////////////////		2008			////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/extracts/pde/20pct/2008/opt1pde2008.dta", replace
keep bene_id srvc_dt prdsrvid dayssply qtydspns gcdf gcdf_desc str
rename prdsrvid ndc

merge m:1 ndc using `solif'
keep if _m==3 
drop _m

sum qtydspns, detail
levelsof qtydspns, clean
tab str
tab gcdf_desc

//drop observations with crazy quantity dispensed
drop if qtydspns<1 & qtydspns!=.
drop if qtydspns>365 & qtydspns!=.

merge m:1 bene_id using `push'
keep if _m==3 | _m==1
drop _m

egen idn = group(bene_id)
sort bene_id srvc_dt

//Two claims for the same bene on the same day?
/* ~38 claims out of a 1000 (for 2006 loops) are by the same person on the same day
(i.e. 19 pairs of claims). We think those prescritpions are meant to be taken together,
meaning that if it's two 30 day fills, it is worth 30 possession days, not 60. 
So, we'll take the max of the two claims on the same day, and then drop one of the 
observations. 
*/
egen claim=group(bene_id srvc_dt)

gen repeat = 1 if srvc_dt==srvc_dt[_n-1] & bene_id==bene_id[_n-1]
replace repeat = 1 if srvc_dt==srvc_dt[_n+1] & bene_id==bene_id[_n+1]

egen repeatday = max(dayssply), by(claim)
replace dayssply = repeatday if repeat==1

drop repeat repeatday claim
drop if srvc_dt==srvc_dt[_n-1] & bene_id==bene_id[_n-1]

//////////////////  Early fills pushes
//for people that fill their prescription before emptying their last, carry the extra pills forward
//extrapill_push is the amount, from that fill date, that is superfluous for reaching the next fill date. Capped at 10. 
gen doy_srvc = doy(srvc_dt)
replace doy_srvc = 0 if year(srvc_dt)<2008
replace doy_srvc = 365 if year(srvc_dt)>2008

gen extrapill_push = 0
replace extrapill_push = extrapill_push + (doy_srvc+dayssply)-(doy_srvc[_n+1]) if bene_id==bene_id[_n+1]
replace extrapill_push = 0 if extrapill_push<0
replace extrapill_push = 10 if extrapill_push>10

//pushstock is the accumulated stock of extra pills. Capped at 10. 
gen pushstock = extrapill_push
replace extrapill_push = 10 if extrapill_push>10

//add the pushstock from last year
replace pushstock = pushstock + solif_push_2007 if bene_id!=bene_id[_n-1] | _n==1

gen need = 0
replace need = (doy_srvc[_n+1])-(doy_srvc+dayssply)
replace need = (365 - (doy_srvc)+dayssply) if bene_id!=bene_id[_n+1]
replace need = 0 if need<0

gen dayssply2 = dayssply

// Creating a loop that will loop through one observation at a time and do the following
/* 1. Add the previous pushstock to the current pushstock
   2. Add the needed extra pills to the dayssply which is the dayssply + the min of either the need or the pushstock since:
	a. If need < pushstock, then need gets added to dayssply
	b. If need >= pushstock, then pushstock gets added to dayssply
   3. Calculate the new pushstock, which is the pushstock minus the need with a bottom cap at 0 and a high cap at 10 */
	
local N=_N

forvalues i=1/`N' {
	quietly replace pushstock=pushstock[_n-1]+pushstock if pushstock[_n-1]<. & bene_id==bene_id[_n-1] in `i'
	quietly replace dayssply2=dayssply2+min(need,pushstock) if bene_id==bene_id[_n-1] in `i'
	quietly replace pushstock=min(max(pushstock-need,0),10) if bene_id==bene_id[_n-1] in `i'
}

//value of early fill push stock at the end of the year
gen earlyfill_push = pushstock if bene_id!=bene_id[_n+1]
replace earlyfill_push = 0 if earlyfill_push<0
replace earlyfill_push = 10 if earlyfill_push>10 & earlyfill_push!=.
//extra pills from last prescription at end of year, capped at 90
gen lastfill_push = (doy_srvc+dayssply-365) if bene_id!=bene_id[_n+1]
replace lastfill_push = 0 if lastfill_push<0 & lastfill_push!=.
replace lastfill_push = 90 if lastfill_push>90 & lastfill_push!=.

xfill earlyfill_push lastfill_push, i(idn)

drop pushstock need extrapill_push

///////////////// Array
forvalues d = 1/365{
	gen solif_a`d' = 1 if `d'>=doy_srvc & `d'<=(doy_srvc+dayssply2) & solif==1
}

drop doy_srvc
xfill solif_a*, i(idn)

/////////////////bene-class timing
egen solif_fdays_2008 = sum(dayssply), by(idn)
egen solif_clms_2008 = count(dayssply), by(idn)
egen solif_minfilldt_2008 = min(srvc_dt), by(idn)
egen solif_maxfilldt_2008 = max(srvc_dt), by(idn)
gen solif_fillperiod_2008 = solif_maxfilldt_2008 - solif_minfilldt_2008 + 1

//////////////// weighted average dose
//claim_mgs is calculated dependent on dose form (at the claim level). 
gen claim_mgs = 0
replace claim_mgs = 5*qtydspns if str=="5MG"
replace claim_mgs = 10*qtydspns if str=="10MG"
replace claim_mgs = 5*qtydspns if str=="5 MG"
replace claim_mgs = 10*qtydspns if str=="10 MG"
//sum mgs for all claims in year by bene
egen itot_mgs = sum(claim_mgs), by(idn)
//average mgs per day supplied
gen solif_dmg_2008 = itot_mgs/solif_fdays_2008
drop claim_mgs itot_mgs

//bene level
sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

replace earlyfill_push = 0 if earlyfill_push==.
replace lastfill_push = 0 if lastfill_push==.

tempfile classarray
save `classarray', replace

/////////////////		 Bring in SNF 		////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/snf/2008/snfc2008.dta", clear
keep bene_id from_dt thru_dt

merge m:1 bene_id using `classarray'
keep if _m==3 | _m==2
drop _m
codebook bene_id

gen doy_from = doy(from_dt)
replace doy_from = 0 if year(from_dt)<2008
replace doy_from = 365 if year(from_dt)>2008
gen doy_thru = doy(thru_dt)
replace doy_thru = 0 if year(thru_dt)<2008
replace doy_thru = 365 if year(thru_dt)>2008

////////////// Array
forvalues d = 1/365{
	gen snf_a`d' = 1 if `d'>=doy_from & `d'<=doy_thru 
}
drop from_dt thru_dt doy_from doy_thru
xfill snf_a*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

///////////   SNF push
//snf_push is the extra days added for SNF days concurrent with drug days. Capped at 10. 
gen snf_push = 0

forvalues d = 1/365{
	replace snf_push = snf_push + 1 if solif_a`d'==1 & snf_a`d'==1

	//if the solif_a`d' is 1, do nothing (already added it to the snf_push)
	//if that solif_a spot is empty:
	gen v1 = 1 if snf_push>0 & solif_a`d'==.
	replace solif_a`d' = 1 if v1==1
	replace snf_push = snf_push-1 if v1==1
	drop v1
	replace snf_push = 10 if snf_push>10
}
drop snf_a*

save `classarray', replace

/////////////////		 Bring in IP 		////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/ip/2008/ipc2008.dta", clear
keep bene_id from_dt thru_dt 

merge m:1 bene_id using `classarray'
keep if _m==3 | _m==2
drop _m
codebook bene_id

gen doy_from = doy(from_dt)
replace doy_from = 0 if year(from_dt)<2008
replace doy_from = 365 if year(from_dt)>2008
gen doy_thru = doy(thru_dt)
replace doy_thru = 0 if year(thru_dt)<2008
replace doy_thru = 365 if year(thru_dt)>2008

//////////////// Array
forvalues d = 1/365{
	gen ips_a`d' = 1 if `d'>=doy_from & `d'<=doy_thru
}
drop from_dt thru_dt doy_from doy_thru
xfill ips_a*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

///////////////  IP push
//ips_push is the extra days added for ip days concurrent with drug days
gen ips_push = 0

forvalues d = 1/365{
	replace ips_push = ips_push + 1 if solif_a`d'==1 & ips_a`d'==1

	//if the solif_a`d' is 1, do nothing (already added it to the ips_push)
	//if that solif_a spot is not full:
	gen v1 = 1 if ips_push>0 & solif_a`d'==.
	replace solif_a`d' = 1 if v1==1
	replace ips_push = ips_push-1 if v1==1
	drop v1
	replace ips_push = 10 if ips_push>10
}
drop ips_a*

////////////////  Final possession day calculations
gen solif_pdays_2008 = 0

/*The array is filled using dayssply2, which includes adjustments for early fills.
Then, the array is adjusted further for IPS and SNF days. 
So, the sum of the ones in the array is the total 2008 possession days
*/
forvalues d = 1/365{
	replace solif_pdays_2008 = solif_pdays_2008 + 1 if solif_a`d'==1
}
drop solif_a*
replace solif_pdays_2008 = 365 if solif_pdays_2008>365

//	mgs in year, for all the possession days
gen solif_totmg_2008 = solif_pdays_2008*solif_dmg_2008
//	total daily doses in the year
gen solif_totdd_2008 = solif_totmg_2008/5

//////////////// Calculate the number of extra push days that could go into next year. 
//inyear_push = extra push days from early fills throughout the year, IPS days, and SNF days. (each of which was capped at 10, but I will still cap whole thing at 30). 
//lastfill_push is the amount from the last fill that goes into next year (capped at 90). 
gen inyear_push = earlyfill_push + snf_push + ips_push
replace inyear_push = 30 if inyear_push>30 & inyear_push!=.
egen solif_push_2008 = rowmax(lastfill_push inyear_push) 
replace solif_push_2008 = 0 if solif_push_2008==.
replace solif_push_2008 = 0 if solif_push_2008<0

keep bene_id solif* 

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2008.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2008.dta", replace

keep bene_id solif_push_2008
tempfile push
save `push', replace

////////////////		2009			////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/extracts/pde/20pct/2009/opt1pde2009.dta", replace
keep bene_id srvc_dt prdsrvid dayssply qtydspns 
rename prdsrvid ndc

merge m:1 ndc using `solif'
keep if _m==3 
drop _m
tempfile t2009
save `t2009', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/DrugInfo/ndcall_tclass_all_2016_02_10.dta", replace
keep ndc dosage_pde dosage_desc_pde strength_pde
rename dosage_pde gcdf
rename dosage_desc_pde gcdf_desc
rename strength_pde str
sort ndc
drop if ndc==ndc[_n-1]

merge 1:m ndc using `t2009'
keep if _m==3
drop _m

sum qtydspns, detail
levelsof qtydspns, clean
tab str
tab gcdf_desc

//drop observations with crazy quantity dispensed
drop if qtydspns<1 & qtydspns!=.
drop if qtydspns>365 & qtydspns!=.

merge m:1 bene_id using `push'
keep if _m==3 | _m==1
drop _m

egen idn = group(bene_id)
sort bene_id srvc_dt

//Two claims for the same bene on the same day?
/* ~38 claims out of a 1000 (for 2006 loops) are by the same person on the same day
(i.e. 19 pairs of claims). We think those prescritpions are meant to be taken together,
meaning that if it's two 30 day fills, it is worth 30 possession days, not 60. 
So, we'll take the max of the two claims on the same day, and then drop one of the 
observations. 
*/
egen claim=group(bene_id srvc_dt)

gen repeat = 1 if srvc_dt==srvc_dt[_n-1] & bene_id==bene_id[_n-1]
replace repeat = 1 if srvc_dt==srvc_dt[_n+1] & bene_id==bene_id[_n+1]

egen repeatday = max(dayssply), by(claim)
replace dayssply = repeatday if repeat==1

drop repeat repeatday claim
drop if srvc_dt==srvc_dt[_n-1] & bene_id==bene_id[_n-1]

//////////////////  Early fills pushes
//for people that fill their prescription before emptying their last, carry the extra pills forward
//extrapill_push is the amount, from that fill date, that is superfluous for reaching the next fill date. Capped at 10. 
gen doy_srvc = doy(srvc_dt)
replace doy_srvc = 0 if year(srvc_dt)<2009
replace doy_srvc = 365 if year(srvc_dt)>2009

gen extrapill_push = 0
replace extrapill_push = extrapill_push + (doy_srvc+dayssply)-(doy_srvc[_n+1]) if bene_id==bene_id[_n+1]
replace extrapill_push = 0 if extrapill_push<0
replace extrapill_push = 10 if extrapill_push>10

//pushstock is the accumulated stock of extra pills. Capped at 10. 
gen pushstock = extrapill_push
replace extrapill_push = 10 if extrapill_push>10

//add the pushstock from last year
replace pushstock = pushstock + solif_push_2008 if bene_id!=bene_id[_n-1] | _n==1

gen need = 0
replace need = (doy_srvc[_n+1])-(doy_srvc+dayssply)
replace need = (365 - (doy_srvc)+dayssply) if bene_id!=bene_id[_n+1]
replace need = 0 if need<0

gen dayssply2 = dayssply

// Creating a loop that will loop through one observation at a time and do the following
/* 1. Add the previous pushstock to the current pushstock
   2. Add the needed extra pills to the dayssply which is the dayssply + the min of either the need or the pushstock since:
	a. If need < pushstock, then need gets added to dayssply
	b. If need >= pushstock, then pushstock gets added to dayssply
   3. Calculate the new pushstock, which is the pushstock minus the need with a bottom cap at 0 and a high cap at 10 */
	
local N=_N

forvalues i=1/`N' {
	quietly replace pushstock=pushstock[_n-1]+pushstock if pushstock[_n-1]<. & bene_id==bene_id[_n-1] in `i'
	quietly replace dayssply2=dayssply2+min(need,pushstock) if bene_id==bene_id[_n-1] in `i'
	quietly replace pushstock=min(max(pushstock-need,0),10) if bene_id==bene_id[_n-1] in `i'
}

//value of early fill push stock at the end of the year
gen earlyfill_push = pushstock if bene_id!=bene_id[_n+1]
replace earlyfill_push = 0 if earlyfill_push<0
replace earlyfill_push = 10 if earlyfill_push>10 & earlyfill_push!=.
//extra pills from last prescription at end of year, capped at 90
gen lastfill_push = (doy_srvc+dayssply-365) if bene_id!=bene_id[_n+1]
replace lastfill_push = 0 if lastfill_push<0 & lastfill_push!=.
replace lastfill_push = 90 if lastfill_push>90 & lastfill_push!=.

xfill earlyfill_push lastfill_push, i(idn)

drop pushstock need extrapill_push

///////////////// Array
forvalues d = 1/365{
	gen solif_a`d' = 1 if `d'>=doy_srvc & `d'<=(doy_srvc+dayssply2) & solif==1
}

drop doy_srvc
xfill solif_a*, i(idn)

/////////////////bene-class timing
egen solif_fdays_2009 = sum(dayssply), by(idn)
egen solif_clms_2009 = count(dayssply), by(idn)
egen solif_minfilldt_2009 = min(srvc_dt), by(idn)
egen solif_maxfilldt_2009 = max(srvc_dt), by(idn)
gen solif_fillperiod_2009 = solif_maxfilldt_2009 - solif_minfilldt_2009 + 1

//////////////// weighted average dose
//claim_mgs is calculated dependent on dose form (at the claim level). 
gen claim_mgs = 0
replace claim_mgs = 5*qtydspns if str=="5MG"
replace claim_mgs = 10*qtydspns if str=="10MG"
replace claim_mgs = 5*qtydspns if str=="5 MG"
replace claim_mgs = 10*qtydspns if str=="10 MG"
//sum mgs for all claims in year by bene
egen itot_mgs = sum(claim_mgs), by(idn)
//average mgs per day supplied
gen solif_dmg_2009 = itot_mgs/solif_fdays_2009
drop claim_mgs itot_mgs

//bene level
sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

replace earlyfill_push = 0 if earlyfill_push==.
replace lastfill_push = 0 if lastfill_push==.

tempfile classarray
save `classarray', replace

/////////////////		 Bring in SNF 		////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/snf/2009/snfc2009.dta", clear
keep bene_id from_dt thru_dt

merge m:1 bene_id using `classarray'
keep if _m==3 | _m==2
drop _m
codebook bene_id

gen doy_from = doy(from_dt)
replace doy_from = 0 if year(from_dt)<2009
replace doy_from = 365 if year(from_dt)>2009
gen doy_thru = doy(thru_dt)
replace doy_thru = 0 if year(thru_dt)<2009
replace doy_thru = 365 if year(thru_dt)>2009

////////////// Array
forvalues d = 1/365{
	gen snf_a`d' = 1 if `d'>=doy_from & `d'<=doy_thru 
}
drop from_dt thru_dt doy_from doy_thru
xfill snf_a*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

///////////   SNF push
//snf_push is the extra days added for SNF days concurrent with drug days. Capped at 10. 
gen snf_push = 0

forvalues d = 1/365{
	replace snf_push = snf_push + 1 if solif_a`d'==1 & snf_a`d'==1

	//if the solif_a`d' is 1, do nothing (already added it to the snf_push)
	//if that solif_a spot is empty:
	gen v1 = 1 if snf_push>0 & solif_a`d'==.
	replace solif_a`d' = 1 if v1==1
	replace snf_push = snf_push-1 if v1==1
	drop v1
	replace snf_push = 10 if snf_push>10
}
drop snf_a*

save `classarray', replace

/////////////////		 Bring in IP 		////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/ip/2009/ipc2009.dta", clear
keep bene_id from_dt thru_dt 

merge m:1 bene_id using `classarray'
keep if _m==3 | _m==2
drop _m
codebook bene_id

gen doy_from = doy(from_dt)
replace doy_from = 0 if year(from_dt)<2009
replace doy_from = 365 if year(from_dt)>2009
gen doy_thru = doy(thru_dt)
replace doy_thru = 0 if year(thru_dt)<2009
replace doy_thru = 365 if year(thru_dt)>2009

//////////////// Array
forvalues d = 1/365{
	gen ips_a`d' = 1 if `d'>=doy_from & `d'<=doy_thru
}
drop from_dt thru_dt doy_from doy_thru
xfill ips_a*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

///////////////  IP push
//ips_push is the extra days added for ip days concurrent with drug days
gen ips_push = 0

forvalues d = 1/365{
	replace ips_push = ips_push + 1 if solif_a`d'==1 & ips_a`d'==1

	//if the solif_a`d' is 1, do nothing (already added it to the ips_push)
	//if that solif_a spot is not full:
	gen v1 = 1 if ips_push>0 & solif_a`d'==.
	replace solif_a`d' = 1 if v1==1
	replace ips_push = ips_push-1 if v1==1
	drop v1
	replace ips_push = 10 if ips_push>10
}
drop ips_a*

////////////////  Final possession day calculations
gen solif_pdays_2009 = 0

/*The array is filled using dayssply2, which includes adjustments for early fills.
Then, the array is adjusted further for IPS and SNF days. 
So, the sum of the ones in the array is the total 2009 possession days
*/
forvalues d = 1/365{
	replace solif_pdays_2009 = solif_pdays_2009 + 1 if solif_a`d'==1
}
drop solif_a*
replace solif_pdays_2009 = 365 if solif_pdays_2009>365

//	mgs in year, for all the possession days
gen solif_totmg_2009 = solif_pdays_2009*solif_dmg_2009
//	total daily doses in the year
gen solif_totdd_2009 = solif_totmg_2009/5

//////////////// Calculate the number of extra push days that could go into next year. 
//inyear_push = extra push days from early fills throughout the year, IPS days, and SNF days. (each of which was capped at 10, but I will still cap whole thing at 30). 
//lastfill_push is the amount from the last fill that goes into next year (capped at 90). 
gen inyear_push = earlyfill_push + snf_push + ips_push
replace inyear_push = 30 if inyear_push>30 & inyear_push!=.
egen solif_push_2009 = rowmax(lastfill_push inyear_push) 
replace solif_push_2009 = 0 if solif_push_2009==.
replace solif_push_2009 = 0 if solif_push_2009<0

keep bene_id solif* 

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2009.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2009.dta", replace
keep bene_id solif_push_2009
tempfile push
save `push', replace

////////////////		2010			////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/extracts/pde/20pct/2010/opt1pde2010.dta", replace
keep bene_id srvc_dt prdsrvid dayssply qtydspns gcdf gcdf_desc str
rename prdsrvid ndc

merge m:1 ndc using `solif'
keep if _m==3 
drop _m

sum qtydspns, detail
levelsof qtydspns, clean
tab str
tab gcdf_desc

//drop observations with crazy quantity dispensed
drop if qtydspns<1 & qtydspns!=.
drop if qtydspns>365 & qtydspns!=.

merge m:1 bene_id using `push'
keep if _m==3 | _m==1
drop _m

egen idn = group(bene_id)
sort bene_id srvc_dt

//Two claims for the same bene on the same day?
/* ~38 claims out of a 1000 (for 2006 loops) are by the same person on the same day
(i.e. 19 pairs of claims). We think those prescritpions are meant to be taken together,
meaning that if it's two 30 day fills, it is worth 30 possession days, not 60. 
So, we'll take the max of the two claims on the same day, and then drop one of the 
observations. 
*/
egen claim=group(bene_id srvc_dt)

gen repeat = 1 if srvc_dt==srvc_dt[_n-1] & bene_id==bene_id[_n-1]
replace repeat = 1 if srvc_dt==srvc_dt[_n+1] & bene_id==bene_id[_n+1]

egen repeatday = max(dayssply), by(claim)
replace dayssply = repeatday if repeat==1

drop repeat repeatday claim
drop if srvc_dt==srvc_dt[_n-1] & bene_id==bene_id[_n-1]

//////////////////  Early fills pushes
//for people that fill their prescription before emptying their last, carry the extra pills forward
//extrapill_push is the amount, from that fill date, that is superfluous for reaching the next fill date. Capped at 10. 
gen doy_srvc = doy(srvc_dt)
replace doy_srvc = 0 if year(srvc_dt)<2010
replace doy_srvc = 365 if year(srvc_dt)>2010

gen extrapill_push = 0
replace extrapill_push = extrapill_push + (doy_srvc+dayssply)-(doy_srvc[_n+1]) if bene_id==bene_id[_n+1]
replace extrapill_push = 0 if extrapill_push<0
replace extrapill_push = 10 if extrapill_push>10

//pushstock is the accumulated stock of extra pills. Capped at 10. 
gen pushstock = extrapill_push
replace extrapill_push = 10 if extrapill_push>10

//add the pushstock from last year
replace pushstock = pushstock + solif_push_2009 if bene_id!=bene_id[_n-1] | _n==1

gen need = 0
replace need = (doy_srvc[_n+1])-(doy_srvc+dayssply)
replace need = (365 - (doy_srvc)+dayssply) if bene_id!=bene_id[_n+1]
replace need = 0 if need<0

gen dayssply2 = dayssply

// Creating a loop that will loop through one observation at a time and do the following
/* 1. Add the previous pushstock to the current pushstock
   2. Add the needed extra pills to the dayssply which is the dayssply + the min of either the need or the pushstock since:
	a. If need < pushstock, then need gets added to dayssply
	b. If need >= pushstock, then pushstock gets added to dayssply
   3. Calculate the new pushstock, which is the pushstock minus the need with a bottom cap at 0 and a high cap at 10 */
	
local N=_N

forvalues i=1/`N' {
	quietly replace pushstock=pushstock[_n-1]+pushstock if pushstock[_n-1]<. & bene_id==bene_id[_n-1] in `i'
	quietly replace dayssply2=dayssply2+min(need,pushstock) if bene_id==bene_id[_n-1] in `i'
	quietly replace pushstock=min(max(pushstock-need,0),10) if bene_id==bene_id[_n-1] in `i'
}

//value of early fill push stock at the end of the year
gen earlyfill_push = pushstock if bene_id!=bene_id[_n+1]
replace earlyfill_push = 0 if earlyfill_push<0
replace earlyfill_push = 10 if earlyfill_push>10 & earlyfill_push!=.
//extra pills from last prescription at end of year, capped at 90
gen lastfill_push = (doy_srvc+dayssply-365) if bene_id!=bene_id[_n+1]
replace lastfill_push = 0 if lastfill_push<0 & lastfill_push!=.
replace lastfill_push = 90 if lastfill_push>90 & lastfill_push!=.

xfill earlyfill_push lastfill_push, i(idn)

drop pushstock need extrapill_push

///////////////// Array
forvalues d = 1/365{
	gen solif_a`d' = 1 if `d'>=doy_srvc & `d'<=(doy_srvc+dayssply2) & solif==1
}

drop doy_srvc
xfill solif_a*, i(idn)

/////////////////bene-class timing
egen solif_fdays_2010 = sum(dayssply), by(idn)
egen solif_clms_2010 = count(dayssply), by(idn)
egen solif_minfilldt_2010 = min(srvc_dt), by(idn)
egen solif_maxfilldt_2010 = max(srvc_dt), by(idn)
gen solif_fillperiod_2010 = solif_maxfilldt_2010 - solif_minfilldt_2010 + 1

//////////////// weighted average dose
//claim_mgs is calculated dependent on dose form (at the claim level). 
gen claim_mgs = 0
replace claim_mgs = 5*qtydspns if str=="5MG"
replace claim_mgs = 10*qtydspns if str=="10MG"
replace claim_mgs = 5*qtydspns if str=="5 MG"
replace claim_mgs = 10*qtydspns if str=="10 MG"
//sum mgs for all claims in year by bene
egen itot_mgs = sum(claim_mgs), by(idn)
//average mgs per day supplied
gen solif_dmg_2010 = itot_mgs/solif_fdays_2010
drop claim_mgs itot_mgs

//bene level
sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

replace earlyfill_push = 0 if earlyfill_push==.
replace lastfill_push = 0 if lastfill_push==.

tempfile classarray
save `classarray', replace

/////////////////		 Bring in SNF 		////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/snf/2010/snfc2010.dta", clear
keep bene_id from_dt thru_dt

merge m:1 bene_id using `classarray'
keep if _m==3 | _m==2
drop _m
codebook bene_id

gen doy_from = doy(from_dt)
replace doy_from = 0 if year(from_dt)<2010
replace doy_from = 365 if year(from_dt)>2010
gen doy_thru = doy(thru_dt)
replace doy_thru = 0 if year(thru_dt)<2010
replace doy_thru = 365 if year(thru_dt)>2010

////////////// Array
forvalues d = 1/365{
	gen snf_a`d' = 1 if `d'>=doy_from & `d'<=doy_thru 
}
drop from_dt thru_dt doy_from doy_thru
xfill snf_a*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

///////////   SNF push
//snf_push is the extra days added for SNF days concurrent with drug days. Capped at 10. 
gen snf_push = 0

forvalues d = 1/365{
	replace snf_push = snf_push + 1 if solif_a`d'==1 & snf_a`d'==1

	//if the solif_a`d' is 1, do nothing (already added it to the snf_push)
	//if that solif_a spot is empty:
	gen v1 = 1 if snf_push>0 & solif_a`d'==.
	replace solif_a`d' = 1 if v1==1
	replace snf_push = snf_push-1 if v1==1
	drop v1
	replace snf_push = 10 if snf_push>10
}
drop snf_a*

save `classarray', replace

/////////////////		 Bring in IP 		////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/ip/2010/ipc2010.dta", clear
keep bene_id from_dt thru_dt 

merge m:1 bene_id using `classarray'
keep if _m==3 | _m==2
drop _m
codebook bene_id

gen doy_from = doy(from_dt)
replace doy_from = 0 if year(from_dt)<2010
replace doy_from = 365 if year(from_dt)>2010
gen doy_thru = doy(thru_dt)
replace doy_thru = 0 if year(thru_dt)<2010
replace doy_thru = 365 if year(thru_dt)>2010

//////////////// Array
forvalues d = 1/365{
	gen ips_a`d' = 1 if `d'>=doy_from & `d'<=doy_thru
}
drop from_dt thru_dt doy_from doy_thru
xfill ips_a*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

///////////////  IP push
//ips_push is the extra days added for ip days concurrent with drug days
gen ips_push = 0

forvalues d = 1/365{
	replace ips_push = ips_push + 1 if solif_a`d'==1 & ips_a`d'==1

	//if the solif_a`d' is 1, do nothing (already added it to the ips_push)
	//if that solif_a spot is not full:
	gen v1 = 1 if ips_push>0 & solif_a`d'==.
	replace solif_a`d' = 1 if v1==1
	replace ips_push = ips_push-1 if v1==1
	drop v1
	replace ips_push = 10 if ips_push>10
}
drop ips_a*

////////////////  Final possession day calculations
gen solif_pdays_2010 = 0

/*The array is filled using dayssply2, which includes adjustments for early fills.
Then, the array is adjusted further for IPS and SNF days. 
So, the sum of the ones in the array is the total 2010 possession days
*/
forvalues d = 1/365{
	replace solif_pdays_2010 = solif_pdays_2010 + 1 if solif_a`d'==1
}
drop solif_a*
replace solif_pdays_2010 = 365 if solif_pdays_2010>365

//	mgs in year, for all the possession days
gen solif_totmg_2010 = solif_pdays_2010*solif_dmg_2010
//	total daily doses in the year
gen solif_totdd_2010 = solif_totmg_2010/5

//////////////// Calculate the number of extra push days that could go into next year. 
//inyear_push = extra push days from early fills throughout the year, IPS days, and SNF days. (each of which was capped at 10, but I will still cap whole thing at 30). 
//lastfill_push is the amount from the last fill that goes into next year (capped at 90). 
gen inyear_push = earlyfill_push + snf_push + ips_push
replace inyear_push = 30 if inyear_push>30 & inyear_push!=.
egen solif_push_2010 = rowmax(lastfill_push inyear_push) 
replace solif_push_2010 = 0 if solif_push_2010==.
replace solif_push_2010 = 0 if solif_push_2010<0

keep bene_id solif* 

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2010.dta", replace

keep bene_id solif_push_2010
tempfile push
save `push', replace

////////////////		2011			////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/extracts/pde/20pct/2011/opt1pde2011.dta", replace
keep bene_id srvc_dt prdsrvid dayssply qtydspns gcdf gcdf_desc str
rename prdsrvid ndc

merge m:1 ndc using `solif'
keep if _m==3 
drop _m

sum qtydspns, detail
levelsof qtydspns, clean
tab str
tab gcdf_desc

//drop observations with crazy quantity dispensed
drop if qtydspns<1 & qtydspns!=.
drop if qtydspns>365 & qtydspns!=.

merge m:1 bene_id using `push'
keep if _m==3 | _m==1
drop _m

egen idn = group(bene_id)
sort bene_id srvc_dt

//Two claims for the same bene on the same day?
/* ~38 claims out of a 1000 (for 2006 loops) are by the same person on the same day
(i.e. 19 pairs of claims). We think those prescritpions are meant to be taken together,
meaning that if it's two 30 day fills, it is worth 30 possession days, not 60. 
So, we'll take the max of the two claims on the same day, and then drop one of the 
observations. 
*/
egen claim=group(bene_id srvc_dt)

gen repeat = 1 if srvc_dt==srvc_dt[_n-1] & bene_id==bene_id[_n-1]
replace repeat = 1 if srvc_dt==srvc_dt[_n+1] & bene_id==bene_id[_n+1]

egen repeatday = max(dayssply), by(claim)
replace dayssply = repeatday if repeat==1

drop repeat repeatday claim
drop if srvc_dt==srvc_dt[_n-1] & bene_id==bene_id[_n-1]

//////////////////  Early fills pushes
//for people that fill their prescription before emptying their last, carry the extra pills forward
//extrapill_push is the amount, from that fill date, that is superfluous for reaching the next fill date. Capped at 10. 
gen doy_srvc = doy(srvc_dt)
replace doy_srvc = 0 if year(srvc_dt)<2011
replace doy_srvc = 365 if year(srvc_dt)>2011

gen extrapill_push = 0
replace extrapill_push = extrapill_push + (doy_srvc+dayssply)-(doy_srvc[_n+1]) if bene_id==bene_id[_n+1]
replace extrapill_push = 0 if extrapill_push<0
replace extrapill_push = 10 if extrapill_push>10

//pushstock is the accumulated stock of extra pills. Capped at 10. 
gen pushstock = extrapill_push
replace extrapill_push = 10 if extrapill_push>10

//add the pushstock from last year
replace pushstock = pushstock + solif_push_2010 if bene_id!=bene_id[_n-1] | _n==1

gen need = 0
replace need = (doy_srvc[_n+1])-(doy_srvc+dayssply)
replace need = (365 - (doy_srvc)+dayssply) if bene_id!=bene_id[_n+1]
replace need = 0 if need<0

gen dayssply2 = dayssply

// Creating a loop that will loop through one observation at a time and do the following
/* 1. Add the previous pushstock to the current pushstock
   2. Add the needed extra pills to the dayssply which is the dayssply + the min of either the need or the pushstock since:
	a. If need < pushstock, then need gets added to dayssply
	b. If need >= pushstock, then pushstock gets added to dayssply
   3. Calculate the new pushstock, which is the pushstock minus the need with a bottom cap at 0 and a high cap at 10 */
	
local N=_N

forvalues i=1/`N' {
	quietly replace pushstock=pushstock[_n-1]+pushstock if pushstock[_n-1]<. & bene_id==bene_id[_n-1] in `i'
	quietly replace dayssply2=dayssply2+min(need,pushstock) if bene_id==bene_id[_n-1] in `i'
	quietly replace pushstock=min(max(pushstock-need,0),10) if bene_id==bene_id[_n-1] in `i'
}

//value of early fill push stock at the end of the year
gen earlyfill_push = pushstock if bene_id!=bene_id[_n+1]
replace earlyfill_push = 0 if earlyfill_push<0
replace earlyfill_push = 10 if earlyfill_push>10 & earlyfill_push!=.
//extra pills from last prescription at end of year, capped at 90
gen lastfill_push = (doy_srvc+dayssply-365) if bene_id!=bene_id[_n+1]
replace lastfill_push = 0 if lastfill_push<0 & lastfill_push!=.
replace lastfill_push = 90 if lastfill_push>90 & lastfill_push!=.

xfill earlyfill_push lastfill_push, i(idn)

drop pushstock need extrapill_push

///////////////// Array
forvalues d = 1/365{
	gen solif_a`d' = 1 if `d'>=doy_srvc & `d'<=(doy_srvc+dayssply2) & solif==1
}

drop doy_srvc
xfill solif_a*, i(idn)

/////////////////bene-class timing
egen solif_fdays_2011 = sum(dayssply), by(idn)
egen solif_clms_2011 = count(dayssply), by(idn)
egen solif_minfilldt_2011 = min(srvc_dt), by(idn)
egen solif_maxfilldt_2011 = max(srvc_dt), by(idn)
gen solif_fillperiod_2011 = solif_maxfilldt_2011 - solif_minfilldt_2011 + 1

//////////////// weighted average dose
//claim_mgs is calculated dependent on dose form (at the claim level). 
gen claim_mgs = 0
replace claim_mgs = 5*qtydspns if str=="5MG"
replace claim_mgs = 10*qtydspns if str=="10MG"
replace claim_mgs = 5*qtydspns if str=="5 MG"
replace claim_mgs = 10*qtydspns if str=="10 MG"
//sum mgs for all claims in year by bene
egen itot_mgs = sum(claim_mgs), by(idn)
//average mgs per day supplied
gen solif_dmg_2011 = itot_mgs/solif_fdays_2011
drop claim_mgs itot_mgs

//bene level
sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

replace earlyfill_push = 0 if earlyfill_push==.
replace lastfill_push = 0 if lastfill_push==.

tempfile classarray
save `classarray', replace

/////////////////		 Bring in SNF 		////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/snf/2011/snfc2011.dta", clear
keep bene_id from_dt thru_dt

merge m:1 bene_id using `classarray'
keep if _m==3 | _m==2
drop _m
codebook bene_id

gen doy_from = doy(from_dt)
replace doy_from = 0 if year(from_dt)<2011
replace doy_from = 365 if year(from_dt)>2011
gen doy_thru = doy(thru_dt)
replace doy_thru = 0 if year(thru_dt)<2011
replace doy_thru = 365 if year(thru_dt)>2011

////////////// Array
forvalues d = 1/365{
	gen snf_a`d' = 1 if `d'>=doy_from & `d'<=doy_thru 
}
drop from_dt thru_dt doy_from doy_thru
xfill snf_a*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

///////////   SNF push
//snf_push is the extra days added for SNF days concurrent with drug days. Capped at 10. 
gen snf_push = 0

forvalues d = 1/365{
	replace snf_push = snf_push + 1 if solif_a`d'==1 & snf_a`d'==1

	//if the solif_a`d' is 1, do nothing (already added it to the snf_push)
	//if that solif_a spot is empty:
	gen v1 = 1 if snf_push>0 & solif_a`d'==.
	replace solif_a`d' = 1 if v1==1
	replace snf_push = snf_push-1 if v1==1
	drop v1
	replace snf_push = 10 if snf_push>10
}
drop snf_a*

save `classarray', replace

/////////////////		 Bring in IP 		////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/ip/2011/ipc2011.dta", clear
keep bene_id from_dt thru_dt 

merge m:1 bene_id using `classarray'
keep if _m==3 | _m==2
drop _m
codebook bene_id

gen doy_from = doy(from_dt)
replace doy_from = 0 if year(from_dt)<2011
replace doy_from = 365 if year(from_dt)>2011
gen doy_thru = doy(thru_dt)
replace doy_thru = 0 if year(thru_dt)<2011
replace doy_thru = 365 if year(thru_dt)>2011

//////////////// Array
forvalues d = 1/365{
	gen ips_a`d' = 1 if `d'>=doy_from & `d'<=doy_thru
}
drop from_dt thru_dt doy_from doy_thru
xfill ips_a*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

///////////////  IP push
//ips_push is the extra days added for ip days concurrent with drug days
gen ips_push = 0

forvalues d = 1/365{
	replace ips_push = ips_push + 1 if solif_a`d'==1 & ips_a`d'==1

	//if the solif_a`d' is 1, do nothing (already added it to the ips_push)
	//if that solif_a spot is not full:
	gen v1 = 1 if ips_push>0 & solif_a`d'==.
	replace solif_a`d' = 1 if v1==1
	replace ips_push = ips_push-1 if v1==1
	drop v1
	replace ips_push = 10 if ips_push>10
}
drop ips_a*

////////////////  Final possession day calculations
gen solif_pdays_2011 = 0

/*The array is filled using dayssply2, which includes adjustments for early fills.
Then, the array is adjusted further for IPS and SNF days. 
So, the sum of the ones in the array is the total 2011 possession days
*/
forvalues d = 1/365{
	replace solif_pdays_2011 = solif_pdays_2011 + 1 if solif_a`d'==1
}
drop solif_a*
replace solif_pdays_2011 = 365 if solif_pdays_2011>365

//	mgs in year, for all the possession days
gen solif_totmg_2011 = solif_pdays_2011*solif_dmg_2011
//	total daily doses in the year
gen solif_totdd_2011 = solif_totmg_2011/5

//////////////// Calculate the number of extra push days that could go into next year. 
//inyear_push = extra push days from early fills throughout the year, IPS days, and SNF days. (each of which was capped at 10, but I will still cap whole thing at 30). 
//lastfill_push is the amount from the last fill that goes into next year (capped at 90). 
gen inyear_push = earlyfill_push + snf_push + ips_push
replace inyear_push = 30 if inyear_push>30 & inyear_push!=.
egen solif_push_2011 = rowmax(lastfill_push inyear_push) 
replace solif_push_2011 = 0 if solif_push_2011==.
replace solif_push_2011 = 0 if solif_push_2011<0

keep bene_id solif* 

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2011.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2011.dta", replace

keep bene_id solif_push_2011
tempfile push
save `push', replace

////////////////		2012			////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/extracts/pde/20pct/2012/opt1pde2012.dta", replace
keep bene_id srvc_dt prdsrvid dayssply qtydspns gcdf gcdf_desc str
rename prdsrvid ndc

merge m:1 ndc using `solif'
keep if _m==3 
drop _m

sum qtydspns, detail
levelsof qtydspns, clean
tab str
tab gcdf_desc

//drop observations with crazy quantity dispensed
drop if qtydspns<1 & qtydspns!=.
drop if qtydspns>365 & qtydspns!=.

merge m:1 bene_id using `push'
keep if _m==3 | _m==1
drop _m

egen idn = group(bene_id)
sort bene_id srvc_dt

//Two claims for the same bene on the same day?
/* ~38 claims out of a 1000 (for 2006 loops) are by the same person on the same day
(i.e. 19 pairs of claims). We think those prescritpions are meant to be taken together,
meaning that if it's two 30 day fills, it is worth 30 possession days, not 60. 
So, we'll take the max of the two claims on the same day, and then drop one of the 
observations. 
*/
egen claim=group(bene_id srvc_dt)

gen repeat = 1 if srvc_dt==srvc_dt[_n-1] & bene_id==bene_id[_n-1]
replace repeat = 1 if srvc_dt==srvc_dt[_n+1] & bene_id==bene_id[_n+1]

egen repeatday = max(dayssply), by(claim)
replace dayssply = repeatday if repeat==1

drop repeat repeatday claim
drop if srvc_dt==srvc_dt[_n-1] & bene_id==bene_id[_n-1]

//////////////////  Early fills pushes
//for people that fill their prescription before emptying their last, carry the extra pills forward
//extrapill_push is the amount, from that fill date, that is superfluous for reaching the next fill date. Capped at 10. 
gen doy_srvc = doy(srvc_dt)
replace doy_srvc = 0 if year(srvc_dt)<2012
replace doy_srvc = 365 if year(srvc_dt)>2012

gen extrapill_push = 0
replace extrapill_push = extrapill_push + (doy_srvc+dayssply)-(doy_srvc[_n+1]) if bene_id==bene_id[_n+1]
replace extrapill_push = 0 if extrapill_push<0
replace extrapill_push = 10 if extrapill_push>10

//pushstock is the accumulated stock of extra pills. Capped at 10. 
gen pushstock = extrapill_push
replace extrapill_push = 10 if extrapill_push>10

//add the pushstock from last year
replace pushstock = pushstock + solif_push_2011 if bene_id!=bene_id[_n-1] | _n==1

gen need = 0
replace need = (doy_srvc[_n+1])-(doy_srvc+dayssply)
replace need = (365 - (doy_srvc)+dayssply) if bene_id!=bene_id[_n+1]
replace need = 0 if need<0

gen dayssply2 = dayssply

// Creating a loop that will loop through one observation at a time and do the following
/* 1. Add the previous pushstock to the current pushstock
   2. Add the needed extra pills to the dayssply which is the dayssply + the min of either the need or the pushstock since:
	a. If need < pushstock, then need gets added to dayssply
	b. If need >= pushstock, then pushstock gets added to dayssply
   3. Calculate the new pushstock, which is the pushstock minus the need with a bottom cap at 0 and a high cap at 10 */
	
local N=_N

forvalues i=1/`N' {
	quietly replace pushstock=pushstock[_n-1]+pushstock if pushstock[_n-1]<. & bene_id==bene_id[_n-1] in `i'
	quietly replace dayssply2=dayssply2+min(need,pushstock) if bene_id==bene_id[_n-1] in `i'
	quietly replace pushstock=min(max(pushstock-need,0),10) if bene_id==bene_id[_n-1] in `i'
}

//value of early fill push stock at the end of the year
gen earlyfill_push = pushstock if bene_id!=bene_id[_n+1]
replace earlyfill_push = 0 if earlyfill_push<0
replace earlyfill_push = 10 if earlyfill_push>10 & earlyfill_push!=.
//extra pills from last prescription at end of year, capped at 90
gen lastfill_push = (doy_srvc+dayssply-365) if bene_id!=bene_id[_n+1]
replace lastfill_push = 0 if lastfill_push<0 & lastfill_push!=.
replace lastfill_push = 90 if lastfill_push>90 & lastfill_push!=.

xfill earlyfill_push lastfill_push, i(idn)

drop pushstock need extrapill_push

///////////////// Array
forvalues d = 1/365{
	gen solif_a`d' = 1 if `d'>=doy_srvc & `d'<=(doy_srvc+dayssply2) & solif==1
}

drop doy_srvc
xfill solif_a*, i(idn)

/////////////////bene-class timing
egen solif_fdays_2012 = sum(dayssply), by(idn)
egen solif_clms_2012 = count(dayssply), by(idn)
egen solif_minfilldt_2012 = min(srvc_dt), by(idn)
egen solif_maxfilldt_2012 = max(srvc_dt), by(idn)
gen solif_fillperiod_2012 = solif_maxfilldt_2012 - solif_minfilldt_2012 + 1

//////////////// weighted average dose
//claim_mgs is calculated dependent on dose form (at the claim level). 
gen claim_mgs = 0
replace claim_mgs = 5*qtydspns if str=="5MG"
replace claim_mgs = 10*qtydspns if str=="10MG"
replace claim_mgs = 5*qtydspns if str=="5 MG"
replace claim_mgs = 10*qtydspns if str=="10 MG"
//sum mgs for all claims in year by bene
egen itot_mgs = sum(claim_mgs), by(idn)
//average mgs per day supplied
gen solif_dmg_2012 = itot_mgs/solif_fdays_2012
drop claim_mgs itot_mgs

//bene level
sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

replace earlyfill_push = 0 if earlyfill_push==.
replace lastfill_push = 0 if lastfill_push==.

tempfile classarray
save `classarray', replace

/////////////////		 Bring in SNF 		////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/snf/2012/snfc2012.dta", clear
keep bene_id from_dt thru_dt

merge m:1 bene_id using `classarray'
keep if _m==3 | _m==2
drop _m
codebook bene_id

gen doy_from = doy(from_dt)
replace doy_from = 0 if year(from_dt)<2012
replace doy_from = 365 if year(from_dt)>2012
gen doy_thru = doy(thru_dt)
replace doy_thru = 0 if year(thru_dt)<2012
replace doy_thru = 365 if year(thru_dt)>2012

////////////// Array
forvalues d = 1/365{
	gen snf_a`d' = 1 if `d'>=doy_from & `d'<=doy_thru 
}
drop from_dt thru_dt doy_from doy_thru
xfill snf_a*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

///////////   SNF push
//snf_push is the extra days added for SNF days concurrent with drug days. Capped at 10. 
gen snf_push = 0

forvalues d = 1/365{
	replace snf_push = snf_push + 1 if solif_a`d'==1 & snf_a`d'==1

	//if the solif_a`d' is 1, do nothing (already added it to the snf_push)
	//if that solif_a spot is empty:
	gen v1 = 1 if snf_push>0 & solif_a`d'==.
	replace solif_a`d' = 1 if v1==1
	replace snf_push = snf_push-1 if v1==1
	drop v1
	replace snf_push = 10 if snf_push>10
}
drop snf_a*

save `classarray', replace

/////////////////		 Bring in IP 		////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/ip/2012/ipc2012.dta", clear
keep bene_id from_dt thru_dt 

merge m:1 bene_id using `classarray'
keep if _m==3 | _m==2
drop _m
codebook bene_id

gen doy_from = doy(from_dt)
replace doy_from = 0 if year(from_dt)<2012
replace doy_from = 365 if year(from_dt)>2012
gen doy_thru = doy(thru_dt)
replace doy_thru = 0 if year(thru_dt)<2012
replace doy_thru = 365 if year(thru_dt)>2012

//////////////// Array
forvalues d = 1/365{
	gen ips_a`d' = 1 if `d'>=doy_from & `d'<=doy_thru
}
drop from_dt thru_dt doy_from doy_thru
xfill ips_a*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

///////////////  IP push
//ips_push is the extra days added for ip days concurrent with drug days
gen ips_push = 0

forvalues d = 1/365{
	replace ips_push = ips_push + 1 if solif_a`d'==1 & ips_a`d'==1

	//if the solif_a`d' is 1, do nothing (already added it to the ips_push)
	//if that solif_a spot is not full:
	gen v1 = 1 if ips_push>0 & solif_a`d'==.
	replace solif_a`d' = 1 if v1==1
	replace ips_push = ips_push-1 if v1==1
	drop v1
	replace ips_push = 10 if ips_push>10
}
drop ips_a*

////////////////  Final possession day calculations
gen solif_pdays_2012 = 0

/*The array is filled using dayssply2, which includes adjustments for early fills.
Then, the array is adjusted further for IPS and SNF days. 
So, the sum of the ones in the array is the total 2012 possession days
*/
forvalues d = 1/365{
	replace solif_pdays_2012 = solif_pdays_2012 + 1 if solif_a`d'==1
}
drop solif_a*
replace solif_pdays_2012 = 365 if solif_pdays_2012>365

//	mgs in year, for all the possession days
gen solif_totmg_2012 = solif_pdays_2012*solif_dmg_2012
//	total daily doses in the year
gen solif_totdd_2012 = solif_totmg_2012/5

//////////////// Calculate the number of extra push days that could go into next year. 
//inyear_push = extra push days from early fills throughout the year, IPS days, and SNF days. (each of which was capped at 10, but I will still cap whole thing at 30). 
//lastfill_push is the amount from the last fill that goes into next year (capped at 90). 
gen inyear_push = earlyfill_push + snf_push + ips_push
replace inyear_push = 30 if inyear_push>30 & inyear_push!=.
egen solif_push_2012 = rowmax(lastfill_push inyear_push) 
replace solif_push_2012 = 0 if solif_push_2012==.
replace solif_push_2012 = 0 if solif_push_2012<0

keep bene_id solif* 

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2012.dta", replace

keep bene_id solif_push_2012
tempfile push
save `push', replace

////////////////		2013			////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/extracts/pde/20pct/2013/opt1pde2013.dta", replace
keep bene_id srvc_dt prdsrvid dayssply qtydspns gcdf gcdf_desc str
rename prdsrvid ndc

merge m:1 ndc using `solif'
keep if _m==3 
drop _m

sum qtydspns, detail
levelsof qtydspns, clean
tab str
tab gcdf_desc

//drop observations with crazy quantity dispensed
drop if qtydspns<1 & qtydspns!=.
drop if qtydspns>365 & qtydspns!=.

merge m:1 bene_id using `push'
keep if _m==3 | _m==1
drop _m

egen idn = group(bene_id)
sort bene_id srvc_dt

//Two claims for the same bene on the same day?
/* ~38 claims out of a 1000 (for 2006 loops) are by the same person on the same day
(i.e. 19 pairs of claims). We think those prescritpions are meant to be taken together,
meaning that if it's two 30 day fills, it is worth 30 possession days, not 60. 
So, we'll take the max of the two claims on the same day, and then drop one of the 
observations. 
*/
egen claim=group(bene_id srvc_dt)

gen repeat = 1 if srvc_dt==srvc_dt[_n-1] & bene_id==bene_id[_n-1]
replace repeat = 1 if srvc_dt==srvc_dt[_n+1] & bene_id==bene_id[_n+1]

egen repeatday = max(dayssply), by(claim)
replace dayssply = repeatday if repeat==1

drop repeat repeatday claim
drop if srvc_dt==srvc_dt[_n-1] & bene_id==bene_id[_n-1]

//////////////////  Early fills pushes
//for people that fill their prescription before emptying their last, carry the extra pills forward
//extrapill_push is the amount, from that fill date, that is superfluous for reaching the next fill date. Capped at 10. 
gen doy_srvc = doy(srvc_dt)
replace doy_srvc = 0 if year(srvc_dt)<2013
replace doy_srvc = 365 if year(srvc_dt)>2013

gen extrapill_push = 0
replace extrapill_push = extrapill_push + (doy_srvc+dayssply)-(doy_srvc[_n+1]) if bene_id==bene_id[_n+1]
replace extrapill_push = 0 if extrapill_push<0
replace extrapill_push = 10 if extrapill_push>10

//pushstock is the accumulated stock of extra pills. Capped at 10. 
gen pushstock = extrapill_push
replace extrapill_push = 10 if extrapill_push>10

//add the pushstock from last year
replace pushstock = pushstock + solif_push_2012 if bene_id!=bene_id[_n-1] | _n==1

gen need = 0
replace need = (doy_srvc[_n+1])-(doy_srvc+dayssply)
replace need = (365 - (doy_srvc)+dayssply) if bene_id!=bene_id[_n+1]
replace need = 0 if need<0

gen dayssply2 = dayssply

// Creating a loop that will loop through one observation at a time and do the following
/* 1. Add the previous pushstock to the current pushstock
   2. Add the needed extra pills to the dayssply which is the dayssply + the min of either the need or the pushstock since:
	a. If need < pushstock, then need gets added to dayssply
	b. If need >= pushstock, then pushstock gets added to dayssply
   3. Calculate the new pushstock, which is the pushstock minus the need with a bottom cap at 0 and a high cap at 10 */
	
local N=_N

forvalues i=1/`N' {
	quietly replace pushstock=pushstock[_n-1]+pushstock if pushstock[_n-1]<. & bene_id==bene_id[_n-1] in `i'
	quietly replace dayssply2=dayssply2+min(need,pushstock) if bene_id==bene_id[_n-1] in `i'
	quietly replace pushstock=min(max(pushstock-need,0),10) if bene_id==bene_id[_n-1] in `i'
}

//value of early fill push stock at the end of the year
gen earlyfill_push = pushstock if bene_id!=bene_id[_n+1]
replace earlyfill_push = 0 if earlyfill_push<0
replace earlyfill_push = 10 if earlyfill_push>10 & earlyfill_push!=.
//extra pills from last prescription at end of year, capped at 90
gen lastfill_push = (doy_srvc+dayssply-365) if bene_id!=bene_id[_n+1]
replace lastfill_push = 0 if lastfill_push<0 & lastfill_push!=.
replace lastfill_push = 90 if lastfill_push>90 & lastfill_push!=.

xfill earlyfill_push lastfill_push, i(idn)

drop pushstock need extrapill_push

///////////////// Array
forvalues d = 1/365{
	gen solif_a`d' = 1 if `d'>=doy_srvc & `d'<=(doy_srvc+dayssply2) & solif==1
}

drop doy_srvc
xfill solif_a*, i(idn)

/////////////////bene-class timing
egen solif_fdays_2013 = sum(dayssply), by(idn)
egen solif_clms_2013 = count(dayssply), by(idn)
egen solif_minfilldt_2013 = min(srvc_dt), by(idn)
egen solif_maxfilldt_2013 = max(srvc_dt), by(idn)
gen solif_fillperiod_2013 = solif_maxfilldt_2013 - solif_minfilldt_2013 + 1

//////////////// weighted average dose
//claim_mgs is calculated dependent on dose form (at the claim level). 
gen claim_mgs = 0
replace claim_mgs = 5*qtydspns if str=="5MG"
replace claim_mgs = 10*qtydspns if str=="10MG"
replace claim_mgs = 5*qtydspns if str=="5 MG"
replace claim_mgs = 10*qtydspns if str=="10 MG"
//sum mgs for all claims in year by bene
egen itot_mgs = sum(claim_mgs), by(idn)
//average mgs per day supplied
gen solif_dmg_2013 = itot_mgs/solif_fdays_2013
drop claim_mgs itot_mgs

//bene level
sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

replace earlyfill_push = 0 if earlyfill_push==.
replace lastfill_push = 0 if lastfill_push==.

tempfile classarray
save `classarray', replace

/////////////////		 Bring in SNF 		////////////////////////////////////
use "/disk/agedisk3/medicare/data/20pct/snf/2013/snfc2013.dta", clear
keep bene_id from_dt thru_dt

merge m:1 bene_id using `classarray'
keep if _m==3 | _m==2
drop _m
codebook bene_id

gen doy_from = doy(from_dt)
replace doy_from = 0 if year(from_dt)<2013
replace doy_from = 365 if year(from_dt)>2013
gen doy_thru = doy(thru_dt)
replace doy_thru = 0 if year(thru_dt)<2013
replace doy_thru = 365 if year(thru_dt)>2013

////////////// Array
forvalues d = 1/365{
	gen snf_a`d' = 1 if `d'>=doy_from & `d'<=doy_thru 
}
drop from_dt thru_dt doy_from doy_thru
xfill snf_a*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

///////////   SNF push
//snf_push is the extra days added for SNF days concurrent with drug days. Capped at 10. 
gen snf_push = 0

forvalues d = 1/365{
	replace snf_push = snf_push + 1 if solif_a`d'==1 & snf_a`d'==1

	//if the solif_a`d' is 1, do nothing (already added it to the snf_push)
	//if that solif_a spot is empty:
	gen v1 = 1 if snf_push>0 & solif_a`d'==.
	replace solif_a`d' = 1 if v1==1
	replace snf_push = snf_push-1 if v1==1
	drop v1
	replace snf_push = 10 if snf_push>10
}
drop snf_a*

save `classarray', replace

/////////////////		 Bring in IP 		////////////////////////////////////
use "/disk/agedisk3/medicare/data/20pct/ip/2013/ipc2013.dta", clear
keep bene_id from_dt thru_dt 

merge m:1 bene_id using `classarray'
keep if _m==3 | _m==2
drop _m
codebook bene_id

gen doy_from = doy(from_dt)
replace doy_from = 0 if year(from_dt)<2013
replace doy_from = 365 if year(from_dt)>2013
gen doy_thru = doy(thru_dt)
replace doy_thru = 0 if year(thru_dt)<2013
replace doy_thru = 365 if year(thru_dt)>2013

//////////////// Array
forvalues d = 1/365{
	gen ips_a`d' = 1 if `d'>=doy_from & `d'<=doy_thru
}
drop from_dt thru_dt doy_from doy_thru
xfill ips_a*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

///////////////  IP push
//ips_push is the extra days added for ip days concurrent with drug days
gen ips_push = 0

forvalues d = 1/365{
	replace ips_push = ips_push + 1 if solif_a`d'==1 & ips_a`d'==1

	//if the solif_a`d' is 1, do nothing (already added it to the ips_push)
	//if that solif_a spot is not full:
	gen v1 = 1 if ips_push>0 & solif_a`d'==.
	replace solif_a`d' = 1 if v1==1
	replace ips_push = ips_push-1 if v1==1
	drop v1
	replace ips_push = 10 if ips_push>10
}
drop ips_a*

////////////////  Final possession day calculations
gen solif_pdays_2013 = 0

/*The array is filled using dayssply2, which includes adjustments for early fills.
Then, the array is adjusted further for IPS and SNF days. 
So, the sum of the ones in the array is the total 2013 possession days
*/
forvalues d = 1/365{
	replace solif_pdays_2013 = solif_pdays_2013 + 1 if solif_a`d'==1
}
drop solif_a*
replace solif_pdays_2013 = 365 if solif_pdays_2013>365

//	mgs in year, for all the possession days
gen solif_totmg_2013 = solif_pdays_2013*solif_dmg_2013
//	total daily doses in the year
gen solif_totdd_2013 = solif_totmg_2013/5

//////////////// Calculate the number of extra push days that could go into next year. 
//inyear_push = extra push days from early fills throughout the year, IPS days, and SNF days. (each of which was capped at 10, but I will still cap whole thing at 30). 
//lastfill_push is the amount from the last fill that goes into next year (capped at 90). 
gen inyear_push = earlyfill_push + snf_push + ips_push
replace inyear_push = 30 if inyear_push>30 & inyear_push!=.
egen solif_push_2013 = rowmax(lastfill_push inyear_push) 
replace solif_push_2013 = 0 if solif_push_2013==.
replace solif_push_2013 = 0 if solif_push_2013<0

keep bene_id solif* 

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2013.dta", replace
*/
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2013.dta", replace

keep bene_id solif_push_2013
tempfile push
save `push', replace

////////////////		2014			////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/extracts/pde/20pct/2014/opt1pde2014.dta", replace
keep bene_id srvc_dt prdsrvid dayssply qtydspns gcdf gcdf_desc str
rename prdsrvid ndc

merge m:1 ndc using `solif'
keep if _m==3 
drop _m

sum qtydspns, detail
levelsof qtydspns, clean
tab str
tab gcdf_desc

//drop observations with crazy quantity dispensed
drop if qtydspns<1 & qtydspns!=.
drop if qtydspns>365 & qtydspns!=.

merge m:1 bene_id using `push'
keep if _m==3 | _m==1
drop _m

egen idn = group(bene_id)
sort bene_id srvc_dt

//Two claims for the same bene on the same day?
/* ~38 claims out of a 1000 (for 2006 loops) are by the same person on the same day
(i.e. 19 pairs of claims). We think those prescritpions are meant to be taken together,
meaning that if it's two 30 day fills, it is worth 30 possession days, not 60. 
So, we'll take the max of the two claims on the same day, and then drop one of the 
observations. 
*/
egen claim=group(bene_id srvc_dt)

gen repeat = 1 if srvc_dt==srvc_dt[_n-1] & bene_id==bene_id[_n-1]
replace repeat = 1 if srvc_dt==srvc_dt[_n+1] & bene_id==bene_id[_n+1]

egen repeatday = max(dayssply), by(claim)
replace dayssply = repeatday if repeat==1

drop repeat repeatday claim
drop if srvc_dt==srvc_dt[_n-1] & bene_id==bene_id[_n-1]

//////////////////  Early fills pushes
//for people that fill their prescription before emptying their last, carry the extra pills forward
//extrapill_push is the amount, from that fill date, that is superfluous for reaching the next fill date. Capped at 10. 
gen doy_srvc = doy(srvc_dt)
replace doy_srvc = 0 if year(srvc_dt)<2014
replace doy_srvc = 365 if year(srvc_dt)>2014

gen extrapill_push = 0
replace extrapill_push = extrapill_push + (doy_srvc+dayssply)-(doy_srvc[_n+1]) if bene_id==bene_id[_n+1]
replace extrapill_push = 0 if extrapill_push<0
replace extrapill_push = 10 if extrapill_push>10

//pushstock is the accumulated stock of extra pills. Capped at 10. 
gen pushstock = extrapill_push
replace extrapill_push = 10 if extrapill_push>10

//add the pushstock from last year
replace pushstock = pushstock + solif_push_2013 if bene_id!=bene_id[_n-1] | _n==1

gen need = 0
replace need = (doy_srvc[_n+1])-(doy_srvc+dayssply)
replace need = (365 - (doy_srvc)+dayssply) if bene_id!=bene_id[_n+1]
replace need = 0 if need<0

gen dayssply2 = dayssply

// Creating a loop that will loop through one observation at a time and do the following
/* 1. Add the previous pushstock to the current pushstock
   2. Add the needed extra pills to the dayssply which is the dayssply + the min of either the need or the pushstock since:
	a. If need < pushstock, then need gets added to dayssply
	b. If need >= pushstock, then pushstock gets added to dayssply
   3. Calculate the new pushstock, which is the pushstock minus the need with a bottom cap at 0 and a high cap at 10 */
	
local N=_N

forvalues i=1/`N' {
	quietly replace pushstock=pushstock[_n-1]+pushstock if pushstock[_n-1]<. & bene_id==bene_id[_n-1] in `i'
	quietly replace dayssply2=dayssply2+min(need,pushstock) if bene_id==bene_id[_n-1] in `i'
	quietly replace pushstock=min(max(pushstock-need,0),10) if bene_id==bene_id[_n-1] in `i'
}

//value of early fill push stock at the end of the year
gen earlyfill_push = pushstock if bene_id!=bene_id[_n+1]
replace earlyfill_push = 0 if earlyfill_push<0
replace earlyfill_push = 10 if earlyfill_push>10 & earlyfill_push!=.
//extra pills from last prescription at end of year, capped at 90
gen lastfill_push = (doy_srvc+dayssply-365) if bene_id!=bene_id[_n+1]
replace lastfill_push = 0 if lastfill_push<0 & lastfill_push!=.
replace lastfill_push = 90 if lastfill_push>90 & lastfill_push!=.

xfill earlyfill_push lastfill_push, i(idn)

drop pushstock need extrapill_push

///////////////// Array
forvalues d = 1/365{
	gen solif_a`d' = 1 if `d'>=doy_srvc & `d'<=(doy_srvc+dayssply2) & solif==1
}

drop doy_srvc
xfill solif_a*, i(idn)

/////////////////bene-class timing
egen solif_fdays_2014 = sum(dayssply), by(idn)
egen solif_clms_2014 = count(dayssply), by(idn)
egen solif_minfilldt_2014 = min(srvc_dt), by(idn)
egen solif_maxfilldt_2014 = max(srvc_dt), by(idn)
gen solif_fillperiod_2014 = solif_maxfilldt_2014 - solif_minfilldt_2014 + 1

//////////////// weighted average dose
//claim_mgs is calculated dependent on dose form (at the claim level). 
gen claim_mgs = 0
replace claim_mgs = 5*qtydspns if str=="5MG"
replace claim_mgs = 10*qtydspns if str=="10MG"
replace claim_mgs = 5*qtydspns if str=="5 MG"
replace claim_mgs = 10*qtydspns if str=="10 MG"
//sum mgs for all claims in year by bene
egen itot_mgs = sum(claim_mgs), by(idn)
//average mgs per day supplied
gen solif_dmg_2014 = itot_mgs/solif_fdays_2014
drop claim_mgs itot_mgs

//bene level
sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

replace earlyfill_push = 0 if earlyfill_push==.
replace lastfill_push = 0 if lastfill_push==.

tempfile classarray
save `classarray', replace

/////////////////		 Bring in SNF 		////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/snf/2014/snfc2014.dta", replace
keep bene_id from_dt thru_dt

merge m:1 bene_id using `classarray'
keep if _m==3 | _m==2
drop _m
codebook bene_id

gen doy_from = doy(from_dt)
replace doy_from = 0 if year(from_dt)<2014
replace doy_from = 365 if year(from_dt)>2014
gen doy_thru = doy(thru_dt)
replace doy_thru = 0 if year(thru_dt)<2014
replace doy_thru = 365 if year(thru_dt)>2014

////////////// Array
forvalues d = 1/365{
	gen snf_a`d' = 1 if `d'>=doy_from & `d'<=doy_thru 
}
drop from_dt thru_dt doy_from doy_thru
xfill snf_a*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

///////////   SNF push
//snf_push is the extra days added for SNF days concurrent with drug days. Capped at 10. 
gen snf_push = 0

forvalues d = 1/365{
	replace snf_push = snf_push + 1 if solif_a`d'==1 & snf_a`d'==1

	//if the solif_a`d' is 1, do nothing (already added it to the snf_push)
	//if that solif_a spot is empty:
	gen v1 = 1 if snf_push>0 & solif_a`d'==.
	replace solif_a`d' = 1 if v1==1
	replace snf_push = snf_push-1 if v1==1
	drop v1
	replace snf_push = 10 if snf_push>10
}
drop snf_a*

save `classarray', replace

/////////////////		 Bring in IP 		////////////////////////////////////
use "/disk/agedisk1/medicare/data/u/c/20pct/ip/2014/ipc2014.dta", replace
keep bene_id from_dt thru_dt 

merge m:1 bene_id using `classarray'
keep if _m==3 | _m==2
drop _m
codebook bene_id

gen doy_from = doy(from_dt)
replace doy_from = 0 if year(from_dt)<2014
replace doy_from = 365 if year(from_dt)>2014
gen doy_thru = doy(thru_dt)
replace doy_thru = 0 if year(thru_dt)<2014
replace doy_thru = 365 if year(thru_dt)>2014

//////////////// Array
forvalues d = 1/365{
	gen ips_a`d' = 1 if `d'>=doy_from & `d'<=doy_thru
}
drop from_dt thru_dt doy_from doy_thru
xfill ips_a*, i(idn)

sort bene_id
drop if bene_id==bene_id[_n-1]
codebook bene_id

///////////////  IP push
//ips_push is the extra days added for ip days concurrent with drug days
gen ips_push = 0

forvalues d = 1/365{
	replace ips_push = ips_push + 1 if solif_a`d'==1 & ips_a`d'==1

	//if the solif_a`d' is 1, do nothing (already added it to the ips_push)
	//if that solif_a spot is not full:
	gen v1 = 1 if ips_push>0 & solif_a`d'==.
	replace solif_a`d' = 1 if v1==1
	replace ips_push = ips_push-1 if v1==1
	drop v1
	replace ips_push = 10 if ips_push>10
}
drop ips_a*

////////////////  Final possession day calculations
gen solif_pdays_2014 = 0

/*The array is filled using dayssply2, which includes adjustments for early fills.
Then, the array is adjusted further for IPS and SNF days. 
So, the sum of the ones in the array is the total 2014 possession days
*/
forvalues d = 1/365{
	replace solif_pdays_2014 = solif_pdays_2014 + 1 if solif_a`d'==1
}
drop solif_a*
replace solif_pdays_2014 = 365 if solif_pdays_2014>365

//	mgs in year, for all the possession days
gen solif_totmg_2014 = solif_pdays_2014*solif_dmg_2014
//	total daily doses in the year
gen solif_totdd_2014 = solif_totmg_2014/5

//////////////// Calculate the number of extra push days that could go into next year. 
//inyear_push = extra push days from early fills throughout the year, IPS days, and SNF days. (each of which was capped at 10, but I will still cap whole thing at 30). 
//lastfill_push is the amount from the last fill that goes into next year (capped at 90). 
gen inyear_push = earlyfill_push + snf_push + ips_push
replace inyear_push = 30 if inyear_push>30 & inyear_push!=.
egen solif_push_2014 = rowmax(lastfill_push inyear_push) 
replace solif_push_2014 = 0 if solif_push_2014==.
replace solif_push_2014 = 0 if solif_push_2014<0

keep bene_id solif* 

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2014.dta", replace

keep bene_id solif_push_2014
tempfile push
save `push', replace

