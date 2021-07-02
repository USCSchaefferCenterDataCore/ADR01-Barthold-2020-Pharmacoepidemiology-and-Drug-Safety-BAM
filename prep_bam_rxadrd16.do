clear all
set more off
capture log close

/*
•	Input: bam_0614.dta, bamsamp_0716.dta, oab_0216.dta, adrdv4_0816.dta
o		ccw_0714.dta, geoses_0714.dta, 
o		bene_hccscoresYYYY.dta (07-09), bsfcuYYYY.dta (07-09)
o		adrx_YYYY.dta (07-12), statcce_YYYYp.dta (07-09), aht0709.dta 
•	Output: bam_rxadrd16.dta

Indivs with 2 claims of BAM in one year, 07-09
o	Must have PTD 07-09, insamp 09
o	No ADRD(AD) prior to 2010
o	If they have an ADRD dx
		Must have FFS at time of any ADRD dx.
	No ADRx use prior to ADRD dx. 
	
Bring in/make controls:
o	HCC, phyvis, comorbidities, geo-inc-edu, statins, aht, 

Make drug exposure variables
*/

////////////////////////////////////////////////////////////////////////////////
//////////////////		drugs insamp + dx info				////////////////////
////////////////////////////////////////////////////////////////////////////////

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_0614.dta", replace
keep bene_id year_bammax year_bammin ///
		bam_d nsl_d nsl2_d m3s_d darif_d fesot_d flavo_d oxybu_d solif_d tolte_d trosp_d ///
		bam_y nsl_y nsl2_y m3s_y darif_y fesot_y flavo_y oxybu_y solif_y tolte_y trosp_y ///
		bam_pdays_2* nsl_pdays_2* nsl2_pdays_2* m3s_pdays_2* darif_pdays_2* fesot_pdays_2* flavo_pdays_2* oxybu_pdays_2* solif_pdays_2* tolte_pdays_2* trosp_pdays_2* ///
		bam_fdays_2* nsl_fdays_2* nsl2_fdays_2* m3s_fdays_2* darif_fdays_2* fesot_fdays_2* flavo_fdays_2* oxybu_fdays_2* solif_fdays_2* tolte_fdays_2* trosp_fdays_2* ///
		bam_clms_2* nsl_clms_2* nsl2_clms_2* m3s_clms_2* darif_clms_2* fesot_clms_2* flavo_clms_2* oxybu_clms_2* solif_clms_2* tolte_clms_2* trosp_clms_2* ///
		bam_totdd_2* nsl_totdd_2* nsl2_totdd_2* m3s_totdd_2* darif_totdd_2* fesot_totdd_2* flavo_totdd_2* oxybu_totdd_2* solif_totdd_2* tolte_totdd_2* trosp_totdd_2* ///
		bam_tdd_2* nsl_tdd_2* nsl2_tdd_2* m3s_tdd_2* darif_tdd_2* fesot_tdd_2* flavo_tdd_2* oxybu_tdd_2* solif_tdd_2* tolte_tdd_2* trosp_tdd_2* 

tempfile bam
save `bam', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_0716.dta", replace
merge 1:1 bene_id using `bam'
keep if _m==3		//must use a drug, and have enrollment info from the samp file
drop _m
tab bam_y

save `bam', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oab_0216.dta", replace
tab oab_y

merge 1:1 bene_id using `bam'
drop if _m==1 		//drop those who are only in the dx file
drop _m

//Fill in drug zeros
local drugs "bam nsl nsl2 m3s darif fesot flavo oxybu solif tolte trosp"
foreach i in `drugs'{
	forvalues j=2006/2014{
		replace `i'_pdays_`j' = 0 if `i'_pdays_`j'==.
		replace `i'_clms_`j' = 0 if `i'_clms_`j'==.
		replace `i'_totdd_`j' = 0 if `i'_totdd_`j'==.
		replace `i'_tdd_`j' = 0 if `i'_tdd_`j'==.
		replace `i'_fdays_`j' = 0 if `i'_fdays_`j'==.
	}
}

//First year with two claims, within 07-09
gen bam_y2 = .
forvalues i=2009(-1)2007{
	replace bam_y2 = `i' if bam_clms_`i'>=2
}

//zeros for specific dx
local icddx "59651 59654 59659 7881 7883 7884 78863"
foreach i in `icddx'{
	replace icd`i' = 0 if icd`i'==.
}
count
tab bam_y
tab oab_y
//Keep if they had PTD all three years 07-09
keep if ptd2007==1 & ptd2008==1 & ptd2009==1
//Keep if insamp 09 (FFS 07-09, age 09 67+, no drop flag)
keep if insamp_2009==1

count
tab bam_y
tab bam_y2
sum bam_clms_20*, detail
sum bam_pdays_20*

//Keep if they had two claims in any of the three years 07-09
keep if bam_clms_2007>=2 | bam_clms_2008>=2 | bam_clms_2009>=2
count
tab bam_y
tab bam_y2
sum bam_clms_20*, detail
sum bam_pdays_20*, detail

//age in year t (first year with 2 claims)
gen age = .
replace age = age_beg_2007 if bam_y2==2007
replace age = age_beg_2008 if bam_y2==2008
replace age = age_beg_2009 if bam_y2==2009

gen agesq = age^2 

save `bam', replace

////////////////////////////////////////////////////////////////////////////////
//////////////////				CCW dx							////////////////
////////////////////////////////////////////////////////////////////////////////

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/ccw_0714.dta", replace
keep bene_id amie atrialfe diabtese strktiae hypert_ever hyperl_ever
rename hypert_ever hypt_d
rename hyperl_ever hypl_d
rename amie ami_d
rename atrialfe atf_d
rename diabtese dia_d
rename strktiae str_d

merge 1:1 bene_id using `bam'
keep if _m==3
drop _m
save `bam', replace

////////////////////////////////////////////////////////////////////////////////
//////////////////				Patricia's dx file				////////////////
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/ferido-dua51866/AD/data/aht/base/adrdv4_0816.dta", replace
keep bene_id alzhe alzhdmte ADv_dt ADRDv_dt 
rename alzhe ADccw_dt
rename alzhdmte ADRDccw_dt

merge 1:1 bene_id using `bam'
drop if _m==1		//drop if in AD file only
drop _m

gen ADccw_yr = year(ADccw_dt)
gen ADRDccw_yr = year(ADRDccw_dt)
gen ADv_yr = year(ADv_dt)
gen ADRDv_yr = year(ADRDv_dt)

gen ADRDccw = 0
replace ADRDccw =1 if ADRDccw_dt!=.
gen ADccw = 0
replace ADccw =1 if ADccw_dt!=.
gen ADRDv = 0
replace ADRDv =1 if ADRDv_dt!=.
gen ADv = 0
replace ADv =1 if ADv_dt!=.

drop if ADRDccw_yr<2010
drop if ADRDv_yr<2010
tab bam_y
tab bam_y2

////////////////////////////////////////////////////////////////////////////////
//////////////////				Timing variables				////////////////
////////////////////////////////////////////////////////////////////////////////
/* 
firstday is the first day of 2007
lastday is either your AD_d, death_d, or last day of the year you were censored

lastdoo is the the last day of the last year you were insamp, or your death day, if that came earlier.
		(anyone that left FFS before death has the day they left FFS as their lastdoo)
		insamp just means you had FFS in t, t-1, t-2, age>=67, no drop flag 
*/
gen firstday = 17167
gen lastday = lastdoo if ADccw==0 
replace lastday = ADccw_d if ADccw==1

count
sum firstday lastday

gen daysobserved = .
replace daysobserved = lastday-firstday

//Must have FFS up to AD date, death date, or censorship date.
drop if ADccw==1 & ADccw_y>year_insampmax & ADccw_y!=.
drop if year(lastday)<2010
//lastday already incorporates death and censorship restrictions from insamp

count
tab bam_y
tab bam_y2

//Years between oabui dx and first bam use
gen rxmdxy = bam_y2 - oab_y + 1
replace rxmdxy = 0 if rxmdxy==.
replace rxmdxy = 0 if rxmdxy<0

//intermediate data set
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_rxadrd16a.dta.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_rxadrd16a.dta.dta", replace
tempfile bam
save `bam', replace

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
//////////////////						 						////////////////
//////////////////				Other exclusions				////////////////
//////////////////						 						////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
//				Parkinson's
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
//				Prior use of ADRx
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/ferido-dua51866/AD/data/aht/addrug/adrx_2007.dta"
keep bene_id adrx_clms
rename adrx_clms adrx_clms_2007
tempfile adrx
save `adrx', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/ferido-dua51866/AD/data/aht/addrug/adrx_2008.dta"
keep bene_id adrx_clms
rename adrx_clms adrx_clms_2008
merge 1:1 bene_id using `adrx'
drop _m
save `adrx', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/ferido-dua51866/AD/data/aht/addrug/adrx_2009.dta"
keep bene_id adrx_clms
rename adrx_clms adrx_clms_2009
merge 1:1 bene_id using `adrx'
drop _m
save `adrx', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/ferido-dua51866/AD/data/aht/addrug/adrx_2010.dta"
keep bene_id adrx_clms
rename adrx_clms adrx_clms_2010
merge 1:1 bene_id using `adrx'
drop _m
save `adrx', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/ferido-dua51866/AD/data/aht/addrug/adrx_2011.dta"
keep bene_id adrx_clms
rename adrx_clms adrx_clms_2011
merge 1:1 bene_id using `adrx'
drop _m
save `adrx', replace
use "/disk/agedisk3/medicare.work/goldman-DUA51866/ferido-dua51866/AD/data/aht/addrug/adrx_2012.dta"
keep bene_id adrx_clms
rename adrx_clms adrx_clms_2012
merge 1:1 bene_id using `adrx'
drop _m

merge 1:1 bene_id using `bam'
drop if _m==1 //dropping those only in adrx file 
drop _m

forvalues i=2007/2012{
	replace adrx_clms_`i' = 0 if adrx_clms_`i'==.
}
gen adrx_pdx = 0
replace adrx_pdx = 1 if adrx_clms_2007>=2 | adrx_clms_2008>=2 | adrx_clms_2009>=2 
forvalues i=2010/2012{
	replace adrx_pdx = 1 if ADRDccw_y==`i' & adrx_clms_`i-1'>=2
}
gen adrx_p2010 = 0 
replace adrx_p2010 = 1 if adrx_clms_2007>=2 | adrx_clms_2008>=2 | adrx_clms_2009>=2 

tab adrx_pdx
tab adrx_p2010

drop if adrx_p2010==1

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
//////////////////						 						////////////////
//////////////////				Covariates 						////////////////
//////////////////						 						////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
//////////////////				OAB dx	 						////////////////
////////////////////////////////////////////////////////////////////////////////
/*
OAB http://www.icd9data.com/2014/Volume1/580-629/590-599/596/default.htm
	596.51 – hypertonicity of bladder (OAB)
	596.54 – neurogenic bladder 
	596.59 – other functional disorder of the bladder 
UI http://www.icd9data.com/2014/Volume1/780-799/780-789/788/default.htm 
	788.1 – dysuria 
	788.3 – urinary incontinence
		Include 30-39. 30 Urinary incontinence, 31 Urge incontinence. 33 Mixed incontinence (male) (female). 34 Incontinence without sensory awareness. 36 Nocturnal enuresis. 39 Other urinary incontinence.
		EXCEPT: 32 Stress incontinence, male. 35 Post-void dribbling. 37 Continuous leakage. 38 Overflow incontinence.
	788.4 – Frequency of urination and polyuria
		Include 41-43: 41 urinary frequency, 42 polyuria, 43 nocturia. 
	788.63 – urgency of urination
*/
//any dx
	gen bldx_oabui = 0
	replace bldx_oabui = 1 if icd59651==1 | icd59654==1 | icd59659==1 | icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1
//no dx
	gen bldx_none = 0
	replace bldx_none = 1 if bldx_oabui==0
//oab
	gen bldx_oab = 0
	replace bldx_oab = 1 if icd59651==1 | icd59654==1 | icd59659==1 
	//hypertonicity of bladder 
		gen bldx_hb = 0
		replace bldx_hb = 1 if icd59651==1
	//neurogenic bladder
		gen bldx_nb = 0
		replace bldx_nb = 1 if icd59654==1
	//other functional disorder of the bladder
		gen bldx_oth = 0
		replace bldx_oth = 1 if icd59659==1
//ui
	gen bldx_ui = 0
	replace bldx_ui = 1 if icd7881==1 | icd7883==1 | icd7884==1 | icd78863==1
	//dysuria
		gen bldx_dis = 0
		replace bldx_dis = 1 if icd7881==1
	//ui
		gen bldx_uis = 0
		replace bldx_uis = 1 if icd7883==1
	//frequency of urination and polyuria
		gen bldx_fup = 0
		replace bldx_fup = 1 if icd7884==1
	//urgency of urination
		gen bldx_urg = 0
		replace bldx_urg = 1 if icd78863==1

save `bam', replace

////////////////////////////////////////////////////////////////////////////////
//				HCC - measure in year t
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/HealthStatus/HCCscores/bene_hccscores2007.dta", clear
keep bene_id resolved_hccyr
rename resolved_hccyr hcc_2007
merge 1:1 bene_id using `bam'
drop if _m==1 //drop those only in HCC file
drop _m
save `bam', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/HealthStatus/HCCscores/bene_hccscores2008.dta", clear
keep bene_id resolved_hccyr
rename resolved_hccyr hcc_2008
merge 1:1 bene_id using `bam'
drop if _m==1 //drop those only in HCC file
drop _m
save `bam', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/HealthStatus/HCCscores/bene_hccscores2009.dta", clear
keep bene_id resolved_hccyr
rename resolved_hccyr hcc_2009
merge 1:1 bene_id using `bam'
drop if _m==1 //drop those only in HCC file
drop _m

drop if hcc_2007==. & hcc_2008==. & hcc_2009==.

gen hcc = .
replace hcc = hcc_2007 if bam_y2==2007
replace hcc = hcc_2008 if bam_y2==2008
replace hcc = hcc_2009 if bam_y2==2009

sum hcc if hcc!=., detail
gen hcc4 = .
replace hcc4 = 1 if hcc>=r(min) & hcc<r(p25)
replace hcc4 = 2 if hcc>=r(p25) & hcc<r(p50)
replace hcc4 = 3 if hcc>=r(p50) & hcc<r(p75)
replace hcc4 = 4 if hcc>=r(p75) & hcc!=.

save `bam', replace

////////////////////////////////////////////////////////////////////////////////
//				Phyvis – measure in year t
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk2/medicare/data/20pct/bsf/2007/bsfcu2007.dta", replace
keep bene_id phys_events
rename phys_events phys_events_2007
merge 1:1 bene_id using `bam'
drop if _m==1 //drop those on in phy file
drop _m
save `bam', replace

use "/disk/agedisk2/medicare/data/20pct/bsf/2008/bsfcu2008.dta", replace
keep bene_id phys_events
rename phys_events phys_events_2008
merge 1:1 bene_id using `bam'
drop if _m==1 //drop those on in phy file
drop _m
save `bam', replace

use "/disk/agedisk2/medicare/data/20pct/bsf/2009/bsfcu2009.dta", replace
keep bene_id phys_events
rename phys_events phys_events_2009
merge 1:1 bene_id using `bam'
drop if _m==1 //drop those on in phy file
drop _m

forvalues y=2007/2009{
	replace phys_events_`y' = 0 if phys_events_`y'==.
}

gen phy = .
replace phy = phys_events_2007 if bam_y2==2007
replace phy = phys_events_2008 if bam_y2==2008
replace phy = phys_events_2009 if bam_y2==2009
replace phy = 0 if phy==.

sum phy if phy!=., detail
gen phy4 = .
replace phy4 = 1 if phy>=r(min) & phy<r(p25)
replace phy4 = 2 if phy>=r(p25) & phy<r(p50)
replace phy4 = 3 if phy>=r(p50) & phy<r(p75)
replace phy4 = 4 if phy>=r(p75) & phy!=.

save `bam', replace

////////////////////////////////////////////////////////////////////////////////
//				Comorbidities - diagnosed before day d (when they had 2 BAMs)
////////////////////////////////////////////////////////////////////////////////
local como "hypt hypl ami atf dia str"
foreach i in `como'{
	gen cmd_`i'_bbam = 0
	replace cmd_`i'_bbam = 1 if year(`i'_d) < bam_y2
}

save `bam', replace

////////////////////////////////////////////////////////////////////////////////
//				Geography - income, education, county, measured at earliest in 07-14
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/geoses_0714.dta", replace

keep bene_id fips_state fips_county zip3 zip5 cen4 zcta5 pct_hsg medinc
rename fips_county county
rename fips_state state

merge 1:1 bene_id using `bam'
keep if _m==3 //need geo info for everyone
drop _m

drop if county==""

sum pct_hsg if pct_hsg!=., detail
gen edu4 = .
replace edu4 = 1 if pct_hsg>=r(min) & pct_hsg<r(p25) 
replace edu4 = 2 if pct_hsg>=r(p25) & pct_hsg<r(p50) 
replace edu4 = 3 if pct_hsg>=r(p50) & pct_hsg<r(p75) 
replace edu4 = 4 if pct_hsg>=r(p75) & pct_hsg!=. 

sum medinc if medinc!=., detail
gen inc4 = .
replace inc4 = 1 if medinc>=r(min) & medinc<r(p25) 
replace inc4 = 2 if medinc>=r(p25) & medinc<r(p50) 
replace inc4 = 3 if medinc>=r(p50) & medinc<r(p75) 
replace inc4 = 4 if medinc>=r(p75) & medinc!=. 

save `bam', replace

////////////////////////////////////////////////////////////////////////////////
//				Statins, measure in year t
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/ferido-dua51866/AD/data/aht/poly/statcce_2007p.dta", replace
keep bene_id stat_consdays_2007 
rename stat_consdays_2007 stat_pdays_2007
merge 1:1 bene_id using `bam'
drop if _m==1 //drop those only in statins file
drop _m
save `bam', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/ferido-dua51866/AD/data/aht/poly/statcce_2008p.dta", replace
keep bene_id stat_consdays_2008 
rename stat_consdays_2008 stat_pdays_2008
merge 1:1 bene_id using `bam'
drop if _m==1 //drop those only in statins file
drop _m
save `bam', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/ferido-dua51866/AD/data/aht/poly/statcce_2009p.dta", replace
keep bene_id stat_consdays_2009 
rename stat_consdays_2009 stat_pdays_2009
merge 1:1 bene_id using `bam'
drop if _m==1 //drop those only in statins file
drop _m

forvalues y=2007/2009{
	replace stat_pdays_`y' = 0 if stat_pdays_`y'==.
}

//stat in bam year
gen stat = .
replace stat = stat_pdays_2007 if bam_y2==2007
replace stat = stat_pdays_2008 if bam_y2==2008
replace stat = stat_pdays_2009 if bam_y2==2009
replace stat = 0 if stat==.

//stat 07-09
gen stat_pdays_0709 = 0
replace stat_pdays_0709 = stat_pdays_2007 + stat_pdays_2008 + stat_pdays_2009

sum stat_pdays_0709 if stat_pdays_0709!=., detail
gen stat4 = .
replace stat4 = 1 if stat_pdays_0709>=r(min) & stat_pdays_0709<r(p25)
replace stat4 = 2 if stat_pdays_0709>=r(p25) & stat_pdays_0709<r(p50)
replace stat4 = 3 if stat_pdays_0709>=r(p50) & stat_pdays_0709<r(p75)
replace stat4 = 4 if stat_pdays_0709>=r(p75) & stat_pdays_0709!=.
replace stat4 = 1 if stat_pdays_0709==0

save `bam', replace

////////////////////////////////////////////////////////////////////////////////
//				AHT, measure in year t
////////////////////////////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/aht0709.dta", replace
merge 1:1 bene_id using `bam'
drop if _m==1 //drop those only in the aht file
drop _m

forvalues y=2007/2009{
	replace aht_pdays_`y' = 0 if aht_pdays_`y'==.
	replace ras_pdays_`y' = 0 if ras_pdays_`y'==.
}

//aht in year of bam
gen aht = .
replace aht = aht_pdays_2007 if bam_y2==2007
replace aht = aht_pdays_2008 if bam_y2==2008
replace aht = aht_pdays_2009 if bam_y2==2009
replace aht = 0 if aht==.

//aht 07-09
gen aht_pdays_0709 = 0
replace aht_pdays_0709 = aht_pdays_2007 + aht_pdays_2008 + aht_pdays_2009

sum aht_pdays_0709 if aht_pdays_0709!=., detail
gen aht4 = .
replace aht4 = 1 if aht_pdays_0709>=r(min) & aht_pdays_0709<r(p25)
replace aht4 = 2 if aht_pdays_0709>=r(p25) & aht_pdays_0709<r(p50)
replace aht4 = 3 if aht_pdays_0709>=r(p50) & aht_pdays_0709<r(p75)
replace aht4 = 4 if aht_pdays_0709>=r(p75) & aht_pdays_0709!=.
replace aht4 = 1 if aht_pdays_0709==0

save `bam', replace

//intermediate data set
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_rxadrd16b.dta.dta", replace
*/
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_rxadrd16b.dta.dta", replace
tempfile bam
save `bam', replace

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
//////////////////						 						////////////////
//////////////////				Drug exposure variables			////////////////
//////////////////						 						////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

//user vars
// note: user vars in the bigger categories pick up people that fall into none of the sub-categories. 
local drugs "bam nsl nsl2 m3s darif fesot flavo oxybu solif tolte trosp"
local specdrugs "darif fesot flavo oxybu solif tolte trosp"
local days "90 180 270"
local ptile "25 50 75"
local years "2007 2008 2009"

foreach i in `drugs'{
	gen `i'_totdd_789 = `i'_totdd_2007+`i'_totdd_2008+`i'_totdd_2009
	gen `i'_tdd_789 = `i'_tdd_2007+`i'_tdd_2008+`i'_tdd_2009
}
foreach i in `drugs'{
	foreach j in `ptile'{
		sum `i'_totdd_789 if `i'_totdd_789>0, detail
		gen `i'_ddp`j'_789 = 0
		replace `i'_ddp`j'_789 = 1 if `i'_totdd_789>=(r(p`j'))
		sum `i'_tdd_789 if `i'_tdd_789>0, detail
		gen `i'_tddp`j'_789 = 0
		replace `i'_tddp`j'_789 = 1 if `i'_tdd_789>=(r(p`j'))
	}
}
foreach j in `ptile'{
	gen any_ddp`j'_789 = 0
	gen num_ddp`j'_789 = 0
	foreach i in `specdrugs'{
		replace any_ddp`j'_789 = 1 if `i'_ddp`j'_789==1
		replace num_ddp`j'_789 = num_ddp`j'_789 + 1 if `i'_ddp`j'_789==1
	}
}
	
//possesor vars
// note: user vars in the bigger categories pick up people that fall into none of the sub-categories. 
//789 possessor=1 if has 2 of the three years
foreach i in `drugs'{
	foreach d in `days'{
		gen `i'_pssr`d'_789 = 0
		replace `i'_pssr`d'_789 = 1 if `i'_pdays_2007>=`d' & `i'_pdays_2008>=`d'
		replace `i'_pssr`d'_789 = 1 if `i'_pdays_2008>=`d' & `i'_pdays_2009>=`d'
		replace `i'_pssr`d'_789 = 1 if `i'_pdays_2007>=`d' & `i'_pdays_2009>=`d'
	}
}
foreach d in `days'{
	gen any_pssr`d'_789 = 0
	gen num_pssr`d'_789 = 0
	foreach i in `specdrugs'{
		replace any_pssr`d'_789 = 1 if `i'_pssr`d'_789==1
		replace num_pssr`d'_789 = num_pssr`d'_789 + 1 if `i'_pssr`d'_789==1
	}
}

//outliers of days supply (fdays)
gen outlier = 0
foreach i in `specdrugs'{
	forvalues y=2007/2009{
		replace outlier = 1 if `i'_fdays_`y'>365 & `i'_fdays_`y'!=.
	}
}
//categorical variables of use - totdd
foreach i in `drugs'{
	gen `i'_catd1_1_89 = 0
	gen `i'_catd1_90_364 = 0
	gen `i'_catd1_365_1094 = 0
	gen `i'_catd1_gt1094 = 0
	replace `i'_catd1_1_89 = 1 if `i'_totdd_789>=1 & `i'_totdd_789<90
	replace `i'_catd1_90_364 = 1 if `i'_totdd_789>=90 & `i'_totdd_789<365
	replace `i'_catd1_365_1094 = 1 if `i'_totdd_789>=365 & `i'_totdd_789<1095
	replace `i'_catd1_gt1094 = 1 if `i'_totdd_789>=1095 & `i'_totdd_789!=.
	
	gen `i'_catd2_1_269 = 0
	gen `i'_catd2_270_719 = 0
	gen `i'_catd2_720_1399 = 0
	gen `i'_catd2_gt1399 = 0
	replace `i'_catd2_1_269 = 1 if `i'_totdd_789>=1 & `i'_totdd_789<270
	replace `i'_catd2_270_719 = 1 if `i'_totdd_789>=270 & `i'_totdd_789<720
	replace `i'_catd2_720_1399 = 1 if `i'_totdd_789>=720 & `i'_totdd_789<1440
	replace `i'_catd2_gt1399 = 1 if `i'_totdd_789>=1440 & `i'_totdd_789!=.
}

local levelsa "1_89 90_364 365_1094 gt1094"
local levelsb "1_269 270_729 730_1399 gt1399"
local levelsc "1_364 365_729 730_1094 gt1094"
local levelsd "1_239 240_629 630_1319 gt1319"
local levelse "1_269 270_729 730_1094 gt1094"
local levelsf "1_269 270_729 730_1259 gt1259"

//categorical variables of use - tdd
foreach i in `drugs'{
	gen `i'_ctdda_1_89 = 0
	gen `i'_ctdda_90_364 = 0
	gen `i'_ctdda_365_1094 = 0
	gen `i'_ctdda_gt1094 = 0
	replace `i'_ctdda_1_89 = 1 if `i'_tdd_789>=1 & `i'_tdd_789<90
	replace `i'_ctdda_90_364 = 1 if `i'_tdd_789>=90 & `i'_tdd_789<365
	replace `i'_ctdda_365_1094 = 1 if `i'_tdd_789>=365 & `i'_tdd_789<1095
	replace `i'_ctdda_gt1094 = 1 if `i'_tdd_789>=1095 & `i'_tdd_789!=.
	
	gen `i'_ctddb_1_269 = 0
	gen `i'_ctddb_270_729 = 0
	gen `i'_ctddb_730_1399 = 0
	gen `i'_ctddb_gt1399 = 0
	replace `i'_ctddb_1_269 = 1 if `i'_tdd_789>=1 & `i'_tdd_789<270
	replace `i'_ctddb_270_729 = 1 if `i'_tdd_789>=270 & `i'_tdd_789<730
	replace `i'_ctddb_730_1399 = 1 if `i'_tdd_789>=730 & `i'_tdd_789<1399
	replace `i'_ctddb_gt1399 = 1 if `i'_tdd_789>=1440 & `i'_tdd_789!=.

	gen `i'_ctddc_1_364 = 0
	gen `i'_ctddc_365_729 = 0
	gen `i'_ctddc_730_1094 = 0
	gen `i'_ctddc_gt1094 = 0
	replace `i'_ctddc_1_364 = 1 if `i'_tdd_789>=1 & `i'_tdd_789<365
	replace `i'_ctddc_365_729 = 1 if `i'_tdd_789>=365 & `i'_tdd_789<730
	replace `i'_ctddc_730_1094 = 1 if `i'_tdd_789>=730 & `i'_tdd_789<1095
	replace `i'_ctddc_gt1094 = 1 if `i'_tdd_789>=1095 & `i'_tdd_789!=.
	
	gen `i'_ctddd_1_239 = 0
	gen `i'_ctddd_240_629 = 0
	gen `i'_ctddd_630_1319 = 0
	gen `i'_ctddd_gt1319 = 0
	replace `i'_ctddd_1_239 = 1 if `i'_tdd_789>=1 & `i'_tdd_789<240
	replace `i'_ctddd_240_629 = 1 if `i'_tdd_789>=240 & `i'_tdd_789<630
	replace `i'_ctddd_630_1319 = 1 if `i'_tdd_789>=630 & `i'_tdd_789<1320
	replace `i'_ctddd_gt1319 = 1 if `i'_tdd_789>1319 & `i'_tdd_789!=.
	
	gen `i'_ctdde_1_269 = 0
	gen `i'_ctdde_270_729 = 0
	gen `i'_ctdde_730_1094 = 0
	gen `i'_ctdde_gt1094 = 0
	replace `i'_ctdde_1_269 = 1 if `i'_tdd_789>=1 & `i'_tdd_789<270
	replace `i'_ctdde_270_729 = 1 if `i'_tdd_789>=270 & `i'_tdd_789<730
	replace `i'_ctdde_730_1094 = 1 if `i'_tdd_789>=730 & `i'_tdd_789<1094
	replace `i'_ctdde_gt1094 = 1 if `i'_tdd_789>=1095 & `i'_tdd_789!=.

	gen `i'_ctddf_1_269 = 0
	gen `i'_ctddf_270_729 = 0
	gen `i'_ctddf_730_1259 = 0
	gen `i'_ctddf_gt1259 = 0
	replace `i'_ctddf_1_269 = 1 if `i'_tdd_789>=1 & `i'_tdd_789<270
	replace `i'_ctddf_270_729 = 1 if `i'_tdd_789>=270 & `i'_tdd_789<730
	replace `i'_ctddf_730_1259 = 1 if `i'_tdd_789>=730 & `i'_tdd_789<1260
	replace `i'_ctddf_gt1259 = 1 if `i'_tdd_789>=1260 & `i'_tdd_789!=.
}
local levelse "1_269 270_729 730_1094 gt1094"
local levelsf "1_269 270_729 730_1259 gt1259"

//number at each level
	foreach le in `levelsa'{
		gen num_ctdda_`le' = 0
		foreach i in `specdrugs'{
			replace num_ctdda_`le' = num_ctdda_`le' + 1 if `i'_ctdda_`le'==1
		}
	}
	foreach le in `levelsb'{
		gen num_ctddb_`le' = 0
		foreach i in `specdrugs'{
			replace num_ctddb_`le' = num_ctddb_`le' + 1 if `i'_ctddb_`le'==1
		}
	}
	foreach le in `levelsc'{
		gen num_ctddc_`le' = 0
		foreach i in `specdrugs'{
			replace num_ctddc_`le' = num_ctddc_`le' + 1 if `i'_ctddc_`le'==1
		}
	}
	foreach le in `levelsd'{
		gen num_ctddd_`le' = 0
		foreach i in `specdrugs'{
			replace num_ctddd_`le' = num_ctddd_`le' + 1 if `i'_ctddd_`le'==1
		}
	}
	foreach le in `levelse'{
		gen num_ctdde_`le' = 0
		foreach i in `specdrugs'{
			replace num_ctdde_`le' = num_ctdde_`le' + 1 if `i'_ctdde_`le'==1
		}
	}
	foreach le in `levelsf'{
		gen num_ctddf_`le' = 0
		foreach i in `specdrugs'{
			replace num_ctddf_`le' = num_ctddf_`le' + 1 if `i'_ctddf_`le'==1
		}
	}


////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
//////////////////						 						////////////////
//////////////////				Save							////////////////
//////////////////						 						////////////////
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
drop ffs2* ptd2* dual2* lis2* age_b* 
drop if county==""

codebook county

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_rxadrd16.dta", replace
count
sum

foreach i in `drugs'{
	sum `i'_tdd_789, detail
}
tab outlier
