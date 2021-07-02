/*
•	Input: ahtco_YYYY.dta (07-09)
•	Output: aht0709.dta
•	Just merge the aht files from the AHT project from 07-09.
*/

//2007
use "/disk/agedisk3/medicare.work/goldman-DUA51866/ferido-dua51866/AD/data/aht/poly/ahtco_2007.dta", replace
gen acei_clms = 0
gen acei_pdays = 0
replace acei_clms = aceiso_clms + aceicchb_clms + aceithia_clms
replace acei_pdays = aceiso_pdays + aceicchb_pdays + aceithia_pdays
replace acei_pdays = 365 if acei_pdays>365

gen a2rb_clms = 0
gen a2rb_pdays = 0
replace a2rb_clms = a2rbso_clms + a2rbthia_clms 
replace a2rb_pdays = a2rbso_pdays + a2rbthia_pdays 
replace a2rb_pdays = 365 if a2rb_pdays>365

gen bblo_clms = 0
gen bblo_pdays = 0
replace bblo_clms = bbloso_clms + bblothia_clms
replace bblo_pdays = bbloso_pdays + bblothia_pdays
replace bblo_pdays = 365 if bblo_pdays>365

gen cchb_clms = 0
gen cchb_pdays = 0
replace cchb_clms = cchbso_clms + aceicchb_clms
replace cchb_pdays = cchbso_pdays + aceicchb_pdays
replace cchb_pdays = 365 if cchb_pdays>365

gen loop_clms = 0
gen loop_pdays = 0
replace loop_clms = loopso_clms 
replace loop_pdays = loopso_pdays 
replace loop_pdays = 365 if loop_pdays>365

gen thia_clms = 0
gen thia_pdays = 0
replace thia_clms = thiaso_clms + aceithia_clms + a2rbthia_clms + bblothia_clms
replace thia_pdays = thiaso_pdays + aceithia_pdays + a2rbthia_pdays + bblothia_pdays
replace thia_pdays = 365 if thia_pdays>365

gen aht_clms = 0
gen aht_pdays = 0
replace aht_clms = acei_clms + a2rb_clms + bblo_clms + cchb_clms + loop_clms + thia_clms
replace aht_pdays = acei_pdays + a2rb_pdays + bblo_pdays + cchb_pdays + loop_pdays + thia_pdays
replace aht_pdays = 365 if aht_pdays>365

gen ras_clms = 0
gen ras_pdays = 0
replace ras_clms = acei_clms + a2rb_clms
replace ras_pdays = acei_pdays + a2rb_pdays
replace ras_pdays = 365 if ras_pdays>365

gen rasso_clms = 0
gen rasso_pdays = 0
replace rasso_clms = aceiso_clms + a2rbso_clms
replace rasso_pdays = aceiso_pdays + a2rbso_pdays
replace rasso_pdays = 365 if rasso_pdays>365

keep bene_id aht_pdays ras_pdays
rename aht_pdays aht_pdays_2007
rename ras_pdays ras_pdays_2007

tempfile 2007
save `2007', replace

//2008
use "/disk/agedisk3/medicare.work/goldman-DUA51866/ferido-dua51866/AD/data/aht/poly/ahtco_2008.dta", replace
gen acei_clms = 0
gen acei_pdays = 0
replace acei_clms = aceiso_clms + aceicchb_clms + aceithia_clms
replace acei_pdays = aceiso_pdays + aceicchb_pdays + aceithia_pdays
replace acei_pdays = 365 if acei_pdays>365

gen a2rb_clms = 0
gen a2rb_pdays = 0
replace a2rb_clms = a2rbso_clms + a2rbthia_clms 
replace a2rb_pdays = a2rbso_pdays + a2rbthia_pdays 
replace a2rb_pdays = 365 if a2rb_pdays>365

gen bblo_clms = 0
gen bblo_pdays = 0
replace bblo_clms = bbloso_clms + bblothia_clms
replace bblo_pdays = bbloso_pdays + bblothia_pdays
replace bblo_pdays = 365 if bblo_pdays>365

gen cchb_clms = 0
gen cchb_pdays = 0
replace cchb_clms = cchbso_clms + aceicchb_clms
replace cchb_pdays = cchbso_pdays + aceicchb_pdays
replace cchb_pdays = 365 if cchb_pdays>365

gen loop_clms = 0
gen loop_pdays = 0
replace loop_clms = loopso_clms 
replace loop_pdays = loopso_pdays 
replace loop_pdays = 365 if loop_pdays>365

gen thia_clms = 0
gen thia_pdays = 0
replace thia_clms = thiaso_clms + aceithia_clms + a2rbthia_clms + bblothia_clms
replace thia_pdays = thiaso_pdays + aceithia_pdays + a2rbthia_pdays + bblothia_pdays
replace thia_pdays = 365 if thia_pdays>365

gen aht_clms = 0
gen aht_pdays = 0
replace aht_clms = acei_clms + a2rb_clms + bblo_clms + cchb_clms + loop_clms + thia_clms
replace aht_pdays = acei_pdays + a2rb_pdays + bblo_pdays + cchb_pdays + loop_pdays + thia_pdays
replace aht_pdays = 365 if aht_pdays>365

gen ras_clms = 0
gen ras_pdays = 0
replace ras_clms = acei_clms + a2rb_clms
replace ras_pdays = acei_pdays + a2rb_pdays
replace ras_pdays = 365 if ras_pdays>365

gen rasso_clms = 0
gen rasso_pdays = 0
replace rasso_clms = aceiso_clms + a2rbso_clms
replace rasso_pdays = aceiso_pdays + a2rbso_pdays
replace rasso_pdays = 365 if rasso_pdays>365

keep bene_id aht_pdays ras_pdays
rename aht_pdays aht_pdays_2008
rename ras_pdays ras_pdays_2008

tempfile 2008
save `2008', replace

//2009
use "/disk/agedisk3/medicare.work/goldman-DUA51866/ferido-dua51866/AD/data/aht/poly/ahtco_2009.dta", replace
gen acei_clms = 0
gen acei_pdays = 0
replace acei_clms = aceiso_clms + aceicchb_clms + aceithia_clms
replace acei_pdays = aceiso_pdays + aceicchb_pdays + aceithia_pdays
replace acei_pdays = 365 if acei_pdays>365

gen a2rb_clms = 0
gen a2rb_pdays = 0
replace a2rb_clms = a2rbso_clms + a2rbthia_clms 
replace a2rb_pdays = a2rbso_pdays + a2rbthia_pdays 
replace a2rb_pdays = 365 if a2rb_pdays>365

gen bblo_clms = 0
gen bblo_pdays = 0
replace bblo_clms = bbloso_clms + bblothia_clms
replace bblo_pdays = bbloso_pdays + bblothia_pdays
replace bblo_pdays = 365 if bblo_pdays>365

gen cchb_clms = 0
gen cchb_pdays = 0
replace cchb_clms = cchbso_clms + aceicchb_clms
replace cchb_pdays = cchbso_pdays + aceicchb_pdays
replace cchb_pdays = 365 if cchb_pdays>365

gen loop_clms = 0
gen loop_pdays = 0
replace loop_clms = loopso_clms 
replace loop_pdays = loopso_pdays 
replace loop_pdays = 365 if loop_pdays>365

gen thia_clms = 0
gen thia_pdays = 0
replace thia_clms = thiaso_clms + aceithia_clms + a2rbthia_clms + bblothia_clms
replace thia_pdays = thiaso_pdays + aceithia_pdays + a2rbthia_pdays + bblothia_pdays
replace thia_pdays = 365 if thia_pdays>365

gen aht_clms = 0
gen aht_pdays = 0
replace aht_clms = acei_clms + a2rb_clms + bblo_clms + cchb_clms + loop_clms + thia_clms
replace aht_pdays = acei_pdays + a2rb_pdays + bblo_pdays + cchb_pdays + loop_pdays + thia_pdays
replace aht_pdays = 365 if aht_pdays>365

gen ras_clms = 0
gen ras_pdays = 0
replace ras_clms = acei_clms + a2rb_clms
replace ras_pdays = acei_pdays + a2rb_pdays
replace ras_pdays = 365 if ras_pdays>365

gen rasso_clms = 0
gen rasso_pdays = 0
replace rasso_clms = aceiso_clms + a2rbso_clms
replace rasso_pdays = aceiso_pdays + a2rbso_pdays
replace rasso_pdays = 365 if rasso_pdays>365

keep bene_id aht_pdays ras_pdays
rename aht_pdays aht_pdays_2009
rename ras_pdays ras_pdays_2009

//merge
merge 1:1 bene_id using `2008'
drop _m
tempfile aht
save `aht', replace

merge 1:1 bene_id using `2007'
drop _m

forvalues i=2007/2009{
	replace aht_pdays_`i' = 0 if aht_pdays_`i'==.	
	replace ras_pdays_`i' = 0 if ras_pdays_`i'==.
}

compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/aht0709.dta", replace
