clear all
set more off

/*
*/

////////////////////////////////////////////////////////////////////////////////
///////////////		IMPORT CCW DIAGNOSES 07-14		////////////////////////////
////////////////////////////////////////////////////////////////////////////////

use "/disk/agedisk2/medicare/data/20pct/bsf/2007/bsfcc2007.dta", clear
keep bene_id ami* atrial* diab* strk* hypert* alzh* hyperl* 
tempfile ccw
save `ccw', replace

use "/disk/agedisk2/medicare/data/20pct/bsf/2008/bsfcc2008.dta", clear
keep bene_id ami* atrial* diab* strk* hypert* alzh* hyperl*
merge 1:1 bene_id using `ccw'
drop _m
save `ccw', replace

use "/disk/agedisk2/medicare/data/20pct/bsf/2009/bsfcc2009.dta", clear
keep bene_id ami* atrial* diab* strk* hypert* alzh* hyperl*
merge 1:1 bene_id using `ccw'
drop _m
save `ccw', replace

use "/disk/agedisk2/medicare/data/20pct/bsf/2010/bsfcc2010.dta", clear
keep bene_id ami* atrial* diab* strk* hypert* alzh* hyperl*
merge 1:1 bene_id using `ccw'
drop _m
save `ccw', replace

use "/disk/agedisk2/medicare/data/20pct/bsf/2011/bsfcc2011.dta", clear
keep bene_id ami* atrial* diab* strk* hypert* alzh* hyperl*
merge 1:1 bene_id using `ccw'
drop _m
save `ccw', replace

use "/disk/agedisk1/medicare/data/u/c/20pct/bsf/2012/bsfcc2012.dta", clear
keep bene_id ami* atrial* diab* strk* hypert* alzh* hyperl*
merge 1:1 bene_id using `ccw'
drop _m
save `ccw', replace

use "/disk/agedisk3/medicare/data/20pct/bsf/2013/bsfcc2013.dta", clear
keep bene_id ami* atrial* diab* strk* hypert* alzh* hyperl* 
merge 1:1 bene_id using `ccw'
drop _m
save `ccw', replace

use "/disk/agedisk1/medicare/data/u/c/20pct/bsf/2014/bsfcc2014.dta", clear
keep bene_id ami* atrial* diab* strk* hypert* alzh* hyperl*
merge 1:1 bene_id using `ccw'
drop _m

use "/disk/agedisk1/medicare/data/u/c/20pct/bsf/2015/bsfcc2015.dta", clear
keep bene_id ami* atrial* diab* strk* hypert* alzh* hyperl*
merge 1:1 bene_id using `ccw'
drop _m

use "/disk/agedisk1/medicare/data/u/c/20pct/bsf/2016/bsfcc2016.dta", clear
keep bene_id ami* atrial* diab* strk* hypert* alzh* hyperl*
merge 1:1 bene_id using `ccw'
drop _m

///////  SAVE
sort bene_id 
compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/ccw_0716.dta", replace
