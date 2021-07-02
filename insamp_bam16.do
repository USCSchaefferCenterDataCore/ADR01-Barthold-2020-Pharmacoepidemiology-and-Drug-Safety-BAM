clear all
set more off
capture log close

/*
•	Input: bene_status_yearYYYY.dta (03-13), bene_demog2013.dta
•	Output: insamp_YYYY.dta (06-13), 0713
•	Gets enrollment status variables for each year, keeps indivs in each year who are insamp:
o	FFS in t, t-1, t-2; age>=67, no drop flag
•	Makes vars for sex, race, death date, and years of observation.
*/

////////////////////////////////////////////////////////////////////////////////
//////////////////		Sample				////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
/*
////////  	2003 		////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2003.dta", replace
keep bene_id enrFFS_allyr age_beg sex death_date birth_date

rename death_date death_d
rename birth_date bene_dob

gen ffs2003 = 0
replace ffs2003 = 1 if enrFFS_allyr=="Y"

drop enrFFS_allyr age_beg
tempfile status
save `status', replace

////////  	2004 		////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2004.dta", replace
keep bene_id enrFFS_allyr age_beg sex death_date birth_date
rename death_date death_d
rename birth_date bene_dob

gen ffs2004 = 0
replace ffs2004 = 1 if enrFFS_allyr=="Y"

drop enrFFS_allyr age_beg

merge 1:1 bene_id using `status'
drop _m
save `status', replace

////////  	2005 		////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2005.dta", replace
keep bene_id enrFFS_allyr age_beg sex death_date birth_date
rename death_date death_d
rename birth_date bene_dob

gen ffs2005 = 0
replace ffs2005 = 1 if enrFFS_allyr=="Y"

drop enrFFS_allyr age_beg

merge 1:1 bene_id using `status'
drop _m
save `status', replace

////////  	2006 		////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2006.dta", replace
keep bene_id enrFFS_allyr ptD_allyr anyptD age_beg race_bg sex death_date birth_date dropflag anydual anylis
rename death_date death_d
rename birth_date bene_dob

gen ptd2006 = 0
replace ptd2006 = 1 if ptD_allyr=="Y"
gen ffs2006 = 0
replace ffs2006 = 1 if enrFFS_allyr=="Y"

gen dual2006 = 0
replace dual2006 = 1 if anydual=="Y"
gen lis2006 = 0
replace lis2006 = 1 if anylis=="Y"

gen age_beg_2006 = age_beg
drop enrFFS_allyr ptD_allyr anyptD age_beg anydual anylis

merge 1:1 bene_id using `status'
drop _m
save `status', replace

gen insamp_2006 = 0
replace insamp_2006 = 1 if (ffs2006==1 & ffs2005==1 & ffs2004==1) & age_beg_2006>=67 & dropflag=="N"
keep if insamp_2006==1
drop dropflag

sort bene_id
compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2006.dta", replace

////////  	2007 		////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2007.dta", replace
keep bene_id enrFFS_allyr ptD_allyr anyptD age_beg race_bg sex death_date birth_date dropflag anydual anylis
rename death_date death_d
rename birth_date bene_dob

gen ptd2007 = 0
replace ptd2007 = 1 if ptD_allyr=="Y"
gen ffs2007 = 0
replace ffs2007 = 1 if enrFFS_allyr=="Y"

gen dual2007 = 0
replace dual2007 = 1 if anydual=="Y"
gen lis2007 = 0
replace lis2007 = 1 if anylis=="Y"

gen age_beg_2007 = age_beg
drop enrFFS_allyr ptD_allyr anyptD age_beg anydual anylis

merge 1:1 bene_id using `status'
drop _m
save `status', replace

gen insamp_2007 = 0
replace insamp_2007 = 1 if (ffs2007==1 & ffs2006==1 & ffs2005==1) & age_beg_2007>=67 & dropflag=="N"
keep if insamp_2007==1
drop dropflag

sort bene_id
compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2007.dta", replace

////////  	2008 		////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2008.dta", replace
keep bene_id enrFFS_allyr ptD_allyr anyptD age_beg race_bg sex death_date birth_date dropflag anydual anylis
rename death_date death_d
rename birth_date bene_dob

gen ptd2008 = 0
replace ptd2008 = 1 if ptD_allyr=="Y"
gen ffs2008 = 0
replace ffs2008 = 1 if enrFFS_allyr=="Y"

gen dual2008 = 0
replace dual2008 = 1 if anydual=="Y"
gen lis2008 = 0
replace lis2008 = 1 if anylis=="Y"

gen age_beg_2008 = age_beg
drop enrFFS_allyr ptD_allyr anyptD age_beg anydual anylis

merge 1:1 bene_id using `status'
drop _m
save `status', replace

gen insamp_2008 = 0
replace insamp_2008 = 1 if (ffs2008==1 & ffs2007==1 & ffs2006==1) & age_beg_2008>=67 & dropflag=="N"
keep if insamp_2008==1
drop dropflag

sort bene_id
compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2008.dta", replace

////////  	2009 		////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2009.dta", replace
keep bene_id enrFFS_allyr ptD_allyr anyptD age_beg race_bg sex death_date birth_date dropflag anydual anylis
rename death_date death_d
rename birth_date bene_dob

gen ptd2009 = 0
replace ptd2009 = 1 if ptD_allyr=="Y"
gen ffs2009 = 0
replace ffs2009 = 1 if enrFFS_allyr=="Y"

gen dual2009 = 0
replace dual2009 = 1 if anydual=="Y"
gen lis2009 = 0
replace lis2009 = 1 if anylis=="Y"

gen age_beg_2009 = age_beg
drop enrFFS_allyr ptD_allyr anyptD age_beg anydual anylis

merge 1:1 bene_id using `status'
drop _m
save `status', replace

gen insamp_2009 = 0
replace insamp_2009 = 1 if (ffs2009==1 & ffs2008==1 & ffs2007==1) & age_beg_2009>=67 & dropflag=="N"
keep if insamp_2009==1
drop dropflag

sort bene_id
compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2009.dta", replace

////////  	2010 		////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2010.dta", replace
keep bene_id enrFFS_allyr ptD_allyr anyptD age_beg race_bg sex death_date birth_date dropflag anydual anylis
rename death_date death_d
rename birth_date bene_dob

gen ptd2010 = 0
replace ptd2010 = 1 if ptD_allyr=="Y"
gen ffs2010 = 0
replace ffs2010 = 1 if enrFFS_allyr=="Y"

gen dual2010 = 0
replace dual2010 = 1 if anydual=="Y"
gen lis2010 = 0
replace lis2010 = 1 if anylis=="Y"

gen age_beg_2010 = age_beg
drop enrFFS_allyr ptD_allyr anyptD age_beg anydual anylis

merge 1:1 bene_id using `status'
drop _m
save `status', replace

gen insamp_2010 = 0
replace insamp_2010 = 1 if (ffs2010==1 & ffs2009==1 & ffs2008==1) & age_beg_2010>=67 & dropflag=="N"
keep if insamp_2010==1
drop dropflag

sort bene_id
compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2010.dta", replace

////////  	2011 		////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2011.dta", replace
keep bene_id enrFFS_allyr ptD_allyr anyptD age_beg race_bg sex death_date birth_date dropflag anydual anylis
rename death_date death_d
rename birth_date bene_dob

gen ptd2011 = 0
replace ptd2011 = 1 if ptD_allyr=="Y"
gen ffs2011 = 0
replace ffs2011 = 1 if enrFFS_allyr=="Y"

gen dual2011 = 0
replace dual2011 = 1 if anydual=="Y"
gen lis2011 = 0
replace lis2011 = 1 if anylis=="Y"

gen age_beg_2011 = age_beg
drop enrFFS_allyr ptD_allyr anyptD age_beg anydual anylis

merge 1:1 bene_id using `status'
drop _m
save `status', replace

gen insamp_2011 = 0
replace insamp_2011 = 1 if (ffs2011==1 & ffs2010==1 & ffs2009==1) & age_beg_2011>=67 & dropflag=="N"
keep if insamp_2011==1
drop dropflag

sort bene_id
compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2011.dta", replace

////////  	2012 		////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2012.dta", replace
keep bene_id enrFFS_allyr ptD_allyr anyptD age_beg race_bg sex death_date birth_date dropflag anydual anylis
rename death_date death_d
rename birth_date bene_dob

gen ptd2012 = 0
replace ptd2012 = 1 if ptD_allyr=="Y"
gen ffs2012 = 0
replace ffs2012 = 1 if enrFFS_allyr=="Y"

gen dual2012 = 0
replace dual2012 = 1 if anydual=="Y"
gen lis2012 = 0
replace lis2012 = 1 if anylis=="Y"

gen age_beg_2012 = age_beg
drop enrFFS_allyr ptD_allyr anyptD age_beg anydual anylis

merge 1:1 bene_id using `status'
drop _m
save `status', replace

gen insamp_2012 = 0
replace insamp_2012 = 1 if (ffs2012==1 & ffs2011==1 & ffs2010==1) & age_beg_2012>=67 & dropflag=="N"
keep if insamp_2012==1
drop dropflag

sort bene_id
compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2012.dta", replace

////////  	2013 		////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2013.dta", replace
keep bene_id enrFFS_allyr ptD_allyr anyptD age_beg race_bg sex death_date birth_date dropflag anydual anylis
rename death_date death_d
rename birth_date bene_dob

gen ptd2013 = 0
replace ptd2013 = 1 if ptD_allyr=="Y"
gen ffs2013 = 0
replace ffs2013 = 1 if enrFFS_allyr=="Y"

gen dual2013 = 0
replace dual2013 = 1 if anydual=="Y"
gen lis2013 = 0
replace lis2013 = 1 if anylis=="Y"

gen age_beg_2013 = age_beg
drop enrFFS_allyr ptD_allyr anyptD age_beg anydual anylis

merge 1:1 bene_id using `status'
drop _m
save `status', replace

gen insamp_2013 = 0
replace insamp_2013 = 1 if (ffs2013==1 & ffs2012==1 & ffs2011==1) & age_beg_2013>=67 & dropflag=="N"
keep if insamp_2013==1
drop dropflag

sort bene_id
compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2013.dta", replace

////////  	2014 		////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2014.dta", replace
keep bene_id enrFFS_allyr ptD_allyr anyptD age_beg race_bg sex death_date birth_date dropflag anydual anylis
rename death_date death_d
rename birth_date bene_dob

gen ptd2014 = 0
replace ptd2014 = 1 if ptD_allyr=="Y"
gen ffs2014 = 0
replace ffs2014 = 1 if enrFFS_allyr=="Y"

gen dual2014 = 0
replace dual2014 = 1 if anydual=="Y"
gen lis2014 = 0
replace lis2014 = 1 if anylis=="Y"

gen age_beg_2014 = age_beg
drop enrFFS_allyr ptD_allyr anyptD age_beg anydual anylis

merge 1:1 bene_id using `status'
drop _m
save `status', replace

gen insamp_2014 = 0
replace insamp_2014 = 1 if (ffs2014==1 & ffs2013==1 & ffs2012==1) & age_beg_2014>=67 & dropflag=="N"
keep if insamp_2014==1
drop dropflag

sort bene_id
compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2014.dta", replace

////////  	2015 		////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2015.dta", replace
keep bene_id enrFFS_allyr ptD_allyr anyptD age_beg race_bg sex death_date birth_date dropflag anydual anylis
rename death_date death_d
rename birth_date bene_dob

gen ptd2015 = 0
replace ptd2015 = 1 if ptD_allyr=="Y"
gen ffs2015 = 0
replace ffs2015 = 1 if enrFFS_allyr=="Y"

gen dual2015 = 0
replace dual2015 = 1 if anydual=="Y"
gen lis2015 = 0
replace lis2015 = 1 if anylis=="Y"

gen age_beg_2015 = age_beg
drop enrFFS_allyr ptD_allyr anyptD age_beg anydual anylis

merge 1:1 bene_id using `status'
drop _m
save `status', replace

gen insamp_2015 = 0
replace insamp_2015 = 1 if (ffs2015==1 & ffs2014==1 & ffs2013==1) & age_beg_2015>=67 & dropflag=="N"
keep if insamp_2015==1
drop dropflag

sort bene_id
compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2015.dta", replace

////////  	2016 		////////////////////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/51866/DATA/Clean_Data/BeneStatus/bene_status_year2016.dta", replace
keep bene_id enrFFS_allyr ptD_allyr anyptD age_beg race_bg sex death_date birth_date dropflag anydual anylis
rename death_date death_d
rename birth_date bene_dob

gen ptd2016 = 0
replace ptd2016 = 1 if ptD_allyr=="Y"
gen ffs2016 = 0
replace ffs2016 = 1 if enrFFS_allyr=="Y"

gen dual2016 = 0
replace dual2016 = 1 if anydual=="Y"
gen lis2016 = 0
replace lis2016 = 1 if anylis=="Y"

gen age_beg_2016 = age_beg
drop enrFFS_allyr ptD_allyr anyptD age_beg anydual anylis

merge 1:1 bene_id using `status'
drop _m
save `status', replace

gen insamp_2016 = 0
replace insamp_2016 = 1 if (ffs2016==1 & ffs2015==1 & ffs2014==1) & age_beg_2016>=67 & dropflag=="N"
keep if insamp_2016==1
drop dropflag

sort bene_id
compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2016.dta", replace
*/
////////////////////////////////////////////////////////////////////////////////
//////////////////		merge wide 07-16	 ///////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2007.dta", replace
gen y2005 = 1
gen y2006 = 1
gen y2007 = 1
tempfile samp
save `samp', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2008.dta", replace
gen y2006 = 1
gen y2007 = 1
gen y2008 = 1
merge 1:1 bene_id using `samp'
drop _m
save `samp', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2009.dta", replace
gen y2007 = 1
gen y2008 = 1
gen y2009 = 1
merge 1:1 bene_id using `samp'
drop _m
save `samp', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2010.dta", replace
gen y2008 = 1
gen y2009 = 1
gen y2010 = 1
merge 1:1 bene_id using `samp'
drop _m
save `samp', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2011.dta", replace
gen y2009 = 1
gen y2010 = 1
gen y2011 = 1
sort bene_id
merge 1:1 bene_id using `samp'
drop _m
save `samp', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2012.dta", replace
gen y2010 = 1
gen y2011 = 1
gen y2012 = 1
merge 1:1 bene_id using `samp'
drop _m
save `samp', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2013.dta", replace
gen y2011 = 1
gen y2012 = 1
gen y2013 = 1
merge 1:1 bene_id using `samp'
drop _m
save `samp', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2014.dta", replace
gen y2012 = 1
gen y2013 = 1
gen y2014 = 1
merge 1:1 bene_id using `samp'
drop _m
save `samp', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2015.dta", replace
gen y2013 = 1
gen y2014 = 1
gen y2015 = 1
merge 1:1 bene_id using `samp'
drop _m
save `samp', replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_2016.dta", replace
gen y2014 = 1
gen y2015 = 1
gen y2016 = 1
merge 1:1 bene_id using `samp'
drop _m

////////////////////////////////////////////////////////////////////////////////
//////////////////		make vars			////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

egen idn = group(bene_id)

//death timing
gen death_y = year(death_d)
gen death_ageD = death_d - bene_dob
gen death_age = death_ageD/365 
gen dead = 0
replace dead = 1 if death_d!=.

//years insamp
gen year_insampmin = .
gen year_insampmax = .
forvalues i=2016(-1)2007{
	replace year_insampmin = `i' if insamp_`i'==1
}
forvalues i=2007(1)2016{
	replace year_insampmax = `i' if insamp_`i'==1
}

//years/days observed
gen firstyoo = .
gen firstdoo = .
forvalues i=2016(-1)2007{
	replace firstyoo = `i' if y`i'==1
}
replace firstdoo = 17167 if firstyoo==2007
replace firstdoo = 17532 if firstyoo==2008
replace firstdoo = 17898 if firstyoo==2009
replace firstdoo = 18263 if firstyoo==2010
replace firstdoo = 18628 if firstyoo==2011
replace firstdoo = 18993 if firstyoo==2012
replace firstdoo = 19359 if firstyoo==2013
replace firstdoo = 19724 if firstyoo==2014
replace firstdoo = 20089 if firstyoo==2015
replace firstdoo = 20454 if firstyoo==2016

gen lastyoo = .
gen lastdoo = .
forvalues i=2007(1)2016{
	replace lastyoo = `i' if y`i'==1
}
replace lastdoo = 17531 if lastyoo==2007
replace lastdoo = 17897 if lastyoo==2008
replace lastdoo = 18262 if lastyoo==2009
replace lastdoo = 18627 if lastyoo==2010
replace lastdoo = 18992 if lastyoo==2011
replace lastdoo = 19358 if lastyoo==2012
replace lastdoo = 19723 if lastyoo==2013
replace lastdoo = 20088 if lastyoo==2014
replace lastdoo = 20453 if firstyoo==2015
replace lastdoo = 20819 if firstyoo==2016

replace lastdoo = death_d if death_d!=. & death_d<lastdoo
replace lastyoo = year(death_d) if death_d!=. & death_d<lastdoo

//race - 0 unknown, 1 white, 2 black, 3 other, 4 asian/pacific islander, 5 hispanic, 6 american indian/alaskan native
gen race_dw = 0
replace race_dw = 1 if race_bg=="1"
gen race_db = 0
replace race_db = 1 if race_bg=="2"
gen race_dh = 0
replace race_dh = 1 if race_bg=="5"
gen race_do = 0
replace race_do = 1 if race_bg=="0" | race_bg=="3" | race_bg=="4" | race_bg=="6" | race_bg==""

//sex - 1 male, 2 female
gen female = 0
replace female = 1 if sex=="2"

//dual, lis
gen dual_ever = 0
gen dual_yrs = 0
gen lis_ever = 0
gen lis_yrs = 0

forvalues i=2007(1)2016{
	replace dual_ever = 1 if dual`i'==1
	replace dual_yrs = dual_yrs+1 if dual`i'==1
	replace lis_ever = 1 if lis`i'==1
	replace lis_yrs = lis_yrs+1 if lis`i'==1
}
tab dual_ever
tab lis_ever
sum dual_yrs lis_yrs, detail

drop idn
compress
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bamsamp_0716.dta", replace
