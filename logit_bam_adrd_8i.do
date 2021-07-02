clear all
set more off
capture log close

/*
•	Input: bam_rxadrd16.dta 
•	Output: logit_bam_adrd_8a.xlsx
•	Across race, with year categories
•	Add insamp requirement for 2011
•	Regress verified incidence (2011-2016) on BAM exposure (2007-2009), (2010 washout period).
o	Compare NSL to M3S, according to tdd (unweighted total daily doses 789)
	4 regs, for each category comparison (2v2, 3v3, 4v4). 
*/
///////////////////////////////////////////////////////////////////////////////
//////////////////  	SAMPLE PREP 		///////////////////////////////////
///////////////////////////////////////////////////////////////////////////////

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/bam_rxadrd16.dta", replace

//washout period
drop if ADRDccw_yr<2011
drop if ADRDv_yr<2011
drop if year(lastday)<2011

count

tab year_insampmin
tab year_insampmax
tab dead
tab death_y

drop if year(lastday)<2016 & ADRDv==0

count
codebook bene_id
gen countvar=1

local drugs "bam nsl nsl2 m3s darif fesot flavo oxybu solif tolte trosp"
local aggdrugs "bam nsl nsl2 m3s"
local specdrugs "darif fesot flavo oxybu solif tolte trosp" 
local specdrugs2 "darif oxybu solif tolte trosp" 

putexcel set "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/bam/logit_bam_adrd_8i.xlsx", sheet(Sum) replace

///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
//////////////////  	LOGIT		 		///////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
preserve

global yvar "ADRDv"
global controls "female age agesq race_d* i.hcc4 i.phy4 dual_ever lis_ever i.stat4 i.aht4 cmd_*" 
global bldx "rxmdxy bldx_hb bldx_nb bldx_oth bldx_dis bldx_uis bldx_fup bldx_urg"
// oabui = oab+ui. oab = hb + nb + oth. ui = dis + uis + fup + urg

///////////////////////////////////////////////////////////////////////////////
//Compare drugs to other drugs 
///////////////////////////////////////////////////////////////////////////////
local drugs1 "nsl m3s"
local levelsa "1_89 90_364 365_1094 gt1094"
local levelsb "1_269 270_729 730_1399 gt1399"
local levelsc "1_364 365_729 730_1094 gt1094"
local levelsd "1_239 240_629 630_1319 gt1319"
local levelse "1_269 270_729 730_1094 gt1094"
local levelsf "1_269 270_729 730_1259 gt1259"

//nsl vs m3s
	//at each level
		foreach le in `levelsc'{
			restore 
			preserve
			keep if nsl_ctddc_`le'==1 | m3s_ctddc_`le'==1 
			logit $yvar nsl_ctddc_`le' $controls $bldx, or vce(cluster county)
			matrix logit1=r(table)'
			putexcel set "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/bam/logit_bam_adrd_8i.xlsx", sheet(all `le') modify
			putexcel A1="Compare drugs to other drugs", bold
			putexcel A2="Logit regressions of ADRD incidence (2011-2016) on exposure to BAM prescription drugs (2007-2009)", bold 
			putexcel B3="Compare nsl to those using m3s at the same level"
			putexcel B4="Sample: tdd `le' of nsl or m3s, no ADRD as of 2011, age 67+, Part D 07-09, FFS 07-09" B5="N =" 
			putexcel C5=(e(N)), nformat(number_sep)
			putexcel A6=matrix(logit1), names nformat(number_d2) left
			putexcel C6="OR"
			putexcel B7:B100, border(right)
			putexcel A5:Z5, border(bottom)
			putexcel A6:Z6, border(bottom)
			matrix drop logit1
			
			putexcel D5="nsl N =" F5="m3s N ="
			sum nsl_ctddc_`le' if nsl_ctddc_`le'==1
			putexcel E5=(r(N)), nformat(number_sep)
			sum m3s_ctddc_`le' if m3s_ctddc_`le'==1
			putexcel G5=(r(N)), nformat(number_sep)
		}		
		
	//female
		foreach le in `levelsc'{
			restore 
			preserve
			keep if nsl_ctddc_`le'==1 | m3s_ctddc_`le'==1 
			keep if female==1
			logit $yvar nsl_ctddc_`le' $controls $bldx, or vce(cluster county)
			matrix logit1=r(table)'
			putexcel set "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/bam/logit_bam_adrd_8i.xlsx", sheet(fem `le') modify
			putexcel A1="Compare drugs to other drugs", bold
			putexcel A2="Logit regressions of ADRD incidence (2011-2016) on exposure to BAM prescription drugs (2007-2009)", bold 
			putexcel B3="Compare nsl to those using m3s at the same level"
			putexcel B4="Sample: tdd `le' of nsl or m3s, no ADRD as of 2011, age 67+, Part D 07-09, FFS 07-09" B5="N =" 
			putexcel C5=(e(N)), nformat(number_sep)
			putexcel A6=matrix(logit1), names nformat(number_d2) left
			putexcel C6="OR"
			putexcel B7:B100, border(right)
			putexcel A5:Z5, border(bottom)
			putexcel A6:Z6, border(bottom)
			matrix drop logit1
			
			putexcel D5="nsl N =" F5="m3s N ="
			sum nsl_ctddc_`le' if nsl_ctddc_`le'==1
			putexcel E5=(r(N)), nformat(number_sep)
			sum m3s_ctddc_`le' if m3s_ctddc_`le'==1
			putexcel G5=(r(N)), nformat(number_sep)
		}
	//male
		foreach le in `levelsc'{
			restore 
			preserve
			keep if nsl_ctddc_`le'==1 | m3s_ctddc_`le'==1 
			keep if female==0
			logit $yvar nsl_ctddc_`le' $controls $bldx, or vce(cluster county)
			matrix logit1=r(table)'
			putexcel set "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/bam/logit_bam_adrd_8i.xlsx", sheet(mal `le') modify
			putexcel A1="Compare drugs to other drugs", bold
			putexcel A2="Logit regressions of ADRD incidence (2011-2016) on exposure to BAM prescription drugs (2007-2009)", bold 
			putexcel B3="Compare nsl to those using m3s at the same level"
			putexcel B4="Sample: tdd `le' of nsl or m3s, no ADRD as of 2011, age 67+, Part D 07-09, FFS 07-09" B5="N =" 
			putexcel C5=(e(N)), nformat(number_sep)
			putexcel A6=matrix(logit1), names nformat(number_d2) left
			putexcel C6="OR"
			putexcel B7:B100, border(right)
			putexcel A5:Z5, border(bottom)
			putexcel A6:Z6, border(bottom)
			matrix drop logit1
			
			putexcel D5="nsl N =" F5="m3s N ="
			sum nsl_ctddc_`le' if nsl_ctddc_`le'==1
			putexcel E5=(r(N)), nformat(number_sep)
			sum m3s_ctddc_`le' if m3s_ctddc_`le'==1
			putexcel G5=(r(N)), nformat(number_sep)
		}
	//white
		foreach le in `levelsc'{
			restore 
			preserve
			keep if nsl_ctddc_`le'==1 | m3s_ctddc_`le'==1 
			keep if race_dw==1
			logit $yvar nsl_ctddc_`le' $controls $bldx, or vce(cluster county)
			matrix logit1=r(table)'
			putexcel set "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/bam/logit_bam_adrd_8i.xlsx", sheet(whi `le') modify
			putexcel A1="Compare drugs to other drugs", bold
			putexcel A2="Logit regressions of ADRD incidence (2011-2016) on exposure to BAM prescription drugs (2007-2009)", bold 
			putexcel B3="Compare nsl to those using m3s at the same level"
			putexcel B4="Sample: tdd `le' of nsl or m3s, no ADRD as of 2011, age 67+, Part D 07-09, FFS 07-09" B5="N =" 
			putexcel C5=(e(N)), nformat(number_sep)
			putexcel A6=matrix(logit1), names nformat(number_d2) left
			putexcel C6="OR"
			putexcel B7:B100, border(right)
			putexcel A5:Z5, border(bottom)
			putexcel A6:Z6, border(bottom)
			matrix drop logit1
			
			putexcel D5="nsl N =" F5="m3s N ="
			sum nsl_ctddc_`le' if nsl_ctddc_`le'==1
			putexcel E5=(r(N)), nformat(number_sep)
			sum m3s_ctddc_`le' if m3s_ctddc_`le'==1
			putexcel G5=(r(N)), nformat(number_sep)
		}
	//black
		foreach le in `levelsc'{
			restore 
			preserve
			keep if nsl_ctddc_`le'==1 | m3s_ctddc_`le'==1 
			keep if race_db==1
			logit $yvar nsl_ctddc_`le' $controls $bldx, or vce(cluster county)
			matrix logit1=r(table)'
			putexcel set "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/bam/logit_bam_adrd_8i.xlsx", sheet(bla `le') modify
			putexcel A1="Compare drugs to other drugs", bold
			putexcel A2="Logit regressions of ADRD incidence (2011-2016) on exposure to BAM prescription drugs (2007-2009)", bold 
			putexcel B3="Compare nsl to those using m3s at the same level"
			putexcel B4="Sample: tdd `le' of nsl or m3s, no ADRD as of 2011, age 67+, Part D 07-09, FFS 07-09" B5="N =" 
			putexcel C5=(e(N)), nformat(number_sep)
			putexcel A6=matrix(logit1), names nformat(number_d2) left
			putexcel C6="OR"
			putexcel B7:B100, border(right)
			putexcel A5:Z5, border(bottom)
			putexcel A6:Z6, border(bottom)
			matrix drop logit1
			
			putexcel D5="nsl N =" F5="m3s N ="
			sum nsl_ctddc_`le' if nsl_ctddc_`le'==1
			putexcel E5=(r(N)), nformat(number_sep)
			sum m3s_ctddc_`le' if m3s_ctddc_`le'==1
			putexcel G5=(r(N)), nformat(number_sep)
		}
	//hisp
		foreach le in `levelsc'{
			restore 
			preserve
			keep if nsl_ctddc_`le'==1 | m3s_ctddc_`le'==1 
			keep if race_dh==1
			logit $yvar nsl_ctddc_`le' $controls $bldx, or vce(cluster county)
			matrix logit1=r(table)'
			putexcel set "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/bam/logit_bam_adrd_8i.xlsx", sheet(his `le') modify
			putexcel A1="Compare drugs to other drugs", bold
			putexcel A2="Logit regressions of ADRD incidence (2011-2016) on exposure to BAM prescription drugs (2007-2009)", bold 
			putexcel B3="Compare nsl to those using m3s at the same level"
			putexcel B4="Sample: tdd `le' of nsl or m3s, no ADRD as of 2011, age 67+, Part D 07-09, FFS 07-09" B5="N =" 
			putexcel C5=(e(N)), nformat(number_sep)
			putexcel A6=matrix(logit1), names nformat(number_d2) left
			putexcel C6="OR"
			putexcel B7:B100, border(right)
			putexcel A5:Z5, border(bottom)
			putexcel A6:Z6, border(bottom)
			matrix drop logit1
			
			putexcel D5="nsl N =" F5="m3s N ="
			sum nsl_ctddc_`le' if nsl_ctddc_`le'==1
			putexcel E5=(r(N)), nformat(number_sep)
			sum m3s_ctddc_`le' if m3s_ctddc_`le'==1
			putexcel G5=(r(N)), nformat(number_sep)
		}
	//oth
		foreach le in `levelsc'{
			restore 
			preserve
			keep if nsl_ctddc_`le'==1 | m3s_ctddc_`le'==1 
			keep if race_do==1
			logit $yvar nsl_ctddc_`le' $controls $bldx, or vce(cluster county)
			matrix logit1=r(table)'
			putexcel set "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/bam/logit_bam_adrd_8i.xlsx", sheet(oth `le') modify
			putexcel A1="Compare drugs to other drugs", bold
			putexcel A2="Logit regressions of ADRD incidence (2011-2016) on exposure to BAM prescription drugs (2007-2009)", bold 
			putexcel B3="Compare nsl to those using m3s at the same level"
			putexcel B4="Sample: tdd `le' of nsl or m3s, no ADRD as of 2011, age 67+, Part D 07-09, FFS 07-09" B5="N =" 
			putexcel C5=(e(N)), nformat(number_sep)
			putexcel A6=matrix(logit1), names nformat(number_d2) left
			putexcel C6="OR"
			putexcel B7:B100, border(right)
			putexcel A5:Z5, border(bottom)
			putexcel A6:Z6, border(bottom)
			matrix drop logit1
			
			putexcel D5="nsl N =" F5="m3s N ="
			sum nsl_ctddc_`le' if nsl_ctddc_`le'==1
			putexcel E5=(r(N)), nformat(number_sep)
			sum m3s_ctddc_`le' if m3s_ctddc_`le'==1
			putexcel G5=(r(N)), nformat(number_sep)
		}

capture log close

