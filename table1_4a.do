clear all
set more off
capture log close

/*
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
codebook bene_id

local levelsa "1_89 90_364 365_1094 gt1094"
local levelsb "1_269 270_729 730_1399 gt1399"
local levelsc "1_364 365_729 730_1094 gt1094"
local levelsd "1_239 240_629 630_1319 gt1319"
local levelse "1_269 270_729 730_1094 gt1094"
local levelsf "1_269 270_729 730_1259 gt1259"
		foreach le in `levelsc'{
			tab bam_ctddc_`le'
		}
		foreach le in `levelsc'{
			sum bam_ctddc_`le' if bam_ctddc_`le'==1
			sum bam_tdd_789 if bam_ctddc_`le'==1
		}
	
///////////////////////////////////////////////////////////////////////////////
//////////////////  	BAM chars		 		///////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
putexcel set "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/bam/table1_4a.xlsx", sheet(Sum) replace
putexcel A1="TABLE 1: Characteristics of bladder antimuscarinic use", bold 
putexcel A2="Sample: 2+ claims in 1+ years of 07 08 09, no ADRD as of 2011, age 67+, Part D 07-09, FFS 07-09", bold
putexcel B3="All BAMs" C3="Non-selective" D3="Fesoterodine" E3="Flavoxate" F3="Oxybutynin" G3="Tolterodine" H3="Trospium" I3="M3-selective" J3="Darifenacin" K3="Solifenacin", bold

putexcel A4="BAM total daily doses (TDD) 2007-2009", bold 
//label rows
local r = 5
	putexcel A`r'="N with >0 TDD"
	local r = `r'+1
	putexcel A`r'="mean"
	local r = `r'+1
	putexcel A`r'="SD"
	local r = `r'+1
	putexcel A`r'="25th %"
	local r = `r'+1
	putexcel A`r'="50th %"
	local r = `r'+1
	putexcel A`r'="75th %"
	local r = `r'+1
	putexcel A`r'="95th %"
	local r = `r'+1
	local r = `r'+1
	putexcel A`r'="N with 1-364 TDD"
	local r = `r'+1
	putexcel A`r'="N with 364-729 TDD"
	local r = `r'+1
	putexcel A`r'="N with 730-1094 TDD"
	local r = `r'+1
	putexcel A`r'="N with >1094 TDD"

//BAM  
local r = 5
	count 
	putexcel B`r'=(r(N)), nformat(number_sep)
	local r = `r'+1
	sum bam_tdd_789 if bam_tdd_789>1, detail
		putexcel B`r'=(r(mean)), nformat(number_sep)
		local r = `r'+1
		putexcel B`r'=(r(sd)), nformat(number_sep)
		local r = `r'+1
		putexcel B`r'=(r(p25)), nformat(number_sep)
		local r = `r'+1
		putexcel B`r'=(r(p50)), nformat(number_sep)
		local r = `r'+1
		putexcel B`r'=(r(p75)), nformat(number_sep)
		local r = `r'+1
		putexcel B`r'=(r(p95)), nformat(number_sep)
		local r = `r'+1
		local r = `r'+1
		foreach le in `levelsc'{
			count if bam_ctddc_`le'==1
			putexcel B`r'=(r(N)), nformat(number_sep)
			local r = `r'+1
		}
//nsl  
local r = 5
	count if nsl_ctddc_1_364==1 | nsl_ctddc_365_729==1 | nsl_ctddc_730_1094==1 | nsl_ctddc_gt1094==1 
	putexcel C`r'=(r(N)), nformat(number_sep)
	local r = `r'+1
	sum nsl_tdd_789 if nsl_tdd_789>1, detail
		putexcel C`r'=(r(mean)), nformat(number_sep)
		local r = `r'+1
		putexcel C`r'=(r(sd)), nformat(number_sep)
		local r = `r'+1
		putexcel C`r'=(r(p25)), nformat(number_sep)
		local r = `r'+1
		putexcel C`r'=(r(p50)), nformat(number_sep)
		local r = `r'+1
		putexcel C`r'=(r(p75)), nformat(number_sep)
		local r = `r'+1
		putexcel C`r'=(r(p95)), nformat(number_sep)
		local r = `r'+1
		local r = `r'+1
		foreach le in `levelsc'{
			count if nsl_ctddc_`le'==1
			putexcel C`r'=(r(N)), nformat(number_sep)
			local r = `r'+1
		}
//fesot  
local r = 5
	count if fesot_ctddc_1_364==1 | fesot_ctddc_365_729==1 | fesot_ctddc_730_1094==1 | fesot_ctddc_gt1094==1 
	putexcel D`r'=(r(N)), nformat(number_sep)
	local r = `r'+1
	sum fesot_tdd_789 if fesot_tdd_789>1, detail
		putexcel D`r'=(r(mean)), nformat(number_sep)
		local r = `r'+1
		putexcel D`r'=(r(sd)), nformat(number_sep)
		local r = `r'+1
		putexcel D`r'=(r(p25)), nformat(number_sep)
		local r = `r'+1
		putexcel D`r'=(r(p50)), nformat(number_sep)
		local r = `r'+1
		putexcel D`r'=(r(p75)), nformat(number_sep)
		local r = `r'+1
		putexcel D`r'=(r(p95)), nformat(number_sep)
		local r = `r'+1
		local r = `r'+1
		foreach le in `levelsc'{
			count if fesot_ctddc_`le'==1
			putexcel D`r'=(r(N)), nformat(number_sep)
			local r = `r'+1
		}
//flavo  
local r = 5
	count if flavo_ctddc_1_364==1 | flavo_ctddc_365_729==1 | flavo_ctddc_730_1094==1 | flavo_ctddc_gt1094==1 
	putexcel E`r'=(r(N)), nformat(number_sep)
	local r = `r'+1
	sum flavo_tdd_789 if flavo_tdd_789>1, detail
		putexcel E`r'=(r(mean)), nformat(number_sep)
		local r = `r'+1
		putexcel E`r'=(r(sd)), nformat(number_sep)
		local r = `r'+1
		putexcel E`r'=(r(p25)), nformat(number_sep)
		local r = `r'+1
		putexcel E`r'=(r(p50)), nformat(number_sep)
		local r = `r'+1
		putexcel E`r'=(r(p75)), nformat(number_sep)
		local r = `r'+1
		putexcel E`r'=(r(p95)), nformat(number_sep)
		local r = `r'+1
		local r = `r'+1
		foreach le in `levelsc'{
			count if flavo_ctddc_`le'==1
			putexcel E`r'=(r(N)), nformat(number_sep)
			local r = `r'+1
		}
//oxybu  
local r = 5
	count if oxybu_ctddc_1_364==1 | oxybu_ctddc_365_729==1 | oxybu_ctddc_730_1094==1 | oxybu_ctddc_gt1094==1 
	putexcel F`r'=(r(N)), nformat(number_sep)
	local r = `r'+1
	sum oxybu_tdd_789 if oxybu_tdd_789>1, detail
		putexcel F`r'=(r(mean)), nformat(number_sep)
		local r = `r'+1
		putexcel F`r'=(r(sd)), nformat(number_sep)
		local r = `r'+1
		putexcel F`r'=(r(p25)), nformat(number_sep)
		local r = `r'+1
		putexcel F`r'=(r(p50)), nformat(number_sep)
		local r = `r'+1
		putexcel F`r'=(r(p75)), nformat(number_sep)
		local r = `r'+1
		putexcel F`r'=(r(p95)), nformat(number_sep)
		local r = `r'+1
		local r = `r'+1
		foreach le in `levelsc'{
			count if oxybu_ctddc_`le'==1
			putexcel F`r'=(r(N)), nformat(number_sep)
			local r = `r'+1
		}
//tolte  
local r = 5
	count if tolte_ctddc_1_364==1 | tolte_ctddc_365_729==1 | tolte_ctddc_730_1094==1 | tolte_ctddc_gt1094==1 
	putexcel G`r'=(r(N)), nformat(number_sep)
	local r = `r'+1
	sum tolte_tdd_789 if tolte_tdd_789>1, detail
		putexcel G`r'=(r(mean)), nformat(number_sep)
		local r = `r'+1
		putexcel G`r'=(r(sd)), nformat(number_sep)
		local r = `r'+1
		putexcel G`r'=(r(p25)), nformat(number_sep)
		local r = `r'+1
		putexcel G`r'=(r(p50)), nformat(number_sep)
		local r = `r'+1
		putexcel G`r'=(r(p75)), nformat(number_sep)
		local r = `r'+1
		putexcel G`r'=(r(p95)), nformat(number_sep)
		local r = `r'+1
		local r = `r'+1
		foreach le in `levelsc'{
			count if tolte_ctddc_`le'==1
			putexcel G`r'=(r(N)), nformat(number_sep)
			local r = `r'+1
		}
//trosp  
local r = 5
	count if trosp_ctddc_1_364==1 | trosp_ctddc_365_729==1 | trosp_ctddc_730_1094==1 | trosp_ctddc_gt1094==1 
	putexcel H`r'=(r(N)), nformat(number_sep)
	local r = `r'+1
	sum trosp_tdd_789 if trosp_tdd_789>1, detail
		putexcel H`r'=(r(mean)), nformat(number_sep)
		local r = `r'+1
		putexcel H`r'=(r(sd)), nformat(number_sep)
		local r = `r'+1
		putexcel H`r'=(r(p25)), nformat(number_sep)
		local r = `r'+1
		putexcel H`r'=(r(p50)), nformat(number_sep)
		local r = `r'+1
		putexcel H`r'=(r(p75)), nformat(number_sep)
		local r = `r'+1
		putexcel H`r'=(r(p95)), nformat(number_sep)
		local r = `r'+1
		local r = `r'+1
		foreach le in `levelsc'{
			count if trosp_ctddc_`le'==1
			putexcel H`r'=(r(N)), nformat(number_sep)
			local r = `r'+1
		}
//m3s  
local r = 5
	count if m3s_ctddc_1_364==1 | m3s_ctddc_365_729==1 | m3s_ctddc_730_1094==1 | m3s_ctddc_gt1094==1 
	putexcel I`r'=(r(N)), nformat(number_sep)
	local r = `r'+1
	sum m3s_tdd_789 if m3s_tdd_789>1, detail
		putexcel I`r'=(r(mean)), nformat(number_sep)
		local r = `r'+1
		putexcel I`r'=(r(sd)), nformat(number_sep)
		local r = `r'+1
		putexcel I`r'=(r(p25)), nformat(number_sep)
		local r = `r'+1
		putexcel I`r'=(r(p50)), nformat(number_sep)
		local r = `r'+1
		putexcel I`r'=(r(p75)), nformat(number_sep)
		local r = `r'+1
		putexcel I`r'=(r(p95)), nformat(number_sep)
		local r = `r'+1
		local r = `r'+1
		foreach le in `levelsc'{
			count if m3s_ctddc_`le'==1
			putexcel I`r'=(r(N)), nformat(number_sep)
			local r = `r'+1
		}
//darif  
local r = 5
	count if darif_ctddc_1_364==1 | darif_ctddc_365_729==1 | darif_ctddc_730_1094==1 | darif_ctddc_gt1094==1 
	putexcel J`r'=(r(N)), nformat(number_sep)
	local r = `r'+1
	sum darif_tdd_789 if darif_tdd_789>1, detail
		putexcel J`r'=(r(mean)), nformat(number_sep)
		local r = `r'+1
		putexcel J`r'=(r(sd)), nformat(number_sep)
		local r = `r'+1
		putexcel J`r'=(r(p25)), nformat(number_sep)
		local r = `r'+1
		putexcel J`r'=(r(p50)), nformat(number_sep)
		local r = `r'+1
		putexcel J`r'=(r(p75)), nformat(number_sep)
		local r = `r'+1
		putexcel J`r'=(r(p95)), nformat(number_sep)
		local r = `r'+1
		local r = `r'+1
		foreach le in `levelsc'{
			count if darif_ctddc_`le'==1
			putexcel J`r'=(r(N)), nformat(number_sep)
			local r = `r'+1
		}
//solif  
local r = 5
	count if solif_ctddc_1_364==1 | solif_ctddc_365_729==1 | solif_ctddc_730_1094==1 | solif_ctddc_gt1094==1 
	putexcel K`r'=(r(N)), nformat(number_sep)
	local r = `r'+1
	sum solif_tdd_789 if solif_tdd_789>1, detail
		putexcel K`r'=(r(mean)), nformat(number_sep)
		local r = `r'+1
		putexcel K`r'=(r(sd)), nformat(number_sep)
		local r = `r'+1
		putexcel K`r'=(r(p25)), nformat(number_sep)
		local r = `r'+1
		putexcel K`r'=(r(p50)), nformat(number_sep)
		local r = `r'+1
		putexcel K`r'=(r(p75)), nformat(number_sep)
		local r = `r'+1
		putexcel K`r'=(r(p95)), nformat(number_sep)
		local r = `r'+1
		local r = `r'+1
		foreach le in `levelsc'{
			count if solif_ctddc_`le'==1
			putexcel K`r'=(r(N)), nformat(number_sep)
			local r = `r'+1
		}

			local r = `r'+1

///////////////////////////////////////////////////////////////////////////////
//////////////////  	BAM chars		 		///////////////////////////////////
///////////////////////////////////////////////////////////////////////////////
local drugs "bam nsl nsl2 m3s darif fesot flavo oxybu solif tolte trosp"
local catcont "ADRDv female race_dw race_db race_dh race_do dual_ever lis_ever cmd_hypt_bbam cmd_hypl_bbam cmd_ami_bbam cmd_atf_bbam cmd_dia_bbam cmd_str_bbam bldx_hb bldx_nb bldx_oth bldx_dis bldx_uis bldx_fup bldx_urg"
local concont "age hcc phy stat aht rxmdxy"

foreach i in `drugs'{
	gen `i'_user=1 if `i'_tdd_789>=1
}

//categorical vars
putexcel A`r'="Characteristics of individuals using BAMs", bold 
local r =`r'+1
putexcel B`r'="All BAMs" C`r'="Non-selective" D`r'="Fesoterodine" E`r'="Flavoxate" F`r'="Oxybutynin" G`r'="Tolterodine" H`r'="Trospium" I`r'="M3-selective" J`r'="Darifenacin" K`r'="Solifenacin", bold
local r =`r'+1
foreach i in `catcont'{
	putexcel A`r'="`i'"
	sum `i' if bam_user==1
	putexcel B`r'=(r(mean)), nformat(percent)
	sum `i' if nsl_user==1
	putexcel C`r'=(r(mean)), nformat(percent)
	sum `i' if fesot_user==1
	putexcel D`r'=(r(mean)), nformat(percent)
	sum `i' if flavo_user==1
	putexcel E`r'=(r(mean)), nformat(percent)
	sum `i' if oxybu_user==1
	putexcel F`r'=(r(mean)), nformat(percent)
	sum `i' if tolte_user==1
	putexcel G`r'=(r(mean)), nformat(percent)
	sum `i' if trosp_user==1
	putexcel H`r'=(r(mean)), nformat(percent)
	sum `i' if m3s_user==1
	putexcel I`r'=(r(mean)), nformat(percent)
	sum `i' if darif_user==1
	putexcel J`r'=(r(mean)), nformat(percent)
	sum `i' if solif_user==1
	putexcel K`r'=(r(mean)), nformat(percent)
local r = `r'+1	
}
local r = `r'+1	
//Continuous vars
putexcel A`r'="Characteristics of individuals using BAMs (mean, SD)", bold 
local r =`r'+1
putexcel B`r'="All BAMs" D`r'="Non-selective" F`r'="Fesoterodine" H`r'="Flavoxate" J`r'="oxybutynin" L`r'="Tolterodine" N`r'="Trospium" P`r'="M3-selective" R`r'="Darifenacin" T`r'="Solifenacin", bold
putexcel B`r':C`r' D`r':E`r' F`r':G`r' H`r':I`r' J`r':K`r' L`r':M`r' N`r':O`r' P`r':Q`r' R`r':S`r' T`r':U`r', merge 
local r =`r'+1
foreach i in `concont'{
	putexcel A`r'="`i'"
	sum `i' if bam_user==1
	putexcel B`r'=(r(mean)), nformat(number_sep)
	putexcel C`r'=(r(sd)), nformat(number_sep_d2)
	sum `i' if nsl_user==1
	putexcel D`r'=(r(mean)), nformat(number_sep)
	putexcel E`r'=(r(sd)), nformat(number_sep_d2)
	sum `i' if fesot_user==1
	putexcel F`r'=(r(mean)), nformat(number_sep)
	putexcel G`r'=(r(sd)), nformat(number_sep_d2)
	sum `i' if flavo_user==1
	putexcel H`r'=(r(mean)), nformat(number_sep)
	putexcel I`r'=(r(sd)), nformat(number_sep_d2)
	sum `i' if oxybu_user==1
	putexcel J`r'=(r(mean)), nformat(number_sep)
	putexcel K`r'=(r(sd)), nformat(number_sep_d2)
	sum `i' if tolte_user==1
	putexcel L`r'=(r(mean)), nformat(number_sep)
	putexcel M`r'=(r(sd)), nformat(number_sep_d2)
	sum `i' if trosp_user==1
	putexcel N`r'=(r(mean)), nformat(number_sep)
	putexcel O`r'=(r(sd)), nformat(number_sep_d2)
	sum `i' if m3s_user==1
	putexcel P`r'=(r(mean)), nformat(number_sep)
	putexcel Q`r'=(r(sd)), nformat(number_sep_d2)
	sum `i' if darif_user==1
	putexcel R`r'=(r(mean)), nformat(number_sep)
	putexcel S`r'=(r(sd)), nformat(number_sep_d2)
	sum `i' if solif_user==1
	putexcel T`r'=(r(mean)), nformat(number_sep)
	putexcel U`r'=(r(sd)), nformat(number_sep_d2)
local r = `r'+1	
}

capture log close

