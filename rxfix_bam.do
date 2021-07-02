clear all
set more off
capture log close

/*
•	Input: XXXXX_YYYY.dta, 06-14, for oxybu tolte flavo darif trosp solif fesot  
•	Output: XXXXX_YYYY.dta, 06-14, for oxybu tolte flavo darif trosp solif fesot  
•	Back out the total mgs consumed in that year, and calculate the total daily doses (unweighted: tdd), then resave the file with its original name.
*/

//darif fesot flavo oxybu solif tolte trosp  
/*
Minimum daily doses:
trospenacin - 7.5 mg
Fesoterodine - 4 mg
Flavoxate - 300 mg
Oxybutynin - patch 3.9 mg, oral 5 mg
Solifenacin - 5 mg
Tolterodine - 2 mg
Trospium - 20 mg
*/
   

////////////////////////	2006 		////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2006.dta", replace
keep bene_id darif_pdays_2006 darif_clms_2006 darif_minfilldt_2006 darif_totdd_2006 darif_dmg_2006 darif_fdays_2006
gen darif_itotmg_2006 = darif_dmg_2006*darif_fdays_2006
gen darif_tdd_2006 = darif_itotmg_2006/7.5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2006.dta", replace

//no 2006 fesot

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2006.dta", replace
keep bene_id flavo_pdays_2006 flavo_clms_2006 flavo_minfilldt_2006 flavo_totdd_2006 flavo_dmg_2006 flavo_fdays_2006
gen flavo_itotmg_2006 = flavo_dmg_2006*flavo_fdays_2006
gen flavo_tdd_2006 = flavo_itotmg_2006/300
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2006.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2006.dta", replace
keep bene_id oxybu_pdays_2006 oxybu_clms_2006 oxybu_minfilldt_2006 oxybu_totdd_2006 oxybu_dmg_2006 oxybu_fdays_2006
gen oxybu_itotmg_2006 = oxybu_dmg_2006*oxybu_fdays_2006
gen oxybu_tdd_2006 = oxybu_itotmg_2006/5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2006.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2006.dta", replace
keep bene_id solif_pdays_2006 solif_clms_2006 solif_minfilldt_2006 solif_totdd_2006 solif_dmg_2006 solif_fdays_2006
gen solif_itotmg_2006 = solif_dmg_2006*solif_fdays_2006
gen solif_tdd_2006 = solif_itotmg_2006/5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2006.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2006.dta", replace
keep bene_id tolte_pdays_2006 tolte_clms_2006 tolte_minfilldt_2006 tolte_totdd_2006 tolte_dmg_2006 tolte_fdays_2006
gen tolte_itotmg_2006 = tolte_dmg_2006*tolte_fdays_2006
gen tolte_tdd_2006 = tolte_itotmg_2006/2
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2006.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2006.dta", replace
keep bene_id trosp_pdays_2006 trosp_clms_2006 trosp_minfilldt_2006 trosp_totdd_2006 trosp_dmg_2006 trosp_fdays_2006
gen trosp_itotmg_2006 = trosp_dmg_2006*trosp_fdays_2006
gen trosp_tdd_2006 = trosp_itotmg_2006/20
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2006.dta", replace


////////////////////////	2007 		////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2007.dta", replace
keep bene_id darif_pdays_2007 darif_clms_2007 darif_minfilldt_2007 darif_totdd_2007 darif_dmg_2007 darif_fdays_2007
gen darif_itotmg_2007 = darif_dmg_2007*darif_fdays_2007
gen darif_tdd_2007 = darif_itotmg_2007/7.5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2007.dta", replace

//no 2007 fesot

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2007.dta", replace
keep bene_id flavo_pdays_2007 flavo_clms_2007 flavo_minfilldt_2007 flavo_totdd_2007 flavo_dmg_2007 flavo_fdays_2007
gen flavo_itotmg_2007 = flavo_dmg_2007*flavo_fdays_2007
gen flavo_tdd_2007 = flavo_itotmg_2007/300
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2007.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2007.dta", replace
keep bene_id oxybu_pdays_2007 oxybu_clms_2007 oxybu_minfilldt_2007 oxybu_totdd_2007 oxybu_dmg_2007 oxybu_fdays_2007
gen oxybu_itotmg_2007 = oxybu_dmg_2007*oxybu_fdays_2007
gen oxybu_tdd_2007 = oxybu_itotmg_2007/5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2007.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2007.dta", replace
keep bene_id solif_pdays_2007 solif_clms_2007 solif_minfilldt_2007 solif_totdd_2007 solif_dmg_2007 solif_fdays_2007
gen solif_itotmg_2007 = solif_dmg_2007*solif_fdays_2007
gen solif_tdd_2007 = solif_itotmg_2007/5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2007.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2007.dta", replace
keep bene_id tolte_pdays_2007 tolte_clms_2007 tolte_minfilldt_2007 tolte_totdd_2007 tolte_dmg_2007 tolte_fdays_2007
gen tolte_itotmg_2007 = tolte_dmg_2007*tolte_fdays_2007
gen tolte_tdd_2007 = tolte_itotmg_2007/2
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2007.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2007.dta", replace
keep bene_id trosp_pdays_2007 trosp_clms_2007 trosp_minfilldt_2007 trosp_totdd_2007 trosp_dmg_2007 trosp_fdays_2007
gen trosp_itotmg_2007 = trosp_dmg_2007*trosp_fdays_2007
gen trosp_tdd_2007 = trosp_itotmg_2007/20
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2007.dta", replace

////////////////////////	2008 		////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2008.dta", replace
keep bene_id darif_pdays_2008 darif_clms_2008 darif_minfilldt_2008 darif_totdd_2008 darif_dmg_2008 darif_fdays_2008
gen darif_itotmg_2008 = darif_dmg_2008*darif_fdays_2008
gen darif_tdd_2008 = darif_itotmg_2008/7.5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2008.dta", replace

//no 2008 fesot

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2008.dta", replace
keep bene_id flavo_pdays_2008 flavo_clms_2008 flavo_minfilldt_2008 flavo_totdd_2008 flavo_dmg_2008 flavo_fdays_2008
gen flavo_itotmg_2008 = flavo_dmg_2008*flavo_fdays_2008
gen flavo_tdd_2008 = flavo_itotmg_2008/300
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2008.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2008.dta", replace
keep bene_id oxybu_pdays_2008 oxybu_clms_2008 oxybu_minfilldt_2008 oxybu_totdd_2008 oxybu_dmg_2008 oxybu_fdays_2008
gen oxybu_itotmg_2008 = oxybu_dmg_2008*oxybu_fdays_2008
gen oxybu_tdd_2008 = oxybu_itotmg_2008/5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2008.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2008.dta", replace
keep bene_id solif_pdays_2008 solif_clms_2008 solif_minfilldt_2008 solif_totdd_2008 solif_dmg_2008 solif_fdays_2008
gen solif_itotmg_2008 = solif_dmg_2008*solif_fdays_2008
gen solif_tdd_2008 = solif_itotmg_2008/5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2008.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2008.dta", replace
keep bene_id tolte_pdays_2008 tolte_clms_2008 tolte_minfilldt_2008 tolte_totdd_2008 tolte_dmg_2008 tolte_fdays_2008
gen tolte_itotmg_2008 = tolte_dmg_2008*tolte_fdays_2008
gen tolte_tdd_2008 = tolte_itotmg_2008/2
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2008.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2008.dta", replace
keep bene_id trosp_pdays_2008 trosp_clms_2008 trosp_minfilldt_2008 trosp_totdd_2008 trosp_dmg_2008 trosp_fdays_2008
gen trosp_itotmg_2008 = trosp_dmg_2008*trosp_fdays_2008
gen trosp_tdd_2008 = trosp_itotmg_2008/20
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2008.dta", replace

////////////////////////	2009 		////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2009.dta", replace
keep bene_id darif_pdays_2009 darif_clms_2009 darif_minfilldt_2009 darif_totdd_2009 darif_dmg_2009 darif_fdays_2009
gen darif_itotmg_2009 = darif_dmg_2009*darif_fdays_2009
gen darif_tdd_2009 = darif_itotmg_2009/7.5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2009.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/fesot_2009.dta", replace
keep bene_id fesot_pdays_2009 fesot_clms_2009 fesot_minfilldt_2009 fesot_totdd_2009 fesot_dmg_2009 fesot_fdays_2009
gen fesot_itotmg_2009 = fesot_dmg_2009*fesot_fdays_2009
gen fesot_tdd_2009 = fesot_itotmg_2009/4
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/fesot_2009.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2009.dta", replace
keep bene_id flavo_pdays_2009 flavo_clms_2009 flavo_minfilldt_2009 flavo_totdd_2009 flavo_dmg_2009 flavo_fdays_2009
gen flavo_itotmg_2009 = flavo_dmg_2009*flavo_fdays_2009
gen flavo_tdd_2009 = flavo_itotmg_2009/300
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2009.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2009.dta", replace
keep bene_id oxybu_pdays_2009 oxybu_clms_2009 oxybu_minfilldt_2009 oxybu_totdd_2009 oxybu_dmg_2009 oxybu_fdays_2009
gen oxybu_itotmg_2009 = oxybu_dmg_2009*oxybu_fdays_2009
gen oxybu_tdd_2009 = oxybu_itotmg_2009/5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2009.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2009.dta", replace
keep bene_id solif_pdays_2009 solif_clms_2009 solif_minfilldt_2009 solif_totdd_2009 solif_dmg_2009 solif_fdays_2009
gen solif_itotmg_2009 = solif_dmg_2009*solif_fdays_2009
gen solif_tdd_2009 = solif_itotmg_2009/5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2009.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2009.dta", replace
keep bene_id tolte_pdays_2009 tolte_clms_2009 tolte_minfilldt_2009 tolte_totdd_2009 tolte_dmg_2009 tolte_fdays_2009
gen tolte_itotmg_2009 = tolte_dmg_2009*tolte_fdays_2009
gen tolte_tdd_2009 = tolte_itotmg_2009/2
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2009.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2009.dta", replace
keep bene_id trosp_pdays_2009 trosp_clms_2009 trosp_minfilldt_2009 trosp_totdd_2009 trosp_dmg_2009 trosp_fdays_2009
gen trosp_itotmg_2009 = trosp_dmg_2009*trosp_fdays_2009
gen trosp_tdd_2009 = trosp_itotmg_2009/20
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2009.dta", replace

////////////////////////	2010 		////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2010.dta", replace
keep bene_id darif_pdays_2010 darif_clms_2010 darif_minfilldt_2010 darif_totdd_2010 darif_dmg_2010 darif_fdays_2010
gen darif_itotmg_2010 = darif_dmg_2010*darif_fdays_2010
gen darif_tdd_2010 = darif_itotmg_2010/7.5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2010.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/fesot_2010.dta", replace
keep bene_id fesot_pdays_2010 fesot_clms_2010 fesot_minfilldt_2010 fesot_totdd_2010 fesot_dmg_2010 fesot_fdays_2010
gen fesot_itotmg_2010 = fesot_dmg_2010*fesot_fdays_2010
gen fesot_tdd_2010 = fesot_itotmg_2010/4
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/fesot_2010.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2010.dta", replace
keep bene_id flavo_pdays_2010 flavo_clms_2010 flavo_minfilldt_2010 flavo_totdd_2010 flavo_dmg_2010 flavo_fdays_2010
gen flavo_itotmg_2010 = flavo_dmg_2010*flavo_fdays_2010
gen flavo_tdd_2010 = flavo_itotmg_2010/300
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2010.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2010.dta", replace
keep bene_id oxybu_pdays_2010 oxybu_clms_2010 oxybu_minfilldt_2010 oxybu_totdd_2010 oxybu_dmg_2010 oxybu_fdays_2010
gen oxybu_itotmg_2010 = oxybu_dmg_2010*oxybu_fdays_2010
gen oxybu_tdd_2010 = oxybu_itotmg_2010/5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2010.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2010.dta", replace
keep bene_id solif_pdays_2010 solif_clms_2010 solif_minfilldt_2010 solif_totdd_2010 solif_dmg_2010 solif_fdays_2010
gen solif_itotmg_2010 = solif_dmg_2010*solif_fdays_2010
gen solif_tdd_2010 = solif_itotmg_2010/5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2010.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2010.dta", replace
keep bene_id tolte_pdays_2010 tolte_clms_2010 tolte_minfilldt_2010 tolte_totdd_2010 tolte_dmg_2010 tolte_fdays_2010
gen tolte_itotmg_2010 = tolte_dmg_2010*tolte_fdays_2010
gen tolte_tdd_2010 = tolte_itotmg_2010/2
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2010.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2010.dta", replace
keep bene_id trosp_pdays_2010 trosp_clms_2010 trosp_minfilldt_2010 trosp_totdd_2010 trosp_dmg_2010 trosp_fdays_2010
gen trosp_itotmg_2010 = trosp_dmg_2010*trosp_fdays_2010
gen trosp_tdd_2010 = trosp_itotmg_2010/20
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2010.dta", replace

////////////////////////	2011 		////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2011.dta", replace
keep bene_id darif_pdays_2011 darif_clms_2011 darif_minfilldt_2011 darif_totdd_2011 darif_dmg_2011 darif_fdays_2011
gen darif_itotmg_2011 = darif_dmg_2011*darif_fdays_2011
gen darif_tdd_2011 = darif_itotmg_2011/7.5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2011.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/fesot_2011.dta", replace
keep bene_id fesot_pdays_2011 fesot_clms_2011 fesot_minfilldt_2011 fesot_totdd_2011 fesot_dmg_2011 fesot_fdays_2011
gen fesot_itotmg_2011 = fesot_dmg_2011*fesot_fdays_2011
gen fesot_tdd_2011 = fesot_itotmg_2011/4
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/fesot_2011.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2011.dta", replace
keep bene_id flavo_pdays_2011 flavo_clms_2011 flavo_minfilldt_2011 flavo_totdd_2011 flavo_dmg_2011 flavo_fdays_2011
gen flavo_itotmg_2011 = flavo_dmg_2011*flavo_fdays_2011
gen flavo_tdd_2011 = flavo_itotmg_2011/300
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2011.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2011.dta", replace
keep bene_id oxybu_pdays_2011 oxybu_clms_2011 oxybu_minfilldt_2011 oxybu_totdd_2011 oxybu_dmg_2011 oxybu_fdays_2011
gen oxybu_itotmg_2011 = oxybu_dmg_2011*oxybu_fdays_2011
gen oxybu_tdd_2011 = oxybu_itotmg_2011/5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2011.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2011.dta", replace
keep bene_id solif_pdays_2011 solif_clms_2011 solif_minfilldt_2011 solif_totdd_2011 solif_dmg_2011 solif_fdays_2011
gen solif_itotmg_2011 = solif_dmg_2011*solif_fdays_2011
gen solif_tdd_2011 = solif_itotmg_2011/5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2011.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2011.dta", replace
keep bene_id tolte_pdays_2011 tolte_clms_2011 tolte_minfilldt_2011 tolte_totdd_2011 tolte_dmg_2011 tolte_fdays_2011
gen tolte_itotmg_2011 = tolte_dmg_2011*tolte_fdays_2011
gen tolte_tdd_2011 = tolte_itotmg_2011/2
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2011.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2011.dta", replace
keep bene_id trosp_pdays_2011 trosp_clms_2011 trosp_minfilldt_2011 trosp_totdd_2011 trosp_dmg_2011 trosp_fdays_2011
gen trosp_itotmg_2011 = trosp_dmg_2011*trosp_fdays_2011
gen trosp_tdd_2011 = trosp_itotmg_2011/20
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2011.dta", replace

////////////////////////	2012 		////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2012.dta", replace
keep bene_id darif_pdays_2012 darif_clms_2012 darif_minfilldt_2012 darif_totdd_2012 darif_dmg_2012 darif_fdays_2012
gen darif_itotmg_2012 = darif_dmg_2012*darif_fdays_2012
gen darif_tdd_2012 = darif_itotmg_2012/7.5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2012.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/fesot_2012.dta", replace
keep bene_id fesot_pdays_2012 fesot_clms_2012 fesot_minfilldt_2012 fesot_totdd_2012 fesot_dmg_2012 fesot_fdays_2012
gen fesot_itotmg_2012 = fesot_dmg_2012*fesot_fdays_2012
gen fesot_tdd_2012 = fesot_itotmg_2012/4
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/fesot_2012.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2012.dta", replace
keep bene_id flavo_pdays_2012 flavo_clms_2012 flavo_minfilldt_2012 flavo_totdd_2012 flavo_dmg_2012 flavo_fdays_2012
gen flavo_itotmg_2012 = flavo_dmg_2012*flavo_fdays_2012
gen flavo_tdd_2012 = flavo_itotmg_2012/300
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2012.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2012.dta", replace
keep bene_id oxybu_pdays_2012 oxybu_clms_2012 oxybu_minfilldt_2012 oxybu_totdd_2012 oxybu_dmg_2012 oxybu_fdays_2012
gen oxybu_itotmg_2012 = oxybu_dmg_2012*oxybu_fdays_2012
gen oxybu_tdd_2012 = oxybu_itotmg_2012/5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2012.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2012.dta", replace
keep bene_id solif_pdays_2012 solif_clms_2012 solif_minfilldt_2012 solif_totdd_2012 solif_dmg_2012 solif_fdays_2012
gen solif_itotmg_2012 = solif_dmg_2012*solif_fdays_2012
gen solif_tdd_2012 = solif_itotmg_2012/5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2012.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2012.dta", replace
keep bene_id tolte_pdays_2012 tolte_clms_2012 tolte_minfilldt_2012 tolte_totdd_2012 tolte_dmg_2012 tolte_fdays_2012
gen tolte_itotmg_2012 = tolte_dmg_2012*tolte_fdays_2012
gen tolte_tdd_2012 = tolte_itotmg_2012/2
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2012.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2012.dta", replace
keep bene_id trosp_pdays_2012 trosp_clms_2012 trosp_minfilldt_2012 trosp_totdd_2012 trosp_dmg_2012 trosp_fdays_2012
gen trosp_itotmg_2012 = trosp_dmg_2012*trosp_fdays_2012
gen trosp_tdd_2012 = trosp_itotmg_2012/20
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2012.dta", replace

////////////////////////	2013 		////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2013.dta", replace
keep bene_id darif_pdays_2013 darif_clms_2013 darif_minfilldt_2013 darif_totdd_2013 darif_dmg_2013 darif_fdays_2013
gen darif_itotmg_2013 = darif_dmg_2013*darif_fdays_2013
gen darif_tdd_2013 = darif_itotmg_2013/7.5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2013.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/fesot_2013.dta", replace
keep bene_id fesot_pdays_2013 fesot_clms_2013 fesot_minfilldt_2013 fesot_totdd_2013 fesot_dmg_2013 fesot_fdays_2013
gen fesot_itotmg_2013 = fesot_dmg_2013*fesot_fdays_2013
gen fesot_tdd_2013 = fesot_itotmg_2013/4
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/fesot_2013.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2013.dta", replace
keep bene_id flavo_pdays_2013 flavo_clms_2013 flavo_minfilldt_2013 flavo_totdd_2013 flavo_dmg_2013 flavo_fdays_2013
gen flavo_itotmg_2013 = flavo_dmg_2013*flavo_fdays_2013
gen flavo_tdd_2013 = flavo_itotmg_2013/300
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2013.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2013.dta", replace
keep bene_id oxybu_pdays_2013 oxybu_clms_2013 oxybu_minfilldt_2013 oxybu_totdd_2013 oxybu_dmg_2013 oxybu_fdays_2013
gen oxybu_itotmg_2013 = oxybu_dmg_2013*oxybu_fdays_2013
gen oxybu_tdd_2013 = oxybu_itotmg_2013/5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2013.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2013.dta", replace
keep bene_id solif_pdays_2013 solif_clms_2013 solif_minfilldt_2013 solif_totdd_2013 solif_dmg_2013 solif_fdays_2013
gen solif_itotmg_2013 = solif_dmg_2013*solif_fdays_2013
gen solif_tdd_2013 = solif_itotmg_2013/5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2013.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2013.dta", replace
keep bene_id tolte_pdays_2013 tolte_clms_2013 tolte_minfilldt_2013 tolte_totdd_2013 tolte_dmg_2013 tolte_fdays_2013
gen tolte_itotmg_2013 = tolte_dmg_2013*tolte_fdays_2013
gen tolte_tdd_2013 = tolte_itotmg_2013/2
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2013.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2013.dta", replace
keep bene_id trosp_pdays_2013 trosp_clms_2013 trosp_minfilldt_2013 trosp_totdd_2013 trosp_dmg_2013 trosp_fdays_2013
gen trosp_itotmg_2013 = trosp_dmg_2013*trosp_fdays_2013
gen trosp_tdd_2013 = trosp_itotmg_2013/20
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2013.dta", replace

////////////////////////	2014 		////////////////////////////////////////
use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2014.dta", replace
keep bene_id darif_pdays_2014 darif_clms_2014 darif_minfilldt_2014 darif_totdd_2014 darif_dmg_2014 darif_fdays_2014
gen darif_itotmg_2014 = darif_dmg_2014*darif_fdays_2014
gen darif_tdd_2014 = darif_itotmg_2014/7.5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/darif_2014.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/fesot_2014.dta", replace
keep bene_id fesot_pdays_2014 fesot_clms_2014 fesot_minfilldt_2014 fesot_totdd_2014 fesot_dmg_2014 fesot_fdays_2014
gen fesot_itotmg_2014 = fesot_dmg_2014*fesot_fdays_2014
gen fesot_tdd_2014 = fesot_itotmg_2014/4
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/fesot_2014.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2014.dta", replace
keep bene_id flavo_pdays_2014 flavo_clms_2014 flavo_minfilldt_2014 flavo_totdd_2014 flavo_dmg_2014 flavo_fdays_2014
gen flavo_itotmg_2014 = flavo_dmg_2014*flavo_fdays_2014
gen flavo_tdd_2014 = flavo_itotmg_2014/300
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/flavo_2014.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2014.dta", replace
keep bene_id oxybu_pdays_2014 oxybu_clms_2014 oxybu_minfilldt_2014 oxybu_totdd_2014 oxybu_dmg_2014 oxybu_fdays_2014
gen oxybu_itotmg_2014 = oxybu_dmg_2014*oxybu_fdays_2014
gen oxybu_tdd_2014 = oxybu_itotmg_2014/5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/oxybu_2014.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2014.dta", replace
keep bene_id solif_pdays_2014 solif_clms_2014 solif_minfilldt_2014 solif_totdd_2014 solif_dmg_2014 solif_fdays_2014
gen solif_itotmg_2014 = solif_dmg_2014*solif_fdays_2014
gen solif_tdd_2014 = solif_itotmg_2014/5
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/solif_2014.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2014.dta", replace
keep bene_id tolte_pdays_2014 tolte_clms_2014 tolte_minfilldt_2014 tolte_totdd_2014 tolte_dmg_2014 tolte_fdays_2014
gen tolte_itotmg_2014 = tolte_dmg_2014*tolte_fdays_2014
gen tolte_tdd_2014 = tolte_itotmg_2014/2
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/tolte_2014.dta", replace

use "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2014.dta", replace
keep bene_id trosp_pdays_2014 trosp_clms_2014 trosp_minfilldt_2014 trosp_totdd_2014 trosp_dmg_2014 trosp_fdays_2014
gen trosp_itotmg_2014 = trosp_dmg_2014*trosp_fdays_2014
gen trosp_tdd_2014 = trosp_itotmg_2014/20
save "/disk/agedisk3/medicare.work/goldman-DUA51866/barthold-dua51866/data/trosp_2014.dta", replace
