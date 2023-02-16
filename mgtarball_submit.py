import os
import jdlfactory, itertools, argparse
from time import strftime

group = jdlfactory.Group.from_file('mgtarball_worker.py')
group.venv()
group.sh([
    'pip install seutils',
    'pip install https://github.com/tklijnsma/jdlfactory/archive/main.zip',
    'pip install https://github.com/tklijnsma/cmssw_interface/archive/main.zip',
    'pip install "svj_jobs_toolkit>=0.2"',
    ])
group.htcondor['on_exit_hold'] = '(ExitBySignal == true) || (ExitCode != 0)'
group.htcondor['request_memory'] = '4000'
group.htcondor['+DESIRED_Sites'] = '"T0_CH_CERN,T1_DE_KIT,T1_ES_PIC,T1_FR_CCIN2P3,T1_IT_CNAF,T1_RU_JINR,T1_UK_RAL,T1_US_FNAL,T2_AT_Vienna,T2_BE_IIHE,T2_BE_UCL,T2_BR_SPRACE,T2_BR_UERJ,T2_CH_CERN,T2_CH_CERN_AI,T2_CH_CERN_HLT,T2_CH_CSCS_HPC,T2_CN_Beijing,T2_DE_RWTH,T2_ES_CIEMAT,T2_ES_IFCA,T2_FI_HIP,T2_FR_CCIN2P3,T2_FR_GRIF_IRFU,T2_FR_GRIF_LLR,T2_FR_IPHC,T2_GR_Ioannina,T2_HU_Budapest,T2_IN_TIFR,T2_IT_Bari,T2_IT_Legnaro,T2_IT_Pisa,T2_IT_Rome,T2_KR_KNU,T2_MY_UPM_BIRUNI,T2_PL_Swierk,T2_PT_NCG_Lisbon,T2_RU_IHEP,T2_RU_INR,T2_RU_ITEP,T2_RU_JINR,T2_RU_PNPI,T2_RU_SINP,T2_TH_CUNSTDA,T2_TW_NCHC,T2_UA_KIPT,T2_UK_London_Brunel,T2_UK_SGrid_Bristol,T2_UK_SGrid_RALPP,T2_US_Caltech,T2_US_MIT,T2_US_Nebraska,T2_US_Purdue,T2_US_UCSD,T2_US_Wisconsin,T3_BG_UNI_SOFIA,T3_BY_NCPHEP,T3_CH_CERN_CAF,T3_CH_PSI,T3_CN_PKU,T3_CO_Uniandes,T3_ES_Oviedo,T3_FR_IPNL,T3_GR_IASA,T3_IN_PUHEP,T3_IN_TIFRCloud,T3_IT_Bologna,T3_IT_Perugia,T3_KR_KNU,T3_KR_UOS,T3_MX_Cinvestav,T3_RU_FIAN,T3_RU_MEPhI,T3_TW_NCU,T3_TW_NTU_HEP,T3_UK_London_QMUL,T3_UK_London_RHUL,T3_UK_SGrid_Oxford,T3_UK_ScotGrid_GLA,T3_US_Baylor,T3_US_Colorado,T3_US_Cornell,T3_US_FIT,T3_US_FIU,T3_US_FNALLPC,T3_US_FSU,T3_US_JHU,T3_US_Kansas,T3_US_MIT,T3_US_NERSC,T3_US_NU,T3_US_NotreDame,T3_US_OSG,T3_US_OSU,T3_US_Omaha,T3_US_Princeton_ICSE,T3_US_PuertoRico,T3_US_Rice,T3_US_Rutgers,T3_US_SDSC,T3_US_TAMU,T3_US_TTU,T3_US_UCD,T3_US_UCSB,T3_US_UMD,T3_US_UMiss"'

group.group_data['stageout'] = 'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/mgtarballs/2023MADPT/'
group.group_data['tarball'] = (
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/svjproductiontarballs/'
    # 'CMSSW_10_6_29_patch1_svjprod_el7_2018UL_kpedro88_madpt_withHLT_85c685d_Oct11.tar.gz'
    'CMSSW_10_6_29_patch1_svjprod_el7_2018UL_cms-svj_Run2_UL_withHLT_996c8dc_Jan18.tar.gz'
    )

parser = argparse.ArgumentParser()
parser.add_argument('--mz', type=int, nargs='+')
parser.add_argument('--rinv', type=float, nargs='+')
parser.add_argument('--mdark', type=int, nargs='+')
parser.add_argument('--boost', type=int, nargs='+')
parser.add_argument('-g', '--go', action='store_true')
args = parser.parse_args()

for mz, rinv, mdark, boost in itertools.product(args.mz, args.rinv, args.mdark, args.boost):
    group.add_job({
        'mz': mz, 'mdark': mdark, 'rinv': rinv,
        'boostvar': 'madpt', 'n': 10000, 'boost': boost
        })

if args.go:
    jobdir = strftime('jobs_mgtarball_{}_%b%d_%H%M%S'.format(len(group.jobs)))
    group.prepare_for_jobs(jobdir)
    os.system('cd {}; condor_submit submit.jdl'.format(jobdir))
else:
    group.run_locally(keep_temp_dir=False)
