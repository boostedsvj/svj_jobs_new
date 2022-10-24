import jdlfactory
group = jdlfactory.Group.from_file('signal_worker.py')
group.venv()
group.sh([
    'pip install seutils',
    'pip install https://github.com/tklijnsma/jdlfactory/archive/main.zip',
    'pip install https://github.com/tklijnsma/cmssw_interface/archive/main.zip',
    'pip install https://github.com/tklijnsma/svj_jobs_toolkit/archive/main.zip',
    ])
group.htcondor['on_exit_hold'] = '(ExitBySignal == true) || (ExitCode != 0)'
group.htcondor['request_memory'] = '4000'

group.group_data['stageout'] = 'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/orthogonalitystudy/'
group.group_data['tarball_search_path'] = 'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/mgtarballs/2022MADPT/'
group.group_data['tarball'] = (
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/svjproductiontarballs/'
    'CMSSW_10_6_29_patch1_svjprod_el7_2018UL_kpedro88_madpt_withHLT_85c685d_Oct11.tar.gz'
    )
group.group_data['n_events_gridpack'] = 10000


data = dict(mz=250, mdark=10, n=500, rinv=.3, boostvar='madpt')
for boost in [0., 200., 400.]:
    data['boost'] = boost
    for i in range(1, 41):
        data['i'] = i
        group.add_job(data)

from time import strftime
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('-g', '--go', action='store_true')
args = parser.parse_args()

if args.go:
    group.prepare_for_jobs(strftime('signal_orthostudy_%b%d_%H%M%S'))
else:
    group.run_locally(keep_temp_dir=False)
