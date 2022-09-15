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

data = dict(
    stageout = 'root://cmseos.fnal.gov//store/user/lpcdarkqcd/MCSamples_UL_Spring2022_NOBOOST',
    boost = 0.,
    mz = 250,
    mdark = 10,
    n = 5000,
    )
for rinv in [0.001, .1, .3, .7]:
    for i in range(1, 201):
        data['rinv'] = rinv
        data['i'] = i
        group.add_job(data)

from time import strftime
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('-r', '--real', action='store_true')
args = parser.parse_args()

if args.real:
    group.prepare_for_jobs(strftime('signal_nocuts_%b%d_%H%M%S'))
else:
    group.run_locally(keep_temp_dir=False)
