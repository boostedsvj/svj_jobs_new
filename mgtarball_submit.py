import jdlfactory, itertools, argparse
from time import strftime

group = jdlfactory.Group.from_file('mgtarball_worker.py')
group.venv()
group.sh([
    'pip install seutils',
    'pip install https://github.com/tklijnsma/jdlfactory/archive/main.zip',
    'pip install https://github.com/tklijnsma/cmssw_interface/archive/main.zip',
    'pip install https://github.com/tklijnsma/svj_jobs_toolkit/archive/main.zip',
    ])
group.htcondor['on_exit_hold'] = '(ExitBySignal == true) || (ExitCode != 0)'
group.htcondor['request_memory'] = '4000'

group.group_data['stageout'] = 'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/mgtarballs/2022MADPT/'
group.group_data['tarball'] = (
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/svjproductiontarballs/'
    'CMSSW_10_6_29_patch1_svjprod_el7_2018UL_kpedro88_madpt_withHLT_85c685d_Oct11.tar.gz'
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
    group.prepare_for_jobs(strftime('jobs_mgtarball_{}_%b%d_%H%M%S'.format(len(group.jobs))))
else:
    group.run_locally(keep_temp_dir=False)
