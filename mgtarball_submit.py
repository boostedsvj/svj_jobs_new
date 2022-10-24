import jdlfactory
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

for boost in [0, 200, 400]:
    for mz in [250, 350, 450]:
        for rinv in [.001, .1, .3, .5, .7]:
            for mdark in [5., 10., 20.]:
                group.add_job({
                    'mz': mz, 'mdark': mdark, 'rinv': rinv,
                    'boostvar': 'madpt', 'n': 10000, 'boost': boost
                    })

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('-g', '--go', action='store_true')
args = parser.parse_args()

if args.go:
    from time import strftime
    group.prepare_for_jobs(strftime(f'jobs_mgtarball_{len(group.jobs)}_%b%d_%H%M%S'))
else:
    group.run_locally(keep_temp_dir=False)
