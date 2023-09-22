import os
from time import strftime
import argparse, itertools
import jdlfactory

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--mz', type=int, choices=[150, 200, 250, 300, 350, 400, 450, 500, 550, 600, 650], nargs='+')
    parser.add_argument('--rinv', type=float, choices=[.001, .1, .3, .7], nargs='+')
    parser.add_argument('--mdark', type=int, choices=[5, 10, 20], nargs='+')
    parser.add_argument('--boost', type=int, choices=[0, 300], default=300)
    parser.add_argument('-n', '--nevents', type=int, default=500)
    parser.add_argument('--njobs', type=int, required=True)
    parser.add_argument('--startseed', type=int, default=0)
    parser.add_argument('-g', '--go', action='store_true')
    parser.add_argument('--scaleunc', action='store_true')
    args = parser.parse_args()

    group = jdlfactory.Group.from_file('signal_worker.py')
    group.venv()
    group.sh([
        'pip install seutils',
        'pip install https://github.com/tklijnsma/jdlfactory/archive/main.zip',
        'pip install https://github.com/tklijnsma/cmssw_interface/archive/main.zip',
        'pip install "svj_jobs_toolkit>=0.2"',
        ])
    group.htcondor['on_exit_hold'] = '(ExitBySignal == true) || (ExitCode != 0)'
    group.htcondor['request_memory'] = '4000'

    group.group_data['tarball_search_path'] = 'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/mgtarballs/2023MADPT/'
    group.group_data['tarball'] = (
        'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/svjproductiontarballs/'
        # 'CMSSW_10_6_29_patch1_svjprod_el7_2018UL_kpedro88_madpt_withHLT_85c685d_Oct11.tar.gz'
        'CMSSW_10_6_29_patch1_svjprod_el7_2018UL_cms-svj_Run2_UL_withHLT_996c8dc_Jan18.tar.gz'
        )
    group.group_data['n_events_gridpack'] = 10000
    group.group_data['stageout'] = (
        'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/signal_madpt{}_2023'
        .format(args.boost)
        )
    if args.scaleunc: group.group_data['stageout'] += '_scaleunc'
    group.group_data['stageout'] += '/'
    group.group_data['scaleunc'] = bool(args.scaleunc)


    data = dict(n=args.nevents, boostvar='madpt', boost=args.boost)

    for mz, rinv, mdark in itertools.product(args.mz, args.rinv, args.mdark):
        group.jobs = []
        data['mz'] = mz
        data['rinv'] = rinv
        data['mdark'] = mdark
        for i in range(1, args.njobs+1):
            data['i'] = args.startseed + i
            group.add_job(data)

        group_name = strftime('signal_madpt{}_mz{}_rinv{}_mdark{}_%b%d_%H%M%S'.format(args.boost, mz, rinv, mdark))
        if args.go:
            group.prepare_for_jobs(group_name)
            os.system('cd {}; condor_submit submit.jdl'.format(group_name))
        else:
            group.run_locally(keep_temp_dir=True)
            return

if __name__ == '__main__':
    main()