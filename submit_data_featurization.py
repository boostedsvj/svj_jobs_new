import os, os.path as osp, json, argparse, fnmatch, random, math
from time import strftime

import jdlfactory
import seutils


datadir = 'root://cmseos.fnal.gov//store/user/lpcsusyhad/SusyRA2Analysis2015/Run2ProductionV20'
years = ['Run2016B-UL2016_HIPM-ver2-v2', 'Run2016C-UL2016_HIPM-v2', 'Run2016D-UL2016_HIPM-v2', 'Run2016E-UL2016_HIPM-v2', 'Run2016F-UL2016_HIPM-v2', 'Run2016G-UL2016-v2', 'Run2016H-UL2016-v2', 'Run2017B-UL2017-v1', 'Run2017C-UL2017-v1', 'Run2017D-UL2017-v1', 'Run2017E-UL2017-v1', 'Run2017F-UL2017-v1', 'Run2018A-UL2018-v1', 'Run2018B-UL2018-v1', 'Run2018C-UL2018-v1', 'Run2018D-UL2018-v2']
data_prefixs = ['JetHT']

def get_list_of_all_rootfiles():
    """
    Gets list of all bkg rootfiles.
    Stores result in a cache file, since the operation is somewhat slow (~5 min).
    """
    cache_file = 'cached_data_rootfiles.json'
    if osp.isfile(cache_file):
        jdlfactory.logger.info('Returning cached list of rootfiles')
        with open(cache_file, 'rb') as f:
            return json.load(f)

    seutils.MAX_RECURSION_DEPTH = 100
    jdlfactory.logger.info('Rebuilding bkg filelist...')
    rootfiles = []
    for year in years:
        for data_prefix in data_prefixs:
            pat = '{datadir}/{year}/{data_prefix}/*.root'.format(datadir=datadir, **locals())
            jdlfactory.logger.info('Querying for pattern %s...', pat)
            rootfiles_for_pat = seutils.ls_wildcard(pat)
            jdlfactory.logger.info('  {} rootfiles found'.format(len(rootfiles_for_pat)))
            rootfiles.extend(rootfiles_for_pat)

    jdlfactory.logger.info('Caching list of {} rootfiles to {}'.format(len(rootfiles), cache_file))
    with open(cache_file, 'wb') as f:
        json.dump(rootfiles, f)
    return rootfiles


def get_list_of_existing_dsts(stageout, cache_file='cached_existing_npzs.json'):
    if osp.isfile(cache_file):
        jdlfactory.logger.info('Returning cached list of existing npz files')
        with open(cache_file, 'rb') as f:
            return json.load(f)

    jdlfactory.logger.info('Building list of all existing .npz files. This can take ~10-15 min.')
    seutils.MAX_RECURSION_DEPTH = 1000
    existing = []
    for path, _, files in seutils.walk(stageout):
        jdlfactory.logger.info(path)
        existing.extend(fnmatch.filter(files, '*.npz'))

    jdlfactory.logger.info('Caching list of {} npz files to {}'.format(len(existing), cache_file))
    with open(cache_file, 'wb') as f:
        json.dump(existing, f)

    return existing


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-g', '--go', action='store_true')
    parser.add_argument('--missing', action='store_true')
    parser.add_argument('--listmissing', action='store_true')
    parser.add_argument('--modifiedpreselection', action='store_true')
    parser.add_argument('--stageout', type=str, help='stageout directory', required=True)
    parser.add_argument('--impl', type=str, help='storage implementation', default='xrd', choices=['xrd', 'gfal'])
    parser.add_argument('--singlejob', action='store_true', help='Single job for testing.')
    args = parser.parse_args()

    group = jdlfactory.Group.from_file('data_featurization.py')
    group.venv()
    group.sh('pip install seutils')
    group.sh('pip install enum')
    group.sh('pip install --ignore-installed --no-cache numpy')
    group.sh('pip install --ignore-installed --no-cache awkward')
    #group.sh('pip install svj_ntuple_processing==0.9')
    group.sh('pip install https://github.com/boostedsvj/svj_ntuple_processing/archive/main.zip')
    
    group.htcondor['on_exit_hold'] = '(ExitBySignal == true) || (ExitCode != 0)'

    group.group_data['modified_preselection'] = bool(args.modifiedpreselection)
    group.group_data['stageout'] = args.stageout
    group.group_data['storage_implementation'] = args.impl


    rootfiles = get_list_of_all_rootfiles()
    # 64249 rootfiles, approx 10s per file means approx 180 CPU hours needed
    # do 5h per job --> 180/5 = 36 jobs with 1785 files each
    # Little bit more liberal with no. of jobs:
    # ~80 jobs -> 800 rootfiles per job
    # ~320 jobs -> 200 rootfiles per job
    n_per_job = 100

    if args.missing or args.listmissing:
        existing_dsts = get_list_of_existing_dsts(group.group_data['stageout'])

        def dst(path):
            # Get the stump starting from the dir with year in it
            path = '/'.join(path.split('/')[-3:])
            path = path.replace('.root', '.npz')
            return osp.join(group.group_data['stageout'], path)

        needed_dsts = [dst(f) for f in rootfiles]
        missing_dsts = set(needed_dsts) - set(existing_dsts)

        rootfiles_for_missing_dsts = []
        for d in missing_dsts:
            rootfiles_for_missing_dsts.append(rootfiles[needed_dsts.index(d)])
        rootfiles = rootfiles_for_missing_dsts

    if args.listmissing:
        rootfiles.sort()
        jdlfactory.logger.info(
            'Missing %s .npz files for the following rootfiles:\n%s',
            len(missing_dsts),
            '  '+'\n  '.join(rootfiles)
            )
        return

    if args.missing:
        random.shuffle(rootfiles) # To avoid job load imbalance
        n_per_job = 10
        jdlfactory.logger.info(
            'Missing %s .npz files; submitting %s jobs',
            len(missing_dsts),
            int(math.ceil(len(missing_dsts)/float(n_per_job)))
            )

    for i in range(0, len(rootfiles), n_per_job):
        group.add_job({'rootfiles' : rootfiles[i:i+n_per_job]})
        if args.singlejob: break


    group_name = strftime('datafeat_%b%d_%H%M%S')
    if args.missing: group_name += '_missing'
    if args.modifiedpreselection: group_name += '_modpresel'

    if args.go:
        group.prepare_for_jobs(group_name)
        os.system('cd {}; condor_submit submit.jdl'.format(group_name))
    else:
        group.run_locally(keep_temp_dir=False)


if __name__ == '__main__':
    main()
