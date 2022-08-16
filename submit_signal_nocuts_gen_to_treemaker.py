import jdlfactory
group = jdlfactory.Group.from_file('signal_worker.py')
group.venv()
group.sh([
    'pip install seutils',
    'pip install https://github.com/tklijnsma/jdlfactory/archive/main.zip',
    'pip install https://github.com/tklijnsma/cmssw_interface/archive/main.zip',
    'pip install https://github.com/tklijnsma/svj_jobs_toolkit/archive/main.zip',
    ])
data = dict(
    stageout = 'root://cmseos.fnal.gov//store/user/lpcdarkqcd/MCSamples_UL_Spring2022_NOBOOST_test2',
    boost = 0.,
    mz = 250,
    mdark = 10,
    rinv = .3,
    n = 10,
    i = 1,
    )
for i in range(1, 501):
    data['i'] = i
    group.add_job(data)

group.run_locally(keep_temp_dir=False)