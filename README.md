# Updated job submission for bSVJ

## Setup

```
python3 -m venv env
source env/bin/activate  # Needed every time
pip install https://github.com/tklijnsma/jdlfactory/archive/main.zip
pip install https://github.com/tklijnsma/cmssw_interface/archive/main.zip
pip install https://github.com/tklijnsma/svj_jobs_toolkit/archive/main.zip
```

## Create MadGraph tarballs (aka gridpacks)

Example command:

```
# Don't forget to renew your grid proxy
python mgtarball_submit.py --mz 250 350 450 --rinv .001 .1 .3 .7 --boost 300 --mdark 5 10 20 --go
cd jobs_mgtarball_36_Oct24_153208
condor_submit submit.jdl
```

One job per combination of given mz/rinv/mdark/boost is generated. Default stageout is `root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/mgtarballs/2022MADPT/`.

Submitted jobs:

```
# Oct 24 2022
python mgtarball_submit.py --mz 250 350 450 --rinv .001 .1 .3 .7 --boost 300 --mdark 5 10 20 --go

# Oct 25 2022
python mgtarball_submit.py --mz 150 200 300 400 500 --rinv .001 .1 .3 .7 --boost 300 --mdark 5 10 20 --go

# Oct 26 2022
python mgtarball_submit.py --mz 550 600 650 --rinv .001 .1 .3 .7 --boost 300 --mdark 5 10 20 --go
```

## Signal production

```
# Oct 24 2022
python submit_madpt_signal.py --mz 250 350 450 --rinv .1 .3 --mdark 10 --njobs 400 --go

# Oct 25 2022
python submit_madpt_signal.py --mz 250 350 450 --rinv .1 .3 --mdark 10 --njobs 600 --startseed 400 --go

# Oct 26 2022
python submit_madpt_signal.py --mz 150 --rinv .1 .3 --mdark 10 --njobs 1000 --go
```