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