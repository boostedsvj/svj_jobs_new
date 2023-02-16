# Updated job submission for bSVJ

## Setup

```
python3 -m venv env
source env/bin/activate  # Needed every time
pip install https://github.com/tklijnsma/jdlfactory/archive/main.zip
pip install https://github.com/tklijnsma/cmssw_interface/archive/main.zip
pip install https://github.com/tklijnsma/svj_jobs_toolkit/archive/main.zip
```

If you want to test jobs locally, include the following installs as well:

```
pip install numpy
pip install awkward
pip install uproot
pip install svj_ntuple_processing==0.3  # (or newer)
```

# BDT featurization

Only for background. Signal featurization can be run <1 min locally, for which there is a separate script (to be linked here!).

```bash
# Don't forget to renew your grid proxy
python submit_bkg_bdt_featurization.py --go --stageout root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/bdt_features/bkg_nov14/BDTFEATURES
cd bkgbdtfeat_Nov15_173735
condor_submit submit.jdl
# Wait ~10h until most jobs are completed. There will be some jobs that are slow.
# Using the --missing flag, the script determines which files are still missing,
# and resubmits many jobs to do only the missing ones.
python submit_bkg_bdt_featurization.py --missing --go --stageout root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/bdt_features/bkg_nov14/BDTFEATURES
cd bkgbdtfeat_Nov16_103923_missing
condor_submit submit.jdl

# 11 Feb 2023, fixed the metdphi issue:
python submit_bkg_bdt_featurization.py --impl xrd --go --stageout root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/bdt_features/bkg_feb11/BDTFEATURES
```


### Preselection studies

There is a `modified_preselection` function in [bkg_bdt_featurization.py](bkg_bdt_featurization.py). Submit jobs as follows to use the modified_preselection:

```bash
python submit_bkg_bdt_featurization.py --modifiedpreselection --stageout davs://hepcms-se2.umd.edu:1094//store/user/thomas.klijnsma/bkg_nov23_modpresel/BDTFEATURES --impl gfal --go
cd bkgbdtfeat_Nov23_173735_modpresel
condor_submit submit.jdl
```


# Production (up to TreeMaker ntuples)

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

```bash
# Oct 24 2022
python mgtarball_submit.py --mz 250 350 450 --rinv .001 .1 .3 .7 --boost 300 --mdark 5 10 20 --go

# Oct 25 2022
python mgtarball_submit.py --mz 150 200 300 400 500 --rinv .001 .1 .3 .7 --boost 300 --mdark 5 10 20 --go

# Oct 26 2022
python mgtarball_submit.py --mz 550 600 650 --rinv .001 .1 .3 .7 --boost 300 --mdark 5 10 20 --go

# Feb 15 2023 -- Updated SVJProductions tarball from cms-svj repository
python mgtarball_submit.py --mz 250 350 --rinv .1 .3 --boost 0 300 --mdark 10 --go
```

## Signal production

```bash
# Oct 24 2022
python submit_madpt_signal.py --mz 250 350 450 --rinv .1 .3 --mdark 10 --njobs 400 --go

# Oct 25 2022
python submit_madpt_signal.py --mz 250 350 450 --rinv .1 .3 --mdark 10 --njobs 600 --startseed 400 --go

# Oct 26 2022
python submit_madpt_signal.py --mz 150 --rinv .1 .3 --mdark 10 --njobs 1000 --go

# Oct 27 2022
python submit_madpt_signal.py --mz 550 --rinv .1 .3 --mdark 10 --njobs 1000 --go
python submit_madpt_signal.py --mz 250 350 --rinv .3 --mdark 10 --njobs 1000 --startseed 1000 --go

# Jan 13 2023: Rerunning with MINIAOD saved
python submit_madpt_signal.py --mz 200 250 350 450 550 --rinv .1 .3 --mdark 10 --njobs 2000 --startseed 5000 --go

# Feb 16: 0-boost run
python submit_madpt_signal.py --boost 0 --mz 250 350 --rinv .1 .3 --mdark 10 --njobs 2000 --nevents 300 --startseed 0 --go
```