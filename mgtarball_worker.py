import os, os.path as osp, uuid

from jdlfactory_server import data, group_data # type: ignore

import seutils # type: ignore
from cmssw_interface import CMSSW, logger # type: ignore
import svj_jobs_toolkit as svj # type: ignore

cmssw = CMSSW.from_tarball(group_data.tarball)

# The gridpack_generation.sh script relies on 'git rev-parse --show-toplevel', 
# but the tarball packing stripped any .git directories.
# Fix: add an empty .git directory in Configuration/GenProduction, so that
# gridpack_generation.sh gets a valid value for 'git rev-parse --show-toplevel'
cmssw.run(['cd Configuration/GenProduction', 'git init'])

def make_mgtarball(cmssw, physics=None, nogridpack=False):
    cmd = (
        "python runMG.py"
        " year={year}"
        " madgraph=1"
        " channel=s"
        " outpre=step0_GRIDPACK"
        " mMediator={mz:.0f}"
        " mDark={mdark:.0f}"
        " rinv={rinv}"
        " boostvar={boostvar}"
        " boost={boost}"
        .format(**physics)
        )
    if physics["max_events"] > 0.0:
        cmd += " maxEvents={}".format(physics["max_events"])
    if nogridpack:
        cmd += " nogridpack=1"
    cmssw.run(['cd SVJ/Production/test', cmd])
    outfile = osp.join(
        cmssw.src, 'SVJ/Production/test',
        "step0_GRIDPACK_s-channel_mMed-{mz:.0f}_mDark-{mdark:.0f}"
        "{boost_str}_13TeV-madgraphMLM-pythia8{max_events_str}.tar.xz".format(
            boost_str=physics.boost_str(),
            max_events_str=physics.max_events_str(),
            **physics
            )
        )
    # step0_GRIDPACK_s-channel_mMed-250_mDark-10_MADPT300_13TeV-madgraphMLM-pythia8_n-10000.tar.xz
    logger.info('Outfile of gridpack generation: %s', outfile)
    logger.info(os.listdir(osp.join(cmssw.src, 'SVJ/Production/test')))
    return outfile

def main():
    physics = svj.Physics({
        'year' : 2018,
        'mz' : data.mz,
        'mdark' : data.mdark,
        'rinv' : data.rinv,
        'boost': data.boost,
        'boostvar' : data.boostvar,
        'max_events' : data.n,
        })
    outfile = make_mgtarball(cmssw, physics)

    # Real stageout
    dst = group_data.stageout + physics.gridpack_filename()
    svj.logger.info('Staging out %s -> %s', outfile, dst)
    seutils.cp(outfile, dst)

main()