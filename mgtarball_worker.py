import os, os.path as osp

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
        physics.filename('step0_GRIDPACK', ext='.tar.xz')
        )
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
    dst = group_data.stageout + osp.basename(outfile)
    svj.logger.info('Staging out %s -> %s', outfile, dst)
    seutils.cp(outfile, dst)

main()