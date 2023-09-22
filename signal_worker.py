import os, os.path as osp

from jdlfactory_server import data, group_data # type: ignore

import seutils # type: ignore
from cmssw_interface import CMSSW # type: ignore
import svj_jobs_toolkit as svj # type: ignore


if group_data.scaleunc:
    svj.logger.info('Setting DO_MG_SYSTEMATICS to True')
    os.environ['DO_MG_SYSTEMATICS'] = 'true'
else:
    svj.logger.info('Not doing scale uncertainties.')


cmssw = CMSSW.from_tarball(group_data.tarball)
cmssw_for_hlt = CMSSW(osp.join(cmssw.path, '../HLT/CMSSW_10_2_16_UL/'))
cmssw_treemaker = CMSSW.from_tarball(
    'root://cmseos.fnal.gov//store/user/lpcdarkqcd/boosted/svjproductiontarballs/'
    'CMSSW_10_6_29_patch1_treemaker_el7_2018_f9c563c_Jun03.tar.gz',
    dst='.'
    )
    
physics = svj.Physics({
    'year' : 2018,
    'mz' : data.mz,
    'mdark' : data.mdark,
    'rinv' : data.rinv,
    'boost': data.boost,
    'boostvar' : getattr(data, 'boostvar', 'genjetpt'),
    'max_events' : data.n,
    'part' : data.i
    })


def dst_for_step(step):
    """Stageout dst for created rootfiles"""
    return (
        '{stageout}/{step}/'
        '{boostvar}{boost:.0f}_mz{mz:.0f}_mdark{mdark:.0f}_rinv{rinv}/{i}.root'
        .format(
            stageout = group_data.stageout,
            step = step.replace('step_', ''),
            i = data.i,
            **physics
            )
        )


def set_start_step(steps):
    """
    Finds the step at which to start (a previous failed job may have
    already produced some output).
    """
    for step in reversed(steps):
        dst = dst_for_step(step)
        if seutils.isfile(dst):
            index = steps.index(step)
            svj.logger.info('Found %s; Doing remaining steps: %s', dst, steps[index+1:])
            return steps[index+1:], dst_for_step(step)
    return steps, None


def main():
    steps = [
        'step_LHE-GEN', 'step_SIM', 'step_DIGI',
        'step_HLT', 'step_RECO', 'step_MINIAOD', 'TREEMAKER'
        ]

    steps, rootfile = set_start_step(steps)

    if not steps:
        svj.logger.info('Nothing to do!')
        return

    svj.logger.info('Doing steps %s', '->'.join(steps))

    # Process rest of the steps
    prev_step = None
    prev_rootfile = None
    for step in steps:
        svj.logger.info('Doing step %s', step)
        try:
            if step == 'TREEMAKER':
                rootfile = svj.run_treemaker(cmssw_treemaker, rootfile)
            elif step == 'step_LHE-GEN':
                svj.download_madgraph_tarball(cmssw, svj.Physics(physics, max_events=group_data.n_events_gridpack), group_data.tarball_search_path)
                rootfile = svj.run_step(
                    cmssw, 'step_LHE-GEN',
                    svj.Physics(physics, maxEventsIn=group_data.n_events_gridpack),
                    inpre='step0_GRIDPACK'
                    )
            else:
                rootfile = svj.run_step(
                    (cmssw_for_hlt if step=='step_HLT' else cmssw),
                    step, physics, in_rootfile=rootfile, inpre=prev_step
                    )

            # Stageout
            if step in ['step_MINIAOD', 'TREEMAKER']:
                svj.logger.info('Staging out %s -> %s', rootfile, dst_for_step(step))
                seutils.cp(rootfile, dst_for_step(step))

        except Exception:
            if prev_rootfile:
                svj.logger.error(
                    'Encountered exception; Staging out %s to %s so resubmissions'
                    ' can continue from that point onward',
                    prev_rootfile, dst_for_step(prev_step)
                    )
                seutils.cp(prev_rootfile, dst_for_step(prev_step))
            raise
        prev_step = step
        prev_rootfile = rootfile


main()
