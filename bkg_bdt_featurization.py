import os, os.path as osp, re, traceback
from jdlfactory_server import data, group_data # type: ignore
import seutils # type: ignore
import svj_ntuple_processing as svj # type: ignore


seutils.set_preferred_implementation(group_data.storage_implementation.encode())


def metadata_from_path(path):
    """
    Uses the filename of a rootfile to compile a metadata dict.
    """
    metadata = {}
    dirname = osp.basename(osp.dirname(path))
    year_dir = osp.basename(osp.dirname(osp.dirname(path)))

    for year in ['16', '17', '18']:
        if year in year_dir:
            metadata['year'] = 2000 + int(year)
            break
    else:
        raise Exception('Could not determine year from filename %s' % path)

    for bkg_type in ['qcd', 'ttjets', 'wjets', 'zjets']:
        if dirname.lower().startswith(bkg_type):
            metadata['bkg_type'] = bkg_type
            break
    else:
        raise Exception('Could not determine bkg type from filename %s' % path)

    if 'HT' in dirname:
        match = re.search(r'HT\-(\d+)[tT]o([\dInf]+)', dirname)
        metadata['htbin'] = [float(match.group(1)), float(match.group(2))]
    elif 'Pt' in dirname:
        match = re.search(r'Pt_(\d+)to([\dInf]+)', dirname)
        metadata['ptbin'] = [float(match.group(1)), float(match.group(2))]

    if bkg_type == 'ttjets':
        if 'SingleLep' in dirname:
            metadata['n_lepton_sample'] = 1
        elif 'DiLep' in dirname:
            metadata['n_lepton_sample'] = 2

        if 'genMET' in dirname: metadata['genmet_sample'] = True

    return metadata


def dst(path):
    """
    Generates a destination .npz for a rootfile
    """
    # Get the stump starting from the dir with year in it
    path = '/'.join(path.split('/')[-3:])
    path = path.replace('.root', '.npz')
    return group_data.stageout + ('' if group_data.stageout.endswith('/') else '/') + path


def modified_preselection(array):
    """
    This function is a copy of svj_ntuple_processing.filter_preselection():
    https://github.com/boostedsvj/svj_ntuple_processing/blob/main/svj_ntuple_processing/__init__.py#L228

    Only use it for testing or studies.
    For final samples we should use the svj_ntuple_processing package!
    """
    import numpy as np, awkward as ak, pprint
    UL = True

    copy = array.copy()
    a = copy.array
    cutflow = copy.cutflow
    
    # AK8Jet.pT>500
    a = a[ak.count(a['JetsAK8.fCoordinates.fPt'], axis=-1)>=1] # At least one jet
    a = a[a['JetsAK8.fCoordinates.fPt'][:,0]>500.] # leading>500
    cutflow['ak8jet.pt>500'] = len(a)

    # Triggers
    trigger_indices = np.array([copy.trigger_branch.index(t) for t in copy.triggers])
    if len(a):
        trigger_decisions = a['TriggerPass'].to_numpy()[:,trigger_indices]
        a = a[(trigger_decisions == 1).any(axis=-1)]
    cutflow['triggers'] = len(a)

    # At least 2 AK15 jets
    a = a[ak.count(a['JetsAK15.fCoordinates.fPt'], axis=-1) >= 2]
    cutflow['n_ak15jets>=2'] = len(a)

    # subleading eta < 2.4 eta
    a = a[a['JetsAK15.fCoordinates.fEta'][:,1]<2.4]
    cutflow['subl_eta<2.4'] = len(a)

    # positive ECF values
    for ecf in [
        'JetsAK15_ecfC2b1', 'JetsAK15_ecfD2b1',
        'JetsAK15_ecfM2b1', 'JetsAK15_ecfN2b2',
        ]:
        a = a[a[ecf][:,1]>0.]
    cutflow['subl_ecf>0'] = len(a)

    # rtx>1.1
    svj.logger.warning('rtx cut is disabled!')
    # rtx = np.sqrt(1. + a['MET'].to_numpy() / a['JetsAK15.fCoordinates.fPt'][:,1].to_numpy())
    # a = a[rtx>1.1]
    cutflow['rtx>1.1'] = len(a)

    # lepton vetoes
    a = a[(a['NMuons']==0) & (a['NElectrons']==0)]
    cutflow['nleptons=0'] = len(a)

    # MET filters
    for b in [
        'HBHENoiseFilter',
        'HBHEIsoNoiseFilter',
        'eeBadScFilter',
        'ecalBadCalibFilter' if UL else 'ecalBadCalibReducedFilter',
        'BadPFMuonFilter',
        'BadChargedCandidateFilter',
        'globalSuperTightHalo2016Filter',
        ]:
        a = a[a[b]!=0] # Pass events if not 0, is that correct?
    cutflow['metfilter'] = len(a)
    cutflow['preselection'] = len(a)

    copy.array = a
    svj.logger.debug('cutflow:\n%s', pprint.pformat(copy.cutflow))
    return copy



failed_rootfiles = []

for i, rootfile in enumerate(data.rootfiles):
    try:
        outfile = dst(rootfile)
        if seutils.isfile(outfile):
            svj.logger.info('File %s exists, skipping', outfile)
            continue
        svj.logger.info('Processing rootfile %s/%s: %s', i, len(data.rootfiles)-1, rootfile)

        array = svj.open_root(rootfile)
        array.metadata = metadata_from_path(rootfile)

        if group_data.modified_preselection:
            svj.logger.warning('Using MODIFIED preselection defined locally!')
            array = modified_preselection(array)
        else:
            array = svj.filter_preselection(array)

        array = svj.filter_stitch(array)
        cols = svj.bdt_feature_columns(array)
        # Check again, to avoid race conditions
        if seutils.isfile(outfile):
            svj.logger.info('File %s exists now, not staging out', outfile)
            continue
        cols.save(outfile)

    except Exception:
        failed_rootfiles.append(rootfile)
        svj.logger.error('Error processing %s; continuing. Error:\n%s', rootfile, traceback.format_exc())


if failed_rootfiles:
    svj.logger.info(
        'Failures were encountered for the following rootfiles:\n%s',
        '\n'.join(failed_rootfiles)
        )
