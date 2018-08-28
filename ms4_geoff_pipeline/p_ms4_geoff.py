from mountainlab_pytools import mdaio
from mountainlab_pytools import mlproc as mlp
import os
import json

processor_name = 'ms4_geoff.sort'
processor_version = '0.1.0'


def sort_dataset(*, raw_fname=None, pre_fname=None, geom_fname=None, params_fname=None,
                 pre_out_fname=None, firings_out=None, metrics_out_fname=None,
                 freq_min=300, freq_max=7000, samplerate=30000, detect_sign=1,
                 adjacency_radius=-1, detect_threshold=3, detect_interval=50, clip_size=50,
                 firing_rate_thresh=0.05, isolation_thresh=0.95, noise_overlap_thresh=0.03,
                 peak_snr_thresh=1.5, opts={}):
    """
        Custom Sorting Pipeline. It will pre-process, sort, and curate (using ms_taggedcuration pipeline).

        Parameters
        ----------
        raw_fname : INPUT
            MxN raw timeseries array (M = #channels, N = #timepoints). If you input this it will pre-process the data.
        pre_fname : INPUT
            MxN pre-processed array timeseries array (M = #channels, N = #timepoints). This is if you want to analyze already pre-processed data.
        geom_fname : INPUT
            Optional geometry file (.csv format).
        params_fname : INPUT
            Optional parameter file (.json format), where the key is the any of the parameters for this pipeline. Any values in this .json file will overwrite any defaults.

        firings_out : OUTPUT
            The filename that will contain the spike data (.mda file), default to '/firings.mda'
        pre_out_fname : OUTPUT
            Optional filename for the pre-processed data.
        metrics_out_fname : OUTPUT
            The output filename (.json) for the metrics that will be computed for each unit.

        samplerate : float
            The sampling rate in Hz
        freq_min : float
            The lower endpoint of the frequency band (Hz)
        freq_max : float
            The upper endpoint of the frequency band (Hz)
        adjacency_radius : float
            Radius of local sorting neighborhood, corresponding to the geometry file (same units). 0 means each channel is sorted independently. -1 means all channels are included in every neighborhood.
        detect_sign : int
            Use 1, -1, or 0 to detect positive peaks, negative peaks, or both, respectively
        detect_threshold : float
            Threshold for event detection, corresponding to the input file. So if the input file is normalized to have noise standard deviation 1 (e.g., whitened), then this is in units of std. deviations away from the mean.
        detect_interval : int
            The minimum number of timepoints between adjacent spikes detected in the same channel neighborhood.
        clip_size : int
            Size of extracted clips or snippets, used throughout
        firing_rate_thresh : float64
            (Optional) firing rate must be above this
        isolation_thresh : float64
            (Optional) isolation must be above this
        noise_overlap_thresh : float64
            (Optional) noise_overlap_thresh must be below this
        peak_snr_thresh : float64
            (Optional) peak snr must be above this
    """

    # if you do not provide an input, it will set the value as an empty string via mountainlab

    if not raw_fname and not pre_fname:
        raise Exception('You must input a raw_fname or a pre_fname!')

    if raw_fname and pre_fname:
        raise Exception('You defined both the raw_fname and the pre_fname, can only use one!')

    params = {'freq_min': freq_min,
              'freq_max': freq_max,
              'samplerate': samplerate,
              'detect_sign': detect_sign,
              'adjacency_radius': adjacency_radius,
              'detect_threshold': detect_threshold,
              'detect_interval': detect_interval,
              'clip_size': clip_size,
              'firing_rate_thresh':  firing_rate_thresh,
              'isolation_thresh': isolation_thresh,
              'noise_overlap_thresh': noise_overlap_thresh,
              'peak_snr_thresh': peak_snr_thresh,
    }

    if params_fname:
        if os.path.exists(params_fname):
            ds_params = read_dataset_params(params_fname)

        # override the default parameters
        for key, value in ds_params.items():
            params[key] = value
    else:
        pass

    if raw_fname:
        # no pre-processing has done, so perform the pre-processing
        if not os.path.exists(raw_fname):
            raise Exception('Raw fname does not exist!')

        output_dir = os.path.dirname(raw_fname)

        if not pre_out_fname:
            pre_out_fname = output_dir + '/pre.mda.prv'

        # Bandpass filter
        band_pass_out = output_dir + '/filt.mda.prv'
        bandpass_filter(
            timeseries=raw_fname,
            timeseries_out=band_pass_out,
            samplerate=params['samplerate'],
            freq_min=params['freq_min'],
            freq_max=params['freq_max'],
            opts=opts
        )

        # Whiten
        whiten(
            timeseries=output_dir + '/filt.mda.prv',
            timeseries_out=pre_out_fname,
            opts=opts
        )

        sort_fname = pre_out_fname

        os.remove(band_pass_out)

    else:
        # pre_fname has to be not None by this point, skip pre-processing since this is the input
        output_dir = os.path.dirname(pre_fname)
        sort_fname = pre_fname

    # Sort

    if not firings_out:
        firings_out = output_dir + '/firings.mda'

    ms4alg_sort(
        timeseries=sort_fname,
        geom=geom_fname,
        firings_out=firings_out,
        adjacency_radius=params['adjacency_radius'],
        detect_sign=params['detect_sign'],
        detect_threshold=params['detect_threshold'],
        clip_size=params['clip_size'],
        opts=opts
    )

    temp_metrics = output_dir + '/temp_metrics.json'

    if not metrics_out_fname:
        metrics_out_fname = output_dir + '/cluster_metrics.json'

    # Compute cluster metrics
    compute_cluster_metrics(
        timeseries=sort_fname,
        firings=firings_out,
        metrics_out=temp_metrics,
        samplerate=params['samplerate'],
        opts=opts
    )

    # Automated curation
    add_curation_tags(cluster_metrics=temp_metrics,
                      output_filename=metrics_out_fname,
                      firing_rate_thresh=0.05,
                      isolation_thresh=0.95,
                      noise_overlap_thresh=0.03,
                      peak_snr_thresh=1.5,
                      opts=opts)

    os.remove(temp_metrics)


def read_dataset_params(params_fname):
    params_fname = mlp.realizeFile(params_fname)
    if not os.path.exists(params_fname):
        raise Exception('Dataset parameter file does not exist: ' + params_fname)
    with open(params_fname) as f:
        return json.load(f)


def bandpass_filter(*, timeseries, timeseries_out, samplerate, freq_min, freq_max, opts={}):
    return mlp.runProcess(
        'ephys.bandpass_filter',
        {
            'timeseries': timeseries
        }, {
            'timeseries_out': timeseries_out
        },
        {
            'samplerate': samplerate,
            'freq_min': freq_min,
            'freq_max': freq_max
        },
        opts
    )


def whiten(*, timeseries, timeseries_out, opts={}):
    return mlp.runProcess(
        'ephys.whiten',
        {
            'timeseries': timeseries
        },
        {
            'timeseries_out': timeseries_out
        },
        {},
        opts
    )


def ms4alg_sort(*, timeseries, geom, firings_out, detect_sign, adjacency_radius, detect_threshold, clip_size, opts={}):
    pp = {}
    pp['detect_sign'] = detect_sign
    pp['adjacency_radius'] = adjacency_radius
    pp['detect_threshold'] = detect_threshold
    pp['clip_size'] = clip_size

    inputs = {'timeseries': timeseries}
    if geom:
        inputs['geom'] = geom

    mlp.runProcess(
        'ms4alg.sort',
        inputs,
        {
            'firings_out': firings_out
        },
        pp,
        opts
    )


def compute_cluster_metrics(*, timeseries, firings, metrics_out, samplerate, opts={}):
    metrics1 = mlp.runProcess(
        'ms3.cluster_metrics',
        {
            'timeseries': timeseries,
            'firings': firings
        },
        {
            'cluster_metrics_out': True
        },
        {
            'samplerate': samplerate
        },
        opts
    )['cluster_metrics_out']
    metrics2 = mlp.runProcess(
        'ms3.isolation_metrics',
        {
            'timeseries': timeseries,
            'firings': firings
        },
        {
            'metrics_out': True
        },
        {
            'compute_bursting_parents': 'true'
        },
        opts
    )['metrics_out']
    return mlp.runProcess(
        'ms3.combine_cluster_metrics',
        {
            'metrics_list': [metrics1, metrics2]
        },
        {
            'metrics_out': metrics_out
        },
        {},
        opts
    )


def add_curation_tags(*, cluster_metrics, output_filename, firing_rate_thresh=0.05,
                      isolation_thresh=0.95, noise_overlap_thresh=0.03, peak_snr_thresh=1.5, opts={}):
    # Automated curation
    mlp.runProcess(
        'pyms.add_curation_tags',
        {
            'metrics': cluster_metrics
        },
        {
            'metrics_tagged': output_filename
        },
        {
            'firing_rate_thresh': firing_rate_thresh,
            'isolation_thresh': isolation_thresh,
            'noise_overlap_thresh': noise_overlap_thresh,
            'peak_snr_thresh': peak_snr_thresh
        },
        opts
    )


sort_dataset.name = processor_name
sort_dataset.version = processor_version
sort_dataset.author = 'Geoffrey Barrett'