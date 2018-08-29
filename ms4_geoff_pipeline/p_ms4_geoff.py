from mountainlab_pytools import mlproc as mlp
import ms4_geoff
import os
import json

processor_name = 'ms4_geoff.sort'
processor_version = '0.1.0'


def sort_dataset(*,
                 raw_fname=None, pre_fname=None, geom_fname=None, params_fname=None,
                 firings_out, pre_out_fname, metrics_out_fname,
                 freq_min=300, freq_max=7000, samplerate=30000, detect_sign=1,
                 adjacency_radius=-1, detect_threshold=3, detect_interval=50, clip_size=50,
                 firing_rate_thresh=0.05, isolation_thresh=0.95, noise_overlap_thresh=0.03,
                 peak_snr_thresh=1.5):
    """
    Custom Sorting Pipeline. It will pre-process, sort, and curate (using ms_taggedcuration pipeline).

    Parameters
    ----------
    raw_fname : INPUT
        MxN raw timeseries array (M = #channels, N = #timepoints). If you input this it will pre-process the data.
    pre_fname : INPUT
        MxN pre-processed array timeseries array (M = #channels, N = #timepoints). This is if you want to analyze already pre-processed data.
    geom_fname : INPUT
        (Optional) geometry file (.csv format).
    params_fname : INPUT
        (Optional) parameter file (.json format), where the key is the any of the parameters for this pipeline. Any values in this .json file will overwrite any defaults.

    firings_out : OUTPUT
        The filename that will contain the spike data (.mda file), default to '/firings.mda'
    pre_out_fname : OUTPUT
        Optional filename for the pre-processed data.
    metrics_out_fname : OUTPUT
        The output filename (.json) for the metrics that will be computed for each unit.

    samplerate : float
        (Optional) The sampling rate in Hz
    freq_min : float
        (Optional) The lower endpoint of the frequency band (Hz)
    freq_max : float
        (Optional) The upper endpoint of the frequency band (Hz)
    adjacency_radius : float
        (Optional) Radius of local sorting neighborhood, corresponding to the geometry file (same units). 0 means each channel is sorted independently. -1 means all channels are included in every neighborhood.
    detect_sign : int
        (Optional) Use 1, -1, or 0 to detect positive peaks, negative peaks, or both, respectively
    detect_threshold : float
        (Optional) Threshold for event detection, corresponding to the input file. So if the input file is normalized to have noise standard deviation 1 (e.g., whitened), then this is in units of std. deviations away from the mean.
    detect_interval : int
        (Optional) The minimum number of timepoints between adjacent spikes detected in the same channel neighborhood.
    clip_size : int
        (Optional) Size of extracted clips or snippets, used throughout
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

    # TODO: find a more pythonic way to do this

    if raw_fname == '':
        raw_fname = None

    if pre_out_fname == '':
        pre_out_fname = None

    if metrics_out_fname == '':
        metrics_out_fname = None

    if pre_fname == '':
        pre_fname = None

    if geom_fname == '':
        geom_fname = None

    if params_fname == '':
        params_fname = None

    if firings_out == '':
        firings_out = None
    # END TODO

    if raw_fname is None and pre_fname is None:
        raise Exception('You must input a raw_fname or a pre_fname!')

    if raw_fname is not None and pre_fname is not None:
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

    if params_fname is not None:
        if os.path.exists(params_fname):
            ds_params = read_dataset_params(params_fname)

        # override the default parameters
        for key, value in ds_params.items():
            params[key] = value
    else:
        pass

    if raw_fname is not None:
        # no pre-processing has done, so perform the pre-processing
        if not os.path.exists(raw_fname):
            raise Exception('The following timeseries does not exist: %s!' % raw_fname)

        output_dir = os.path.dirname(raw_fname)

        if pre_out_fname is None:
            pre_out_fname = output_dir + '/pre.mda.prv'

        print('Creating the following pre-process file: %s' % pre_out_fname)

        # Bandpass filter
        band_pass_out = output_dir + '/filt.mda.prv'
        ms4_geoff.bandpass_filter(
            timeseries=raw_fname,
            timeseries_out=band_pass_out,
            samplerate=params['samplerate'],
            freq_min=params['freq_min'],
            freq_max=params['freq_max'],
            # opts=opts
        )

        # Whiten
        ms4_geoff.whiten(
            timeseries=output_dir + '/filt.mda.prv',
            timeseries_out=pre_out_fname,
            # opts=opts
        )

        sort_fname = pre_out_fname

        os.remove(band_pass_out)

    else:

        if not os.path.exists(pre_fname):
            raise Exception('The following timeseries does not exist: %s!' % pre_fname)

        output_dir = os.path.dirname(pre_fname)
        sort_fname = pre_fname

    # Sort

    if firings_out is None:
        firings_out = output_dir + '/firings.mda'

    ms4_geoff. ms4alg_sort(
        timeseries=sort_fname,
        geom=geom_fname,
        firings_out=firings_out,
        adjacency_radius=params['adjacency_radius'],
        detect_sign=params['detect_sign'],
        detect_threshold=params['detect_threshold'],
        clip_size=params['clip_size'],
        # opts=opts
    )

    temp_metrics = output_dir + '/temp_metrics.json'

    if metrics_out_fname is None:
        metrics_out_fname = output_dir + '/cluster_metrics.json'

    # Compute cluster metrics
    ms4_geoff.compute_cluster_metrics(
        timeseries=sort_fname,
        firings=firings_out,
        metrics_out=temp_metrics,
        samplerate=params['samplerate'],
        # opts=opts
    )

    # Automated curation
    ms4_geoff.add_curation_tags(cluster_metrics=temp_metrics,
                      output_filename=metrics_out_fname,
                      firing_rate_thresh=0.05,
                      isolation_thresh=0.95,
                      noise_overlap_thresh=0.03,
                      peak_snr_thresh=1.5,
                      # opts=opts
                    )

    os.remove(temp_metrics)
    return True

def read_dataset_params(params_fname):
    params_fname = mlp.realizeFile(params_fname)
    if not os.path.exists(params_fname):
        raise Exception('Dataset parameter file does not exist: ' + params_fname)
    with open(params_fname) as f:
        return json.load(f)


sort_dataset.name = processor_name
sort_dataset.version = processor_version
sort_dataset.author = 'Geoffrey Barrett'
