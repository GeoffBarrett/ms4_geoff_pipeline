from mountainlab_pytools import mlproc as mlp
import os


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


def _mask_artifacts(*, timeseries, timeseries_out, threshold=6, chunk_size=2000, num_write_chunks=150, opts={}):
    return mlp.runProcess(
        'ephys.mask_out_artifacts',
        {
            'timeseries': timeseries
        },
        {
            'timeseries_out': timeseries_out
        },
        {
            'threshold': threshold,
            'chunk_size': chunk_size,
            'num_write_chunks': num_write_chunks,
        },
        opts
    )


def _whiten(*, timeseries, timeseries_out, opts={}):
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


def ms4alg_sort(*, timeseries, geom, firings_out, detect_sign, adjacency_radius, detect_threshold, detect_interval,
                clip_size, num_features=10, max_num_clips_for_pca=1000, num_workers=os.cpu_count(), opts={}):
    pp = {}
    pp['detect_sign'] = detect_sign
    pp['adjacency_radius'] = adjacency_radius
    pp['detect_threshold'] = detect_threshold
    pp['clip_size'] = clip_size
    pp['detect_interval'] = detect_interval
    pp['num_workers'] = num_workers
    pp['num_features'] = num_features
    pp['max_num_clips_for_pca'] = max_num_clips_for_pca

    inputs = {'timeseries': timeseries}
    if geom is not None:
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