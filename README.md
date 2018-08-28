# ms4_geoff_pipeline
This is a full custom MountainSort pipeline that I use to analyze data using the ms4 algorithm.

## Installation

This pipeline also depends on the ms_taggedcuration pipeline located [here](https://bitbucket.org/franklab/franklab_mstaggedcuration) to the $CONDA_PREFIX/etc/mountainlab/packages directory.

Clone this repository into $CONDA_PREFIX/etc/mountainlab/packages directory

Example: 
```
cd ~/conda/envs/mlab/etc/mountainlab/packages
git clone https://github.com/GeoffBarrett/ms4_geoff_pipeline.git
```

Check that the have been added to the processor list

```
ml-list-processors | grep ms4_geoff
```

If you do not see **ms4_geoff.sort** then it is possible that the .mp files do not have permissions. Execute the following.

```
cd ~/conda/envs/mlab/etc/mountainlab/packages/ms4_geoff_pipeline/ms4_geoff_pipeline
chmod a+x ms4_geoff_spec.py.mp
```

