#!/usr/bin/env python3

import sys

from mountainlab_pytools import processormanager as pm

import p_ms4_geoff

PM=pm.ProcessorManager()

PM.registerProcessor(p_ms4_geoff.sort_dataset)

if not PM.run(sys.argv):
    exit(-1)