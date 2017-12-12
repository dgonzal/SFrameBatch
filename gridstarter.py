#!/usr/bin/env python

import sys
import subprocess

gridconfig_base = """

[global]
backend = condor
task = UserTask          

[jobs]
memory = 8000 ; this is not tuned
jobs = -1                 ; Submit script two times
wall time = 24:00              
in flight = -1  
max retry = 5
in queue = 1500
monitor job = dashboard

[UserTask]
executable = sframe_main ; Name of the script
;arguments = @XML_NR@.xml ; Arguments for the executable
files per job = 1
dataset provider = scan                  ; change default dataset provider to "scan"
dataset =  %s/*.xml      ; list of directories to scan for files
se path = %s
se output files = *.root
se output pattern = JOB_OUTPUT_@GC_JOBID@_@X@
"""
# ;[parameters]
# ;parameters   = XML_NR
# ;XML_NR =
# """


class gridstart(object):
    def __init__(self, workdir, outdir):  # , xml_base, n_xml):
        gridconfig = gridconfig_base % (workdir, outdir)
        with open("temp.conf", 'w') as file_:
            file_.write(gridconfig)
        sys.exit(42)
        subprocess.Popen("Go.py temp.conf", shell=True)
