#!/usr/bin/env python
from optparse import OptionParser
from argparse import ArgumentParser
from xml.dom.minidom import parse, parseString
import xml.sax

import os
import sys
import shutil
import timeit
import StringIO
import subprocess
#import multiprocessing
from Manager import *

if __name__ == "__main__":
    parser = OptionParser(usage="usage: %prog [options] filename",
                          version="%prog 0.2")
    parser.add_option("-w", "--workdir",
                      action="store",
                      dest="workdir",
                      default="",
                      help="Overwrite the place where to store overhead.")
    parser.add_option("-s", "--submit",
                      action="store_true", # optional because action defaults to "store"
                      dest="submit",
                      default=False,
                      help="Submit Jobs to the grid")
    parser.add_option("-r", "--resubmit",
                      action="store_true", # optional because action defaults to "store"
                      dest="resubmit",
                      default=False,
                      help="Resubmit Jobs were no files are found in the OutputDir/workdir .")
    parser.add_option("-l", "--loopCheck",
                      action="store_true", # optional because action defaults to "store"
                      dest="loop",
                      default=False,
                      help="Look which jobs finished and where transfered to your storage device.")
    parser.add_option("-a", "--addFiles",
                      action="store_true",
                      dest="add",
                      default=False,
                      help="hadd files to one") 
    parser.add_option("-T", "--addFilesNoTree",
                      action="store_true",
                      dest="addNoTree",
                      default=False,
                      help="hadd files to one, without merging TTrees. Can be combined with -f.") 
    parser.add_option("-f", "--forceMerge",
                      action="store_true", # optional because action defaults to "store"
                      dest="forceMerge",
                      default=False,
                      help="Force to hadd the root files from the workdir into the ouput directory.")
    parser.add_option("-c", "--continueMerge",
                      action="store_true",
                      dest="waitMerge",
                      default=False,
                      help="Wait for all merging subprocess to finish before exiting program. All the subprocesses that finish in the meantime become zombies until the main program finishes.")
    parser.add_option("-k", "--keepGoing",
                      action="store_true",
                      dest="keepGoing",
                      default=False,
                      help="Never ask for user input, but keep going on.")
    parser.add_option("-x", "--exitOnQuestion",
                      action="store_true",
                      dest="exitOnQuestion",
                      default=False,
                      help="Never ask for user input, but exit instead. (Overwrites keepGoing)")
    
    (options, args) = parser.parse_args()
    

    start = timeit.default_timer()

    if len(args) != 1:
        parser.error("wrong number of arguments help can be invoked with --help")

    xmlfile = args[0]
    if os.path.islink(xmlfile):
        xmlfile = os.path.abspath(os.readlink(xmlfile))
    # softlink JobConfig.dtd into current directory
    scriptpath = os.path.realpath(__file__)[:-15]
    if not os.path.exists('JobConfig.dtd'):
        os.system('ln -sf %s/JobConfig.dtd .' % scriptpath)

    #print xmlfile, os.getcwd
    proc_xmllint = subprocess.Popen(['xmllint','--noent',xmlfile],stdout=subprocess.PIPE)
    xmlfile_strio = StringIO.StringIO(proc_xmllint.communicate()[0])
    sax_parser = xml.sax.make_parser()
    xmlparsed = parse(xmlfile_strio,sax_parser)
    header = header(xmlfile)
    node = xmlparsed.getElementsByTagName('JobConfiguration')[0]
    Job = JobConfig(node)

    workdir = header.Workdir
    if options.workdir : workdir = options.workdir
    if not workdir : workdir="workdir"
    currentDir = os.getcwd()
    if not os.path.exists(workdir+'/'):
        os.makedirs(workdir+'/')
        print workdir,'has been created'
        shutil.copy(scriptpath+"JobConfig.dtd",workdir)
        shutil.copy(args[0],workdir)
    #print header.Version[0]

    for cycle in Job.Job_Cylce:
        if cycle.OutputDirectory.startswith('./'):             
            cycle.OutputDirectory = currentDir+cycle.OutputDirectory[1:]
        print 'filling manager'
        manager = JobManager(options,header,workdir)
        manager.process_jobs(cycle.Cycle_InputData,Job)
        nameOfCycle = cycle.Cyclename.replace('::','.')
        if options.submit: manager.submit_jobs(cycle.OutputDirectory,nameOfCycle)
        manager.check_jobstatus(cycle.OutputDirectory, nameOfCycle,False,False)
        if options.resubmit: manager.resubmit_jobs()
        #get once into the loop for resubmission
        loop_check = True #options.loop
        while loop_check==True:   
            if not options.loop:loop_check = False
            manager.merge_files(cycle.OutputDirectory,nameOfCycle,cycle.Cycle_InputData)
            manager.check_jobstatus(cycle.OutputDirectory,nameOfCycle)
            if manager.get_subInfoFinish() or (not manager.merge.get_mergerStatus() and manager.missingFiles==0):
                print 'if grid pid information got lost root Files could still be transferring'
                break
            if options.loop: 
                manager.print_status()
                print '='*80
                time.sleep(5)
        #print 'Total progress', tot_prog
        manager.merge_wait()
        manager.check_jobstatus(cycle.OutputDirectory,nameOfCycle,False,False)
        print '-'*80
        manager.print_status()
    stop = timeit.default_timer()
    print "SFrame Batch was running for",round(stop - start,2),"sec"
    #exit gracefully
    if all(si.status == 1 for si in manager.subInfo):
        exit(0)  
    else:
        exit(-1)
