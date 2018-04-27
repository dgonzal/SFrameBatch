#!/usr/bin/env python

from subprocess import call
from subprocess import Popen
from subprocess import PIPE
import os

from tree_checker import *
#from fhadd import fhadd


def write_script(name,workdir,header):
    sframe_wrapper=open(workdir+'/sframe_wrapper.sh','w')
    sframe_wrapper.write(
        """#!/bin/bash
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH_STORED
sframe_main $1
        """)
    sframe_wrapper.close()
    os.system('chmod u+x '+workdir+'/sframe_wrapper.sh')    
    if (header.Notification == 'as'):
        condor_notification = 'Error'
    elif (header.Notification == 'n'):
        condor_notification = 'Never'
    elif (header.Notification == 'e'):
        condor_notification = 'Complete'
    else:
        condor_notification = ''
        
    submit_file = open(workdir+'/CondorSubmitfile_'+name+'.submit','w')
    submit_file.write(
        """#HTC Submission File for SFrameBatch
# +MyProject        =  "af-cms" 
requirements      =  OpSysAndVer == "SL6"
universe          = vanilla
# #Running in local mode with 8 cpu slots
# universe          =  local
# request_cpus      =  8 
notification      = """+condor_notification+"""
notify_user       = """+header.Mail+"""
initialdir        = """+workdir+"""
output            = $(Stream)/"""+name+""".o$(ClusterId).$(Process)
error             = $(Stream)/"""+name+""".e$(ClusterId).$(Process)
log               = $(Stream)/"""+name+""".$(Cluster).log
#Requesting CPU and DISK Memory - default +RequestRuntime of 3h stays unaltered
RequestMemory     = """+header.RAM+"""G
RequestDisk       = """+header.DISK+"""G
#You need to set up sframe
getenv            = True
environment       = "LD_LIBRARY_PATH_STORED="""+os.environ.get('LD_LIBRARY_PATH')+""""
executable        = """+workdir+"""/sframe_wrapper.sh
MyIndex           = $(Process) + 1
fileindex         = $INT(MyIndex,%d)
arguments         = """+name+"""_$(fileindex).xml
""")
    submit_file.close()
        
def resub_script(name,workdir,header):    
    if (header.Notification == 'as'):
        condor_notification = 'Error'
    elif (header.Notification == 'n'):
        condor_notification = 'Never'
    elif (header.Notification == 'e'):
        condor_notification = 'Complete'
    else:
        condor_notification = ''
        
    submitfile = open(workdir+'/CondorSubmitfile_'+name+'.submit','w')
    submitfile.write(
"""#HTC Submission File for SFrameBatch
# +MyProject        =  "af-cms" 
requirements      =  OpSysAndVer == "SL6"
universe          = vanilla
# #Running in local mode with 8 cpu slots
# universe          =  local
# request_cpus      =  8 
notification      = """+condor_notification+"""
notify_user       = """+header.Mail+"""
initialdir        = """+workdir+"""
output            = $(Stream)/"""+name+""".o$(ClusterId).$(Process)
error             = $(Stream)/"""+name+""".e$(ClusterId).$(Process)
log               = $(Stream)/"""+name+""".$(Cluster).log
#Requesting CPU and DISK Memory - default +RequestRuntime of 3h stays unaltered
# RequestMemory     = """+header.RAM+"""G
RequestMemory     = 8G
RequestDisk       = """+header.DISK+"""G
#You need to set up sframe
getenv            = True
environment       = "LD_LIBRARY_PATH_STORED="""+os.environ.get('LD_LIBRARY_PATH')+""""
executable        = """+workdir+"""/sframe_wrapper.sh
arguments         = """+name+""".xml
queue
""")
    submitfile.close()

def submit_qsub(NFiles,Stream,name,workdir):
    #print '-t 1-'+str(int(NFiles))
    #call(['ls','-l'], shell=True)

    if not os.path.exists(Stream):
        os.makedirs(Stream)
        print Stream+' has been created'
 
    #call(['qsub'+' -t 1-'+str(NFiles)+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'], shell=True)
    # proc_qstat = Popen(['condor_qsub'+' -t 1-'+str(NFiles)+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'],shell=True,stdout=PIPE)
    # return (proc_qstat.communicate()[0].split()[2]).split('.')[0]
    proc_qstat = Popen(['condor_submit'+' '+workdir+'/CondorSubmitfile_'+name+'.submit'+' -a "Stream='+Stream.split('/')[1]+'" -a "queue '+str(NFiles)+'"'],shell=True,stdout=PIPE)
    return (proc_qstat.communicate()[0].split()[7]).split('.')[0]


def resubmit(Stream,name,workdir,header):
    #print Stream ,name
    resub_script(name,workdir,header)	
    if not os.path.exists(Stream):
        os.makedirs(Stream)
        print Stream+' has been created'
    #call(['qsub'+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'], shell=True)
    # proc_qstat = Popen(['condor_qsub'+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'],shell=True,stdout=PIPE)
    # return proc_qstat.communicate()[0].split()[2]
    proc_qstat = Popen(['condor_submit'+' '+workdir+'/CondorSubmitfile_'+name+'.submit'+' -a "Stream='+Stream.split('/')[1]+'"'],shell=True,stdout=PIPE)
    return (proc_qstat.communicate()[0].split()[7]).split('.')[0]

def add_histos(directory,name,NFiles,workdir,outputTree, onlyhists,outputdir):
    if not os.path.exists(outputdir):
        os.makedirs(outputdir)
    FNULL = open(os.devnull, 'w')
    if os.path.exists(directory+name+'.root'):
        call(['rm '+directory+name+'.root'], shell=True)
    string=''
    proc = None
    position = -1
    command_string = 'nice -n 10 hadd ' # -v 1 ' # the -v stopped working in root 6.06/01 now we get a lot of crap
    if onlyhists: command_string += '-T '
    if(outputTree):
        for i in range(NFiles):
            if check_TreeExists(directory+workdir+'/'+name+'_'+str(i)+'.root',outputTree) and position ==-1:
                position = i
                string+=str(i)
                break

    for i in range(NFiles):
        if not position == i and not position == -1:
            string += ','+str(i)
        elif position ==-1:
            string += str(i)
            position = 0

    source_files = ""
    if NFiles > 1:
        source_files = directory+workdir+'/'+name+'_{'+string+'}.root'
    else:
        source_files = directory+workdir+'/'+name+'_'+string+'.root'

    #print command_string+directory+name+'.root '+source_files
    #print outputdir+'/hadd.log'
    if not string.isspace():
        proc = Popen([str(command_string+directory+name+'.root '+source_files+' > '+outputdir+'/hadd.log')], shell=True, stdout=FNULL, stderr=FNULL)
    else:
        print 'Nothing to merge for',name+'.root'
    return proc 


