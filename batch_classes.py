#!/usr/bin/env python

from subprocess import call
from subprocess import Popen
from subprocess import PIPE
import os

from tree_checker import *
#from fhadd import fhadd


def write_script(name,workdir,header):
    myfile = open(workdir+'/split_script_'+name+'.sh','w')
    
    myfile.write(
    """#!/bin/bash

##This is a simple example of a SGE batch script
##Use home server with scientific linux 6 
#$ -l os=sld6 
#$ -l site=hh 
#$ -cwd
##You need to set up sframe
#$ -V 
##email Notification
#$ -m """+header.Notification+"""
#$ -M """+header.Mail+"""
##running in local mode with 8-12 cpu slots
##$ -pe local 8-12
##CPU memory
#$ -l h_vmem="""+header.RAM+"""G
##DISK memory
#$ -l h_fsize="""+header.DISK+"""G   
cd """+workdir+"""
sframe_main """+name+"""_${SGE_TASK_ID}.xml
""")
    
    myfile.close()


def resub_script(name,workdir,header):
    myfile = open(workdir+'/split_script_'+name+'.sh','w')
    
    myfile.write(
    """#!/bin/bash

##This is a simple example of a SGE batch script
##Use home server with scientific linux 6 
#$ -l os=sld6 
#$ -l site=hh 
#$ -cwd
##You need to set up sframe
#$ -V 
##email Notification
#$ -m """+header.Notification+"""
#$ -M """+header.Mail+"""
##running in local mode with 8-12 cpu slots
##$ -pe local 8-12
##CPU memory
#$ -l h_vmem="""+header.RAM+"""G
##DISK memory
#$ -l h_fsize="""+header.DISK+"""G   
cd """+workdir+"""
sframe_main """+name+""".xml

""")    
    myfile.close()

def submit_qsub(NFiles,Stream,name,workdir):
    #print '-t 1-'+str(int(NFiles))
    #call(['ls','-l'], shell=True)

    if not os.path.exists(Stream):
        os.makedirs(Stream)
        print Stream+' has been created'
 
    #call(['qsub'+' -t 1-'+str(NFiles)+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'], shell=True)
    proc_qstat = Popen(['qsub'+' -t 1-'+str(NFiles)+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'],shell=True,stdout=PIPE)
    return (proc_qstat.communicate()[0].split()[2]).split('.')[0]


def resubmit(Stream,name,workdir,header):
    #print Stream ,name
    resub_script(name,workdir,header)	
    if not os.path.exists(Stream):
        os.makedirs(Stream)
        print Stream+' has been created'
    #call(['qsub'+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'], shell=True)
    proc_qstat = Popen(['qsub'+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'],shell=True,stdout=PIPE)
    return proc_qstat.communicate()[0].split()[2]

def add_histos(directory,name,NFiles,workdir,outputTree, onlyhists) :
    if os.path.exists(directory+name+'.root'):
        call(['rm '+directory+name+'.root'], shell=True)
    string=''
    proc = None
    position = -1
    command_string = 'nice -n 10 hadd -v 1 ' 
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

    if not string.isspace():
        proc = Popen([command_string+directory+name+'.root '+source_files], shell=True,stdout=PIPE)
    else:
        print 'Nothing to merge for',name+'.root'
    return proc 


