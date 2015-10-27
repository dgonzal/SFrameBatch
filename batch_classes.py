#!/usr/bin/env python

from subprocess import call
import os


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
 
    print call(['qsub'+' -t 1-'+str(NFiles)+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'], shell=True)

def resubmit(Stream,name,workdir,header):
    #print Stream ,name
    resub_script(name,workdir,header)	
    if not os.path.exists(Stream):
        os.makedirs(Stream)
        print Stream+' has been created'
 
    print call(['qsub'+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'], shell=True)


def add_histos(directory, name, NFiles,workdir) :
    print 'Merging',name
    call(['rm '+directory+name+'.root'], shell=True)
    string =" "

    for i in range(NFiles):
        string += directory+workdir+'/'+name+'_'+str(i)+'.root'
        string += " "

    #print string
    call(['hadd '+'-v 1 '+directory+name+'.root'+string], shell=True)

    
