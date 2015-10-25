#!/usr/bin/env python

from subprocess import call
import os


def write_script(name,workdir):
    myfile = open(workdir+'/split_script_'+name+'.sh','w')
    
    myfile.write(
    """#!/bin/bash
#
##This is a simple example of a SGE batch script
##Use home server with scientific linux 6 
#$ -l os=sld6 
#$ -l site=hh 
#$ -cwd
##You need to set up sframe
#$ -V 
##email Notification
#$ -m sa
#$ -M daniel.gonzalez@desy.de
##running in local mode with 8-12 cpu slots
##$ -pe local 8-12
##CPU memory
##$ -l h_vmem=16G
##DISK memory
#$ -l h_fsize=2G   
sframe_main """+workdir+"""/"""+name+"""_${SGE_TASK_ID}.xml
""")
    
    myfile.close()


def resub_script(name,workdir):
    myfile = open(workdir+'/split_script_'+name+'.sh','w')
    
    myfile.write(
    """#!/bin/zsh
#
#$ -M daniel.gonzalez@desy.de
##This is a simple example of a SGE batch script
##Use home server with scientific linux 6 
#$ -l os=sld6 
#$ -l site=hh 
#$ -cwd
##You need to set up sframe
#$ -V 
##email Notification
#$ -m as
#$ -M daniel.gonzalez@desy.de
##running in local mode with 8-12 cpu slots
##$ -pe local 8-12
##CPU memory
#$ -l h_vmem=4G
##DISK memory
#$ -l h_fsize=2G   
sframe_main """+workdir+"""/"""+name+""".xml

""")    
    myfile.close()



def submitt_qsub(NFiles,Stream,name,workdir):
    #print '-t 1-'+str(int(NFiles))
    #call(['ls','-l'], shell=True)

    if not os.path.exists(Stream):
        os.makedirs(Stream)
        print Stream+' has been created'
 
    print call(['qsub'+' -t 1-'+str(NFiles)+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'], shell=True)

def resubmit(Stream,name,workdir):
    #print Stream ,name
    resub_script(name,workdir)	
    if not os.path.exists(Stream):
        os.makedirs(Stream)
        print Stream+' has been created'
 
    print call(['qsub'+' -o '+Stream+'/'+' -e '+Stream+'/'+' '+workdir+'/split_script_'+name+'.sh'], shell=True)


def add_histos(directory, name, NFiles,workdir) :

    print call(['rm '+directory+name+'.root'], shell=True)
    string =" "

    for i in range(NFiles):
        string += directory+workdir+'/'+name+'_'+str(i)+'.root'
        string += " "

    #print string
    call(['hadd '+directory+name+'.root'+string], shell=True)

    
