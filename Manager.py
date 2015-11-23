#!/usr/bin/env python
# -*- coding: utf-8 -*-

from io_func import *
from batch_classes import *
from Inf_Classes import *

import os
import subprocess
import datetime
import json
#import cPickle

import StringIO
from xml.dom.minidom import parse, parseString
import xml.sax


class pidWatcher(object):
    def __init__(self):
        self.pidList = []
        self.taskList = []
        self.stateList = []
        proc_qstat = subprocess.Popen(['qstat','-xml'],stdout=subprocess.PIPE)
        qstat_xml =  StringIO.StringIO(proc_qstat.communicate()[0])
        sax_parser = xml.sax.make_parser()
        qstat_xml_par = parse(qstat_xml,sax_parser) 
        #print qstat_xml_par.toprettyxml()
        tags = qstat_xml_par.getElementsByTagName("job_list")
        for jobs in tags:
            #print jobs.getElementsByTagName("state")[0].firstChild.nodeValue
            self.pidList.append(jobs.getElementsByTagName("JB_job_number")[0].firstChild.data)
            self.stateList.append(jobs.getElementsByTagName("state")[0].firstChild.nodeValue)
            self.taskList.append(jobs.getElementsByTagName("tasks")[0].firstChild.data)

    def check_pidstatus(self,pid,task):
        for i in range(len(self.pidList)):
            #print pid, self.stateList[i],type(self.stateList[i]),type('r') , str(self.stateList[i]),str(self.stateList[i].encode('ascii','ignore')) 
            if str(self.pidList[i]) == pid and str(self.taskList[i] == task):
                if str(self.stateList[i]) == 'r' or str(self.stateList[i]) == 'qw':
                    return 1
                else: 
                    return 2
        return 0

class HelpJSON:
    def __init__(self,json_file):
        print 'Using saved settings from:', json_file
        self.data = None
        #return
        if os.path.isfile(json_file):
            self.data = json.load(open(json_file,'r'))
            #self.data = json.load(self.data)

    def check(self,datasetname):
        for element in self.data:
            jdict = json.loads(element)
            if datasetname == jdict['name']:
                mysub = SubInfo()
                mysub.load_JSON(mtest)
                return mysub
        return None


class SubInfo(object):
    def __init__(self,name='',numberOfFiles=0,data_type='',resubmit =0):
        self.name = name
        self.numberOfFiles =numberOfFiles #number of expected files
        self.data_type = data_type
        self.rootFileCounter=0 #number of expected files 
        self.status = 0
	self.missingFiles = []
        self.pids = ['']*numberOfFiles
        self.jobnum = [False]*numberOfFiles
        self.arrayPid = None
        self.resubmit = resubmit
    def to_JSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)
    def load_JSON(self,data):
        self.__dict__ = data
    
class JobManager(object):
    def __init__(self,options,header,workdir):
        self.header = header #how do I split stuff, sframe_batch header in xml file
        self.workdir = workdir #name of the workdir
        self.merge  = MergeManager(options.add,options.forceMerge,options.waitMerge)
        self.subInfo = [] #information about the submission status
        self.deadJobs = 0 #check if no file has been written to disk and nothing is on running on the batch
        self.totalFiles =0
    #read xml file and do the magic 
    def process_jobs(self,InputData,Job):
        jsonhelper = HelpJSON(self.workdir+'/SubmissinInfoSave.p')
        for process in range(len(InputData)):
            found = None
            processName = ([InputData[process].Version])
            if jsonhelper.data:
                helpSubInfo = SubInfo()
                found = jsonhelper.check(processName)
                if found:
                    self.subInfo.append(found)
            if not found: 
                self.subInfo.append(SubInfo(InputData[process].Version,write_all_xml(self.workdir+'/'+InputData[process].Version,processName,self.header,Job,self.workdir),InputData[process].Type))
            self.totalFiles += self.subInfo[process].numberOfFiles
            write_script(processName[0],self.workdir,self.header)
    #submit the jobs to the batch as array job
    #the used function should soon return the pid of the job for killing and knowing if something failed
    def submit_jobs(self):
        for process in self.subInfo:
            process.arrayPid = submit_qsub(process.numberOfFiles,self.workdir+'/Stream_'+str(process.name),str(process.name),self.workdir)
            print 'Submitted jobs',process.name, 'pid', process.arrayPid
    #resubmit the jobs see above      
    def resubmit_jobs(self):
        proc_qstat = subprocess.Popen(['qstat'],stdout=subprocess.PIPE)
        qstat_out = proc_qstat.communicate()[0]
        if qstat_out:
            print '\n' + qstat_out
            res = raw_input('Some jobs are still running (see above). Do you really want to resubmit? Y/[N] ')
            if res.lower() != 'y':
                exit(0)
        for process in self.subInfo:
	    for it in process.missingFiles:
                process.pids.append(resubmit(self.workdir+'/Stream_'+process.name,process.name+'_'+str(it),self.workdir,self.header))
                process.jobnum[it] = True
                print 'Resubmitted job',process.name, 'pid', process.pids

    #see how many jobs finished, were copied to workdir or were merged 
    def check_jobstatus(self, OutputDirectory, nameOfCycle,remove = True):
        watch = pidWatcher()
        missing = open(self.workdir+'/missing_files.txt','w+')
        #jsonFile = open(self.workdir+'/SubmissinInfoSave.p','wb+')
        #jsonFile.write('[')
        ListOfDict =[]
        for i in xrange(len(self.subInfo)-1, -1, -1):
            process = self.subInfo[i]
            ListOfDict.append(process.to_JSON())
            #jsonFile.write(process.to_JSON())
            rootFiles =0
            #self.subInfo
            status = -1
	    self.subInfo[i].missingFiles = []     
            for it in range(process.numberOfFiles):
                if process.jobnum[it]: 
                    status = watch.check_pidstatus(process.pids[it],it+1)
                else:
                    status = watch.check_pidstatus(process.arrayPid,it+1)

                #if status == 2 and (self.resubmit ==-1 or self.resubmit>0):
                # kill jobs with pid    
                # resubmit and lower resubmit if it not -1
                filename = OutputDirectory+'/'+self.workdir+'/'+nameOfCycle+'.'+process.data_type+'.'+process.name+'_'+str(it)+'.root'
                if not os.path.exists(filename) or status==1:
                    missing.write(self.workdir+'/'+nameOfCycle+'.'+process.data_type+'.'+process.name+'_'+str(it)+'.root\n')
		    self.subInfo[i].missingFiles.append(it+1)		    
                else:
                    #print datetime.datetime.fromtimestamp(os.stat(filename).st_mtime),datetime.datetime.fromtimestamp(os.stat(filename).st_ctime),datetime.datetime.fromtimestamp(os.stat(filename).st_atime)
                    rootFiles+=1
            process.rootFileCounter=rootFiles
            if remove:
                if os.path.exists(OutputDirectory+'/'+nameOfCycle+'.'+process.data_type+'.'+process.name+'.root'):
                    del self.subInfo[i]
        missing.close()
        #jsonFile.write(']')
        jsonFile = open(self.workdir+'/SubmissinInfoSave.p','wb+')
        #jsonFile.write(ListOfDict)
        json.dump(ListOfDict, jsonFile)
        jsonFile.close()
                
    #print status of jobs 
    def print_status(self):
        print 'Status of unmerged files'
        print '%30s: %6s %6s %.6s'% ('Sample Name','Ready','#Files','[%]')
        for process in self.subInfo:
            print '%30s: %6i %6i %.3i'% (process.name, process.rootFileCounter,process.numberOfFiles, 100*process.rootFileCounter/float(process.numberOfFiles)), 'Done' if process.rootFileCounter == process.numberOfFiles else ''
        print 'Number of files expected: ',self.totalFiles
    #take care of merging
    def merge_files(self,OutputDirectory,nameOfCycle,InputData):
        self.merge.merge(OutputDirectory,nameOfCycle,self.subInfo,self.workdir,InputData)
    #wait for every process to finish
    def merge_wait(self):
        self.merge.wait_till_finished()
                               
class MergeManager(object):
    def __init__(self,add,force,wait):
        self.add = add
        self.force = force
        self.active_process=[]
        self.wait = wait
        
    def merge(self,OutputDirectory,nameOfCycle,info,workdir,InputData):
        if not self.add and not self.force: return  
        print "Don't worry your are using nice = 10 "
        OutputTreeName = ""
        for inputObj in InputData:
            for mylist in inputObj.io_list.other:
                if "OutputTree" in mylist:
                    OutputTreeName= mylist[2]
        for process in info:
            if not process.numberOfFiles == process.rootFileCounter:
                continue
            if not os.path.exists(OutputDirectory+'/'+nameOfCycle+'.'+process.data_type+'.'+process.name+'.root') or self.force:
                if process.status ==0:
                    self.active_process.append(add_histos(OutputDirectory,nameOfCycle+'.'+process.data_type+'.'+process.name,process.numberOfFiles,workdir,OutputTreeName))
                    process.status = 1
    
    def wait_till_finished(self):
        if not self.wait: return
        for process in self.active_process:
            if not process: continue
            if not process.poll():
                process.wait()
                #os.kill(process.pid,-9)
