#!/usr/bin/env python
# -*- coding: utf-8 -*-

from io_func import *
from batch_classes import *
from Inf_Classes import *

import os
import subprocess
import datetime
import json
import time

import StringIO
from xml.dom.minidom import parse, parseString
import xml.sax

# takes care of looking into qstat 
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
            if jobs.getElementsByTagName("tasks"):
                taskvalue = str(jobs.getElementsByTagName("tasks")[0].firstChild.data)
                self.taskList.append(taskvalue)
            else:
                self.taskList.append(-1)
            
    def check_pidstatus(self,pid,task):
        for i in range(len(self.pidList)):
            inrange = False
            if ':' in str(self.taskList[i]):
                splitted_string = (str(self.taskList[i].split(':')[0])).split('-')
                inrange = int(splitted_string[0]) >= int(task) and  int(splitted_string[1]) <= int(task)
            else:
                inrange = str(self.taskList[i])==str(task)
            #if pid == self.pidList[i]:print pid,self.pidList[i], self.stateList[i],self.taskList[i],task 
            if str(self.pidList[i]) == str(pid) and (inrange or self.taskList[i]==-1):
                if str(self.stateList[i]) == 'r' or str(self.stateList[i]) == 'qw' or str(self.stateList[i]) == 't':
                    return 1  # in the batch
                else: 
                    return 2  # error state
        return 0  # not available

#JSON Format is used to store the submission information
class HelpJSON:
    def __init__(self,json_file):
        self.data = None
        #return
        if os.path.isfile(json_file):
            print 'Using saved settings from:', json_file
            self.data = json.load(open(json_file,'r'))
            #self.data = json.load(self.data)

    def check(self,datasetname):
        for element in self.data:
            jdict = json.loads(element)
            if str(datasetname) == str(jdict['name']) and (str(jdict['arrayPid']) or any(jdict['pids'])):
                print 'Found Submission Info for',jdict['name']
                mysub = SubInfo()
                mysub.load_Dict(jdict)
                return mysub
        return None
# class for the submission information
class SubInfo(object):
    def __init__(self,name='',numberOfFiles=0,data_type='',resubmit =0):
        self.name = name
        self.numberOfFiles =numberOfFiles #number of expected files
        self.data_type = data_type
        self.rootFileCounter = 0 #number of expected files 
        self.status = 0   # 0: init, 1: finished, >1: error
        self.missingFiles = []
        self.pids = ['']*numberOfFiles
        self.reachedBatch = [False]*numberOfFiles
        self.jobsDone = [False]*numberOfFiles
        self.arrayPid = -1
        self.resubmit = [resubmit]*numberOfFiles
        self.startingTime = 0
    def reset_resubmit(self,value):
        self.resubmit =[value]*self.numberOfFiles
    def to_JSON(self):
        #print json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=4)
    def load_Dict(self,data):
        self.__dict__ = data
    
class JobManager(object):
    def __init__(self,options,header,workdir):
        self.header = header #how do I split stuff, sframe_batch header in xml file
        self.workdir = workdir #name of the workdir
        self.merge  = MergeManager(options.add,options.forceMerge,options.waitMerge,options.addNoTree)
        self.subInfo = [] #information about the submission status
        self.deadJobs = 0 #check if no file has been written to disk and nothing is on running on the batch
        self.totalFiles = 0  
        self.missingFiles = -1
        self.move_cursor_up_cmd = None # pretty print status
        self.numOfResubmit =0
        self.watch = None
        self.keepGoing = options.keepGoing
        self.exitOnQuestion = options.exitOnQuestion
    #read xml file and do the magic 
    def process_jobs(self,InputData,Job):
        jsonhelper = HelpJSON(self.workdir+'/SubmissinInfoSave.p')
        for process in range(len(InputData)):
            found = None
            processName = ([InputData[process].Version])
            if jsonhelper.data:
                helpSubInfo = SubInfo()
                found = jsonhelper.check(InputData[process].Version)
                if found:
                    self.subInfo.append(found)
            if not found: 
                self.subInfo.append(SubInfo(InputData[process].Version,write_all_xml(self.workdir+'/'+InputData[process].Version,processName,self.header,Job,self.workdir),InputData[process].Type))
            if self.subInfo[-1].numberOfFiles == 0:
                print 'Removing',self.subInfo[-1].name
                self.subInfo.pop()
            else:
                self.totalFiles += self.subInfo[-1].numberOfFiles
                self.subInfo[-1].reset_resubmit(self.header.AutoResubmit) #Reset the retries every time you start
                write_script(processName[0],self.workdir,self.header)
    #submit the jobs to the batch as array job
    #the used function should soon return the pid of the job for killing and knowing if something failed
    def submit_jobs(self,OutputDirectory,nameOfCycle):
        for process in self.subInfo:
            process.startingTime = time.time()
            process.arrayPid = submit_qsub(process.numberOfFiles,self.workdir+'/Stream_'+str(process.name),str(process.name),self.workdir)
            print 'Submitted jobs',process.name, 'pid', process.arrayPid
            if process.status != 0:
                process.status = 0
                process.pids = ['']*process.numberOfFiles
    #resubmit the jobs see above      
    def resubmit_jobs(self):
        proc_qstat = subprocess.Popen(['qstat'],stdout=subprocess.PIPE)
        qstat_out = proc_qstat.communicate()[0]
        ask = True
        for process in self.subInfo:
	    for it in process.missingFiles:
                batchstatus = -1
                if process.pids[it-1]: 
                    batchstatus = self.watch.check_pidstatus(process.pids[it-1],it)
                else:
                    batchstatus = self.watch.check_pidstatus(process.arrayPid,it)
                if qstat_out and batchstatus==-1 and ask:
                    print '\n' + qstat_out
                    if self.exitOnQuestion:
                        exit(-1)
                    elif not self.keepGoing:
                        res = raw_input('Some jobs are still running (see above). Do you really want to resubmit? Y/[N] ')
                        if res.lower() != 'y':
                            exit(-1)
                    ask = False
                if batchstatus != 1:
                    process.pids[it-1] = resubmit(self.workdir+'/Stream_'+process.name,process.name+'_'+str(it),self.workdir,self.header)
                    print 'Resubmitted job',process.name,it, 'pid', process.pids[it-1]
                    if process.status != 0:process.status =0
                    
    #see how many jobs finished, were copied to workdir 
    def check_jobstatus(self, OutputDirectory, nameOfCycle,remove = False, autoresubmit = True):
        missing = open(self.workdir+'/missing_files.txt','w+')
        missingRootFiles = 0 
        ListOfDict =[]
        self.watch = pidWatcher()
        ask = True
        for i in xrange(len(self.subInfo)-1, -1, -1):
            process = self.subInfo[i]
            ListOfDict.append(process.to_JSON())
            if not process.missingFiles and process.status !=0: continue            
            rootFiles =0
            batchstatus = -1
            self.subInfo[i].missingFiles = []
            for it in range(process.numberOfFiles):
                #have a look at the pids with qstat
                if process.pids[it]: 
                    batchstatus = self.watch.check_pidstatus(process.pids[it],it+1)
                else:
                    batchstatus = self.watch.check_pidstatus(process.arrayPid,it+1)
                if batchstatus == 1:
                    process.reachedBatch[it] = True
                #kill jobs with have an error state
                if batchstatus == 2:
                     if process.pids[it]: 
                         subprocess.Popen(['qdel',str(process.pids[it])],stdout=subprocess.PIPE)
                         batchstatus = -1
                     else:
                         subprocess.Popen(['qdel',str(process.arrayPid)+'.'+str(it)],stdout=subprocess.PIPE)
                         batchstatus = -1
                #check if files have arrived 
                filename = OutputDirectory+'/'+self.workdir+'/'+nameOfCycle+'.'+process.data_type+'.'+process.name+'_'+str(it)+'.root'
                if os.path.exists(filename) and process.startingTime < os.path.getctime(filename):
                    process.jobsDone[it] = True
                if batchstatus==1 or not process.jobsDone[it]:
                    missing.write(self.workdir+'/'+nameOfCycle+'.'+process.data_type+'.'+process.name+'_'+str(it)+'.root\n')
                    self.subInfo[i].missingFiles.append(it+1)
                    missingRootFiles +=1
                else:
                    rootFiles+=1
                    batchstatus = -1
                #auto resubmit if job dies, take care that there was some job before and warn the user if more then 20% of jobs die 
                #print process.name,'batch status',batchstatus, 'process.reachedBatch',process.reachedBatch, 'process status',process.status,'resubmit counter',process.resubmit[it], 'resubmit active',autoresubmit
                if (
                    batchstatus==0 and
                    (process.reachedBatch[it] and not process.jobsDone[it]) and
                    (process.status == 0 or process.status == 4) and
                    (process.resubmit[it] ==-1 or process.resubmit[it]>0) and
                    (process.pids[it] or process.arrayPid) and
                    autoresubmit
                ):
                    if float(self.numOfResubmit)/float(self.totalFiles) >.10 and ask:
                        if self.exitOnQuestion:
                            exit(-1)
                        elif not self.keepGoing:
                            res = raw_input('More then 10% of jobs are dead, do you really want to continue? Y/[N] ')
                            if res.lower() != 'y':
                                exit(-1)
                        ask = False
                    process.pids[it] = resubmit(self.workdir+'/Stream_'+process.name,process.name+'_'+str(it+1),self.workdir,self.header)
                    process.reachedBatch[it] = False
                    batchstatus = -1
                    if process.resubmit[it] > 0 : 
                        process.resubmit[it] -= 1
                        self.numOfResubmit +=1
                #If resubmits are used up go into failed
                if batchstatus==0 and process.status == 0 and process.resubmit[it] ==0:
                    process.status = 4

            process.rootFileCounter=rootFiles
            if not process.missingFiles and not process.status > 1:
                process.status = 1
            #remove from list of submission infor this is deprecated and will go soon
            if remove:
                if os.path.exists(OutputDirectory+'/'+nameOfCycle+'.'+process.data_type+'.'+process.name+'.root'):
                    del self.subInfo[i]


        missing.close()
        self.missingFiles = missingRootFiles
        #Save/update pids and other information to json file, such that it can be loaded and used later
        jsonFile = open(self.workdir+'/SubmissinInfoSave.p','wb+')
        json.dump(ListOfDict, jsonFile)
        jsonFile.close()
                
    #print status of jobs 
    def print_status(self):
        if not self.move_cursor_up_cmd:
            self.move_cursor_up_cmd = '\x1b[1A\x1b[2K'*(len(self.subInfo) + 3)
            self.move_cursor_up_cmd += '\x1b[1A' # move once more up since 'print' finishes the line
            print 'Status of files'
        else:
            print self.move_cursor_up_cmd
            time.sleep(.1)  # 'blink'

        print '%30s: %6s %6s %.6s'% ('Sample Name','Ready','#Files','[%]')
        readyFiles =0

        for process in self.subInfo:
            status_message = ['\033[94m Working \033[0m','\033[92m Transferred \033[0m','Merging','Already Merged','\033[91m Failed \033[0m']
            #print process.status
            print '%30s: %6i %6i %.3i'% (process.name, process.rootFileCounter,process.numberOfFiles, 100*float(process.rootFileCounter)/float(process.numberOfFiles)), status_message[process.status]
            readyFiles += process.rootFileCounter
        print 'Number of files: ',readyFiles,'/',self.totalFiles,'(%.3i)' % (100*(1-float(readyFiles)/float(self.totalFiles)))
    #take care of merging
    def merge_files(self,OutputDirectory,nameOfCycle,InputData):
        self.merge.merge(OutputDirectory,nameOfCycle,self.subInfo,self.workdir,InputData)
    #wait for every process to finish
    def merge_wait(self):
        self.merge.wait_till_finished()
    #see how many jobs finished (or error)
    def get_subInfoFinish(self):
        for process in self.subInfo:
            if process.status==0 or process.status==1: 
                return False
        return True
#class to take care of merging (maybe rethink design)
class MergeManager(object):
    def __init__(self,add,force,wait,onlyhist=False):
        self.add = add
        self.force = force
        self.active_process=[]
        self.wait = wait
        self.onlyhist = onlyhist

    def get_mergerStatus(self):
        if self.add or self.force or self.onlyhist:
            return True
        else:
            return False

    def merge(self,OutputDirectory,nameOfCycle,info,workdir,InputData):
        if not self.add and not self.force and not self.onlyhist: return  
        #print "Don't worry your are using nice = 10" 
        OutputTreeName = ""
        for inputObj in InputData:
            for mylist in inputObj.io_list.other:
                if "OutputTree" in mylist:
                    OutputTreeName= mylist[2]
        for process in info:
            if not process.numberOfFiles == process.rootFileCounter:
                continue
            if (not os.path.exists(OutputDirectory+'/'+nameOfCycle+'.'+process.data_type+'.'+process.name+'.root') and process.status!=2) or self.force:
                self.active_process.append(add_histos(OutputDirectory,nameOfCycle+'.'+process.data_type+'.'+process.name,process.numberOfFiles,workdir,OutputTreeName,self.onlyhist))
                process.status = 2
            #elif process.status !=2: 
            #    process.status = 3

    def wait_till_finished(self):
        if not self.wait: return
        for process in self.active_process:
            if not process: continue
            print process.communicate()[0]
            if not process.poll():
                process.wait()
                #os.kill(process.pid,-9)
