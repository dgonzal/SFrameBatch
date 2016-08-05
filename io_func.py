#!/usr/bin/env python

#python classes
#import xml.dom.minidom

#import glob
#import getopt

from xml.dom.minidom import parse, parseString
from xml.dom.minidom import Document
import xml.sax

import math
import time
import ROOT
import copy

#my classes
from Inf_Classes import *
from batch_classes import *

def write_job(Job,Version=-1,SkipEvents=0,MaxEvents=-1,NFile=None, FileSplit=-1,workdir="workdir",LumiWeight=1):
    doc = Document()
    root = doc.createElement("JobConfiguration")
    root.setAttribute( 'JobName', Job.JobName)
    root.setAttribute( 'OutputLevel', Job.OutputLevel)
    
    for lib in Job.Libs:
        # Create Element
        tempChild = doc.createElement('Library')
        root.appendChild(tempChild)
        # Set Attr.
        tempChild.setAttribute( 'Name', lib)
    
    for pack in Job.Packs:
        # Create Element
        tempChild = doc.createElement('Package')
        root.appendChild(tempChild)
        # Set Attr.
        tempChild.setAttribute( 'Name', pack)
        
    for cycle in Job.Job_Cylce:
        # Create Element
        tempChild = doc.createElement('Cycle')
        root.appendChild(tempChild)
        # Set Attr.
        tempChild.setAttribute( 'Name', cycle.Cyclename)

        if not os.path.exists(cycle.OutputDirectory+'/'+workdir+'/') and "__NOTSET__" not in cycle.OutputDirectory:
            os.makedirs(cycle.OutputDirectory+'/'+workdir+'/')
        tempChild.setAttribute('OutputDirectory', cycle.OutputDirectory+'/'+workdir+'/')
    
        if NFile is not None  and NFile is not -1:
            tempChild.setAttribute('PostFix', cycle.PostFix+'_'+str(NFile))
        tempChild.setAttribute('TargetLumi', cycle.TargetLumi)
        
        cycleLumiWeight = LumiWeight if cycle.usingSFrameWeight else 1.

        for p in range(len(cycle.Cycle_InputData)):
            version_check = True
            if(Version!=-1): 
                version_check = False
                for entry in Version:
                    if(cycle.Cycle_InputData[p].Version==entry):
                        version_check = True

            if not version_check: continue;
            # Create Element
            InputGrandchild= doc.createElement('InputData')
            tempChild.appendChild(InputGrandchild)
            
            InputGrandchild.setAttribute('Lumi', str(float(cycle.Cycle_InputData[p].Lumi)*cycleLumiWeight))
            InputGrandchild.setAttribute('Type', cycle.Cycle_InputData[p].Type)
            InputGrandchild.setAttribute('Version', cycle.Cycle_InputData[p].Version)
            if FileSplit!=-1:
                InputGrandchild.setAttribute('Cacheable', 'False')
            else:
                InputGrandchild.setAttribute('Cacheable', cycle.Cycle_InputData[p].Cacheable)
                InputGrandchild.setAttribute('NEventsSkip', str(SkipEvents))
                InputGrandchild.setAttribute('NEventsMax', str(MaxEvents))
        
            count_i =-1
            #print len(cycle.Cycle_InputData[p].io_list)
            for entry in cycle.Cycle_InputData[p].io_list.FileInfoList:
                count_i +=1
                if FileSplit > 0:
                    if not (count_i<(NFile+1)*FileSplit and count_i>= NFile*FileSplit):
                        continue
                Datachild= doc.createElement(entry[0])
                InputGrandchild.appendChild(Datachild)
                for it in range(1,len(entry),2):
                    #print entry[it],entry[it+1]
                    Datachild.setAttribute(entry[it],entry[it+1])
                
            for entry in cycle.Cycle_InputData[p].io_list.other:
                Datachild= doc.createElement(entry[0])
                InputGrandchild.appendChild(Datachild)
                for it in range(1,len(entry),2):
                    #print entry[it],entry[it+1]
                    Datachild.setAttribute(entry[it],entry[it+1])
            if len(cycle.Cycle_InputData[p].io_list.InputTree)!=3:
                print 'something wrong with the InputTree, lenght',len(cycle.Cycle_InputData[p].io_list.InputTree)
                print cycle.Cycle_InputData[p].io_list.InputTree
                print 'going to exit'
                exit(0)
            
            Datachild= doc.createElement(cycle.Cycle_InputData[p].io_list.InputTree[0])
            InputGrandchild.appendChild(Datachild)
            Datachild.setAttribute(cycle.Cycle_InputData[p].io_list.InputTree[1],cycle.Cycle_InputData[p].io_list.InputTree[2])
           
        #InGrandGrandchild= doc.createElement('In')
        ConfigGrandchild  = doc.createElement('UserConfig')
        tempChild.appendChild(ConfigGrandchild)

        for item in cycle.Cycle_UserConf:
            ConfigGrandGrandchild = doc.createElement('Item')
            ConfigGrandchild.appendChild(ConfigGrandGrandchild)
            ConfigGrandGrandchild.setAttribute('Name',item.Name)
            ConfigGrandGrandchild.setAttribute('Value',item.Value)

    return root.toprettyxml()


class fileheader(object):
    def __init__(self,xmlfile):
        f = open(xmlfile)
        line = f.readline()
        self.header = []
        self.Version = []
        self.AutoResubmit =0
        self.MaxJobsPerProcess = -1
        self.RemoveEmptyFileSplit = False
        while '<JobConfiguration' not in line:
            self.header.append(line)
            line = f.readline()
            if 'ConfigParse' in line:
                self.ConfigParse = parseString(line).getElementsByTagName('ConfigParse')[0]
                self.NEventsBreak = int(self.ConfigParse.attributes['NEventsBreak'].value)
                self.FileSplit = int(self.ConfigParse.attributes['FileSplit'].value)
                if self.ConfigParse.hasAttribute('AutoResubmit'):
                    self.AutoResubmit = int(self.ConfigParse.attributes['AutoResubmit'].value)
                if self.ConfigParse.hasAttribute('MaxJobsPerProcess'):
                    self.MaxJobsPerProcess = int(self.ConfigParse.attributes['MaxJobsPerProcess'].value)
                if self.ConfigParse.hasAttribute('RemoveEmptyFileSplit'):
                    self.RemoveEmptyFileSplit = bool(self.ConfigParse.attributes['RemoveEmptyFileSplit'].value)

            if 'ConfigSGE' in line:
                self.ConfigSGE = parseString(line).getElementsByTagName('ConfigSGE')[0]
                self.RAM = self.ConfigSGE.attributes['RAM'].value
                self.DISK = self.ConfigSGE.attributes['DISK'].value
                self.Notification = self.ConfigSGE.attributes['Notification'].value
                self.Mail = self.ConfigSGE.attributes['Mail'].value
                self.Workdir = self.ConfigSGE.attributes['Workdir'].value
        f.close()   

def get_number_of_events(Job, Version, atleastOneEvent = False):
    InputData = filter(lambda inp: inp.Version==Version[0], Job.Job_Cylce[0].Cycle_InputData)[0]
    NEvents = 0
    if len(InputData.io_list.FileInfoList[:])<5:
        atleastOneEvent=False
    for entry in InputData.io_list.FileInfoList[:]:
            for name in entry:
                if name.endswith('.root'):
                    f = ROOT.TFile(name)
                    try:
                        n = f.Get(str(InputData.io_list.InputTree[2])).GetEntriesFast()
                        if n < 1:
                            InputData.io_list.FileInfoList.remove(entry)
                            break
                        else:
                            NEvents += n
                            if atleastOneEvent: 
                                f.Close()
                                return 1
                    except:
                        print name,'does not contain an InputTree'
                    f.Close()
    return NEvents

def write_all_xml(path,datasetName,header,Job,workdir):
    NEventsBreak= header.NEventsBreak
    FileSplit=header.FileSplit
    FileSplitCompleteRemove = header.RemoveEmptyFileSplit
    MaxJobs = header.MaxJobsPerProcess
    NFiles=0

    Version =datasetName
    if Version[0] =='-1':Version =-1

    if NEventsBreak!=0 and FileSplit<=0:
        NEvents = get_number_of_events(Job, Version)
        if NEvents<=0: 
            print Version[0],'has no InputTree'
            return NFiles
        print '%s: %i events' % (Version[0], NEvents)
        NFiles = int(math.ceil(NEvents / float(NEventsBreak)))
        if NFiles > MaxJobs and MaxJobs > 0:
            print 'Too many Jobs, changing NEventsBreak mode'
            print 'Max number of Jobs',MaxJobs,'Number of xml-Files per Job',NFiles
            NEventsBreak = int(math.ceil(NEvents/float(MaxJobs)))
        SkipEvents = NEventsBreak
        MaxEvents = NEventsBreak   

        for i in xrange(NFiles):
            if i*SkipEvents >= NEvents:
                break 
            if (i+1)*MaxEvents >= NEvents:
                MaxEvents = NEvents-i*SkipEvents
            LumiWeight = float(NEvents)/float(MaxEvents)
            outfile = open(path+'_'+str(i+1)+'.xml','w+')
            for line in header.header:
                outfile.write(line)
            outfile.write(write_job(Job,Version,i*SkipEvents,MaxEvents,i,-1,workdir,LumiWeight))
            outfile.close()
 
    elif FileSplit>0:
        if FileSplitCompleteRemove:
            print "Removing all empty files in FileSplit mode."
        for entry in Version:
            NEvents = get_number_of_events(Job,[entry], FileSplitCompleteRemove)
            if NEvents <= 0:
                print 'No entries found for',entry,'Going to ignore this sample.'
                continue
            print 'Splitting job by files',entry
            for cycle in Job.Job_Cylce:
                for p in range(len(cycle.Cycle_InputData)):
                    if(cycle.Cycle_InputData[p].Version==entry) or Version ==-1:
		        Total_xml = len(cycle.Cycle_InputData[p].io_list.FileInfoList)
                        numberOfJobs = int(math.ceil(float(Total_xml)/FileSplit))
                        numberOfSplits = FileSplit
                        if numberOfJobs > MaxJobs and MaxJobs >0 :
                            numberOfSplits = int(math.ceil(float(Total_xml)/MaxJobs))
                            numberOfJobs = int(math.ceil(float(Total_xml)/numberOfSplits))
                            print 'More than',MaxJobs,'Jobs. Changing FileSplit mode'
                            print 'New number of Jobs',numberOfJobs,'Number of xml-Files per Job',numberOfSplits

                        for it in range(numberOfJobs):
                            outfile = open(path+'_'+str(it+1)+'.xml','w+')
                            for line in header.header:
                                outfile.write(line)
                            outfile.write(write_job(Job,Version,0,-1,it,numberOfSplits,workdir))
                            outfile.close()
                            NFiles+=1
    else:
        NFiles+=1
        outfile = open(path+'_OneCore'+'.xml','w+')
        for line in header.header:
            outfile.write(line)
        outfile.write(write_job(Job,Version,0,-1,"",0,workdir))
        outfile.close()

    return NFiles


def result_info(Job, path, header, other = []):
    #get a xml file with all the infomartion that you need to proced
    ResultJob = copy.deepcopy(Job)
    for cycle in ResultJob.Job_Cylce:
        for inputdata in cycle.Cycle_InputData:
            #print inputdata.io_list.InputTree
            #print inputdata.io_list.other
            output_exist = False
            other_index = 0
            for listoflists in inputdata.io_list.other:
                for part in listoflists:
                    if part == 'OutputTree':
                        output_exist = True
                        break
                    other_index +=1
            if not output_exist:
                return 0

            if len(inputdata.io_list.FileInfoList)==0:
                continue

            inputdata.io_list.FileInfoList = [['In','Lumi',inputdata.io_list.FileInfoList[0][2],'FileName',cycle.OutputDirectory+"/"+path+"/uhh2.AnalysisModuleRunner.*."+inputdata.Version+"_*.root"]]   
            inputdata.io_list.InputTree  =['InputTree','Name',inputdata.io_list.other[other_index][2]]
            if not other:
                inputdata.io_list.other = []
            else:
                if len(other) == 1:
                    if other[0] =="-1":
                        inputdata.io_list.other = [['OutputTree','Name',inputdata.io_list.other[other_index][2]]]
                    else:
                        inputdata.io_list.other = [['OutputTree','Name',other[0]]]  
                else:
                    inputdata.io_list.other = other
            
        for cycle_item in cycle.Cycle_UserConf:
            if cycle_item.Name == 'AnalysisModule':
                cycle_item.Value = "__NOTSET__"
        cycle.OutputDirectory = "__NOTSET__"

    outfile = open(path+'/Result.xml','w')
    for line in header.header:
        outfile.write(line)
    outfile.write(write_job(ResultJob,-1,0,-1,None,0,""))
    outfile.close()
    
    return 1




