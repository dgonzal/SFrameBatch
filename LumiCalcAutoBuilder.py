#!/usr/bin/env python
# -*- coding: utf-8 -*-

import ConfigParser
# SFrameBatch comes with a standalone version to read the number of entries.
# Keep this in mind if it need to be updated some day
import readaMCatNloEntries

import sys, glob, copy, os, re
# From a list of CrossSections and XML Files this class creates the sframe steering file!
# Lets see how complicated this can get ???  


class process_helper(object):
    def __init__(self,name,crosssection,path,numberEvents):
        self.name = name
        
        self.crosssection = float(crosssection)
        self.path = path
        self.numberEvents = float(numberEvents)
        self.lumi = float(numberEvents)/float(crosssection)
    def printInfo(self):
        print "Process Name:",self.name,"CrossSection:",self.crosssection,"XML path:",self.path,"Number of Events:",self.numberEvents,"Lumi:",self.Lumi

def str2bool(v):
  return v.lower() in ("yes", "true", "t", "1")

class lumicalc_autobuilder(object):
    def __init__(self,path_to_Data):
        self.ProcessList = []
        finished_samples = False
        self.UserConfigText = []
        with open(str(path_to_Data)) as f:
            for line in f:
                if '#' in line or line == '\n' :
                    continue
                elif 'USERCONFIGBLOCK' in line:
                    finished_samples = True
                    continue
                if finished_samples:
                    striped_line = line.lstrip().rstrip()
                    if striped_line: self.UserConfigText.append(striped_line)
                    continue
                    
                tmpsplit = line.split()
                if not tmpsplit:continue

                for exp in glob.glob(tmpsplit[1]):
                    list_process = copy.deepcopy(tmpsplit)
                    print list_process
                    currentfile = ''
                    if '*' in tmpsplit[1]:
                        split_wildcards = tmpsplit[1].split('*')
                        currentwork = exp[len(split_wildcards[0])-1:len(exp)-len(split_wildcards[1])]
                        list_process[0]= list_process[0].replace('*',currentwork)
                        list_process[1]= exp

                    #if '/' not in list_process[1]:
                    list_process[1] = os.path.abspath(list_process[1])

                    #print list_process
                    if 'data' in list_process[0].lower():
                        self.ProcessList.append(process_helper(list_process[0],1,list_process[1],1))
                        continue
                    numberEvents = 0
                    lastxmlline = ''
                
                    for xmlline in reversed(open(list_process[1]).readlines()):
                        lastxmlline = xmlline.rstrip()
                        #print list_process[1],lastxmlline
                        if lastxmlline:
                            break
                    #print 'Bool',str2bool(list_process[4])

                    if '<!--' not in lastxmlline or '-->' not in lastxmlline:
                        #print list_process[1]
                        if len(list_process) < 5:
                            if len(list_process) == 4: 
                                numberEvents = float(list_process[3])
                            else:
                                print "No idea which method to use to read entries please add cores and method"
                                print "for",list_process[0]
                                exit(1)
                        if len(list_process) > 4:
                            methodname = 'fast'
                            if not str2bool(list_process[4]): methodname = 'weights' 
                            print 'going to count events for',list_process[0]
                            numberEvents = readaMCatNloEntries.readEntries(int(list_process[3]),[list_process[1]],str2bool(list_process[4]))[0]
                            
                            with open(list_process[2], "a") as myfile:
                                myfile.write('<!-- NumberEntries="'+str(numberEvents)+'" Method="'+str(methodname)+'" -->')
                    else:
                        if len(list_process) == 4: 
                            numberEvents = float(list_process[3])
                        else:
                            splitted_lastwords = lastxmlline.split('"')
                            #print splitted_lastwords
                            if len(splitted_lastwords)>1:
                                numberEvents = splitted_lastwords[1]
                            else:
                                #print lastxmlline.split(' ')
                                numberEvents = lastxmlline.split(' ')[-2]

                    crosssectionNumber = list_process[2]
                    if '*' in crosssectionNumber:
                        numbers = crosssectionNumber.split('*')
                        crosssectionNumber = float(1.)
                        for num in numbers:
                            #print num
                            crosssectionNumber = float(crosssectionNumber)*float(num)
                    self.ProcessList.append(process_helper(list_process[0],crosssectionNumber,list_process[1],numberEvents))
                         
                
        # Follows sframe conventions as of 2016
        # Parts are filled with __CHANGE_ME__!!!
        # UserConfig Section is missing  
    def write_to_toyxml(self,xmlname):
        with open(str('lumi_'+xmlname.replace('xml','py')),'w+') as nf:
            nf.write('lumi_list = [\n')
            for i in self.ProcessList:
                nf.write('\t[\''+i.name+'\','+str(i.lumi)+'],\n')
            nf.write('\t]')
        
        with open(str(xmlname),'w+') as f:
            f.write('<?xml version="1.0" encoding="UTF-8"?>\n')
            f.write('<!DOCTYPE JobConfiguration PUBLIC "" "JobConfig.dtd"[\n')

            for i in self.ProcessList:
                f.write('<!ENTITY '+i.name+' SYSTEM "'+i.path+'">\n')
                print "Added to Entity",i.name,i.path
            f.write(']>\n') 
            f.write('\n<!-- \n<ConfigParse NEventsBreak="0" FileSplit="30" AutoResubmit="3" MaxJobsPerProcess="150"/>\n<ConfigSGE RAM ="2" DISK ="2" Mail="XXX@desy.de" Notification="as" Workdir="workdir.PreSel_v1"/>\n-->\n\n')
            f.write('<JobConfiguration JobName="ExampleCycleJob" OutputLevel="INFO">\n')
            f.write('\t<Library Name="__CHANGE_ME__"/>\n')
            f.write('\t<Package Name="__CHANGE_ME__.par" />\n')
            f.write('\t<Cycle Name="uhh2::AnalysisModuleRunner" OutputDirectory="./__CHANGE_ME__/" PostFix="" TargetLumi="__CHANGE_ME__">\n')
            for i in self.ProcessList:                
                datatype = 'MC'
                if 'data' in i.name.lower(): datatype = 'DATA'
                f.write('\t\t<InputData Lumi="'+str(i.lumi)+'" NEventsMax="-1" Type="'+datatype+'" Version="'+i.name+'" Cacheable="False">\n')
                f.write('\t\t\t&'+i.name+';\n')
                f.write('\t\t\t<InputTree Name="AnalysisTree" />\n')
                f.write('\t\t\t<OutputTree Name="AnalysisTree" />\n')
                f.write('\t\t</InputData>\n')
                print 'Added Process to InputData:', i.name,'with lumi:',i.lumi
            f.write('\t\t<UserConfig>\n')
            f.write('\t\t<!-- Please add all the collections and stuff you need -->\n')
            for line in self.UserConfigText:
                f.write('\t\t\t'+line+'\n')
            f.write('\t\t</UserConfig>\n')
            f.write('\t</Cycle>\n')
            f.write('</JobConfiguration>\n')
            print 'Process list contains',len(self.ProcessList),'entries'
        #con = create_database(data)
        #lumi


if __name__ == "__main__":
    print "Expected format: path to Database, Name for new pre-XMl file"
    TestInfo = lumicalc_autobuilder(sys.argv[1:][0])
    TestInfo.write_to_toyxml(sys.argv[1:][1])
    exit(0)
