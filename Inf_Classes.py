#!/usr/bin/env python

import xml.dom.minidom as minidom

class JobConfig(object):
    def __init__(self,node):
        for item in node.attributes.items():
            if(item[0]=='JobName'): self.JobName = item[1]
            if(item[0]=='OutputLevel'):  self.OutputLevel = item[1]
        self.Libs = []
        for child in node.getElementsByTagName('Library'):
            for lib in child.attributes.items():
                self.Libs.append(lib[1])
        self.Packs = []
        for child in node.getElementsByTagName('Package'):
            for pack in child.attributes.items():
                self.Packs.append(lib[1])
        self.Job_Cylce = []
        for item in node.getElementsByTagName('Cycle'):
            self.Job_Cylce.append(Cycle(item))

class Cycle(object):

    def __init__(self,node):
       
        for item in node.attributes.items():
            if(item[0]=='Name'): self.Cyclename = item[1]
            #if(item[0]=='RunMode'):  self.RunMode = item[1]
            #if(item[0]=='ProofServer'): self.ProofServer = item[1]
            #if(item[0]=='ProofWorkDir'):  self.ProofWorkDir = item[1]
            if(item[0]=='OutputDirectory'): self.OutputDirectory = item[1]
            if(item[0]=='PostFix'):  self.PostFix = item[1]
            if(item[0]=='TargetLumi'):  self.TargetLumi = item[1]
        
        self.Cycle_InputData =[]
        for item in node.getElementsByTagName('InputData'):
            self.Cycle_InputData.append(InputData(item))
        
        self.Cycle_UserConf = []
        for child in node.getElementsByTagName('UserConfig'):
            for item in child.getElementsByTagName('Item'):
                name =  None
                value = None
                for attr in item.attributes.items():
                    if(attr[0]=='Name'): name=attr[1]
                    if(attr[0]=='Value'): value=attr[1]
                self.Cycle_UserConf.append(UserConfig(name,value))
            
        #print self.Cycle_UserConf

class InputData(object):
    def __init__(self,node):
        self.NEventsSkip = 0        
        for item in node.attributes.items():
            if(item[0]=='Lumi'): self.Lumi = item[1]
            if(item[0]=='NEventsMax'): self.NEventsMax = item[1]
            if(item[0]=='Type'): self.Type = item[1]
            if(item[0]=='Version'): self.Version = item[1]
            if(item[0]=='Cacheable'): self.Cacheable = False
            if(item[0]=='NEventsSkip'): self.NEventsSkip = item[1]
        #print self.Version
        self.io_list =[]
        for item in node.childNodes:
            #print item.nodeValue
            if not item.nodeType == 3:
                help_list = []
                help_list.append(item.nodeName)
                for entry in item.attributes.items():
                    for y in entry:
                        help_list.append(y)
                self.io_list.append(help_list)
    def split_NEvents(self,NEventsBreak,LastBreak):
        self.NEventsBreak = NEventsBreak
        self.LastBreak = LastBreak
        
    
class UserConfig(object):
    def __init__(self,Name,Value):
        self.Name = Name
        self.Value = Value
