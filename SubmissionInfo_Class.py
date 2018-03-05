#!/usr/bin/env python
# -*- coding: utf-8 -*-
import subprocess
import json
import time

# class for the submission information
class SubInfo(object):
    def __init__(self,name='',numberOfFiles=0,data_type='',resubmit =0):
        self.name = name
        self.numberOfFiles =numberOfFiles #number of expected files
        self.data_type = data_type
        self.rootFileCounter = 0 #number of expected files 
        self.status = 0   # 0: init, 1: data on disk
        self.missingFiles = []
        self.pids = ['']*numberOfFiles
        self.notFoundCounter = [0]*numberOfFiles
        self.reachedBatch = [False]*numberOfFiles
        self.jobsRunning = [False]*numberOfFiles
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
    def process_batchStatus(self,batch,it):
        self.jobsRunning[it] = False
        self.notFoundCounter[it] += 1
        if batch == 1:
            self.notFoundCounter[it]=0 # Safeguard, no action is taken if a job is not found once.
            self.reachedBatch[it] = True # Use to understand when a job reached the batch before taking any action
            self.jobsRunning[it] = True
        #kill jobs with have an error state
        if batch == 2:
            print "not yet implemented in ht condor! Do not remember what happens!"
            return -2
        return batch

