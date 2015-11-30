#!/usr/bin/env python

import ROOT

def check_TreeExists(filename,treename):
     rootfile = ROOT.TFile.Open(filename)
     #print filename
     try:
          rootTree = rootfile.Get(treename)
          #print 'True'
          return True
     except:
          #print 'False'
          return False
     #entries = rootTree.GetEntriesFast()
     #if rootTree: return False
     #return True
