#!/usr/bin/env python

import ROOT

def check_TreeExists(filename,treename):
     rootfile = ROOT.TFile.Open(filename)
     rootTree = rootfile.Get(treename)
     entries = rootTree.GetEntriesFast()
     if entries == 0: return False
     return True
