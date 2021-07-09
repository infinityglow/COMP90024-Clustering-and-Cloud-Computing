#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  8 13:48:14 2021

@author: Hongzhi Fu
"""

def line2offset():
    f = open("tweets.json")
    with open("line2offset.txt", 'w') as fw:
        fw.write("%d " % 0)
        line = f.readline()
        while line:
            fw.write("%d " % f.tell())
            line = f.readline()
    f.close()

line2offset()
