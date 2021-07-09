#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  8 16:37:16 2021

@author: Yixuan Liang & Hongzhi Fu
"""

"""
Global Funtions for all worker nodes
"""
# read in scores
def get_senti_map():
    """

    Returns
    -------
    scores : Dictionary
        A dictionary that maps all key words to their scores.

    """
    AFINN = "AFINN.txt"
    f = open(AFINN, "r")
    words = str(f.read()).split('\n')
    f.close()
    
    from collections import Counter
    scores = Counter()
    for word in words:
        word_score = word.split('\t')
        scores[word_score[0]] = int(word_score[1])
    
    #print("There are {} semtiment words.".format(len(scores)))
    return scores

def get_region_map(tweet):
    """
    Parameters
    ----------
    tweet : a json format tweet content
    
    Returns
    -------
    region: string, the corresponding region this tweet belongs to 

    """
    melbGrid = "melbGrid.json"
    
    # loading boundary information from json
    import json
    f = open(melbGrid, "r")
    melbGrid = json.loads(f.read())
    f.close()
    
    grid_x = [0, 1, 2, 3, 4, 5]
    grid_y = ['0', 'A', 'B', 'C', 'D']
    
    x_boundaries = {}
    y_boundaries = {}
    x_boundary_id = [0,1,2,3,12,12]
    y_boundary_id = [0,0,4,8,13]
    
    for i in range(len(grid_x)):
        if i != 5:
            x_boundaries[grid_x[i]] = melbGrid["features"][x_boundary_id[i]]['properties']["xmin"]
        else:
            x_boundaries[grid_x[i]] = melbGrid["features"][x_boundary_id[i]]['properties']["xmax"]
    #print(x_boundaries)
    
    for i in range(len(grid_y)):
        if i != 0:
            y_boundaries[grid_y[i]] = melbGrid["features"][y_boundary_id[i]]['properties']["ymin"]
        else:
            y_boundaries[grid_y[i]] = melbGrid["features"][y_boundary_id[i]]['properties']["ymax"]
    
    
    # Categorise tweets into regions
    
    # get coordinates
    coordinates = tweet["value"]["geometry"]["coordinates"]
    x_coordinate = coordinates[0]
    y_coordinate = coordinates[1]

    # get region
    x_id, y_id = -1, -1
    for k, v in x_boundaries.items():
        if x_coordinate>v:
            continue
        else:
            x_id = k
            break
    
    for k, v in y_boundaries.items():
        if y_coordinate<=v:
            continue
        else:
            y_id = k
            break
    region = str(y_id)+str(x_id)
    
    grid_names = ['A1', 'A2', 'A3', 'A4', 'B1', 'B2', 'B3', 'B4', 
          'C1', 'C2', 'C3', 'C4', 'C5', 'D3', 'D4', 'D5']
    
    # Final check, return "None" if location not on grid
    # should not have any issue for tweets in Melbourne
    if region in grid_names:
        return region
    else: 
        return "None"


def process_one_tweet(tweet, sentiment):
    
    """
    handle one tweet at a time
    input: a dictionary format data
    output: score，region
    """
    region = get_region_map(tweet)
    if region == "None":
        score = 0
    else:
        # calculate the sentiment score for each tweets
        text = tweet["value"]["properties"]["text"]
        score = 0
        tokens = text.lower().split(" ")
        
        symbols = ['!', ',', '?', '.', '"' ,'\'']
        
        for token in tokens:        
    
            for char in token:
                if char in symbols:
                    token = token.split(char)[0]
                    break
            
            if token in sentiment.keys():
                score += sentiment[token]
                
    return [score, region]


def process_batch_tweets(tweets, sentiment):
    """
    handle a list of tweets

    Parameters
    ----------
    tweets : a list of dictionarys
        
    sentiment : dictionary
        

    Returns
    -------
    region_scoreboard : dictionay {region:score}
    region_count : dictionay {region:count}

    """
    from collections import Counter
    scoreboard = Counter()
    region_count = Counter()
    
    for tweet in tweets:
        score, region = process_one_tweet(tweet, sentiment)
        if region != "None":
            scoreboard[region] += score
            region_count[region] += 1
    return [scoreboard, region_count]

def get_offset(filename):
    with open(filename, 'r') as f:
        offsets = f.readlines()[0]
        offsets = list(map(int, offsets.split()))
    return offsets[1:-2]

def txt2dict(txt):
    txt = txt.strip('\n')
    if txt.endswith(','):
        txt = txt[:-1]
    elif txt.endswith(']}'):
        txt = txt[:-2]
    return txt

def enum(*sequential, **named):
    """Handy way to fake an enumerated type in Python
    http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python
    """
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)
