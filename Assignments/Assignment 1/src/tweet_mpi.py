#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr  5 18:19:54 2021

@author: short
"""


"""
Global variables
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
    f = open("../data/AFINN.txt", "r")
    words = str(f.read()).split('\n')
    f.close()
    
    from collections import Counter
    scores = Counter()
    for word in words:
        word_score = word.split('\t')
        scores[word_score[0]] = int(word_score[1])
    
    #print("There are {} semtiment words.".format(len(scores)))
    return scores

# categorise tweets into sections

# what is offset in tweet.json?

def get_region_map(tweet):
    
    # loading boundary information from json
    import json
    f = open("../data/melbGrid.json", "r")
    melbGrid = json.loads(f.read())
    f.close()

    
    # use dictionary instead
    
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
    
    
    # categorise tweets into regions
    
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
    
    # final check, should not have any issue for tweets in Melbourne
    if region in grid_names:
        return region
    else: 
        return "none"


def process_one_tweet(tweet, sentiment):
    
    """
    一次处理一条tweet，
    input: a dictionary format data
    output: score，region
    """
    # calculate the sentiment score for each tweets
    text = tweet["value"]["properties"]["text"]
    score = 0
    tokens = text.lower().split(" ")
    #print(tokens)
    
    symbols = ['!', ',', '?', '.', '"' ,'\'']
    
    for token in tokens:        
    # method 2: {'C2': 9795}, time used: 2.663618803024292

        for char in token:
            if char in symbols:
                token = token.split(char)[0]
                break
        
        if token in sentiment.keys():
            score += sentiment[token]
                
    region = get_region_map(tweet)
    return [score, region]


def process_batch_tweets(tweets, sentiment):
    """
    一次处理一个列表的tweets，

    Parameters
    ----------
    tweets : a list of dictionaries
        
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
        scoreboard[region] += score
        region_count[region] += 1
    return [scoreboard, region_count]

    

def enum(*sequential, **named):
    """Handy way to fake an enumerated type in Python
    http://stackoverflow.com/questions/36932/how-can-i-represent-an-enum-in-python
    """
    enums = dict(zip(sequential, range(len(sequential))), **named)
    return type('Enum', (), enums)

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

def main():

    import time
    from mpi4py import MPI

    start = time.time()
    
    # Initializations and preliminaries
    comm = MPI.COMM_WORLD   # get MPI communicator object
    size = comm.Get_size()  # total number of processes
    rank = comm.Get_rank()  # rank of this process

    def worker():
        while True:
            task = comm.recv(source=0)  # repetitively receive data from master process
            if task:
                sentiment = get_senti_map()
                result = process_batch_tweets(task, sentiment)  # task is a list format of multiple tweets content
                del task
                comm.send(result, dest=0)  # send the temporal result to master
            else:
                break

    def master():
        start = time.time()
        from collections import Counter
        score_board = Counter()  # for calculating the total scores
        count_board = Counter()  # for counting the number of tweets

        import json
        offsets = get_offset("line2offset_small.txt")  # get offset table

        # parameters
        num_tweets = len(offsets)
        tweet_idx = 0; batch_size = 1000
        num_batches = num_tweets // batch_size
        split_size = batch_size // (size - 1)
        tweets = []; batch_id = 1

        f = open("../data/smallTwitter.json", 'r')
        while tweet_idx < num_tweets:
            offset = offsets[tweet_idx]  # get the offset of i-th tweet in the file
            f.seek(offset)
            tweet = json.loads(txt2dict(f.readline()))  # preprocessing tweet and convert to json format
            tweets.append(tweet)

            # assign task to workers
            if batch_id <= num_batches and (tweet_idx+1) % split_size == 0:

                assigned_rank = 1 + (tweet_idx // split_size) % (size - 1)  # worker rank id
                comm.send(tweets, dest=assigned_rank)
                del tweets
                tweets = []


            # print batch information
            if (tweet_idx+1) % batch_size == 0:
                print("Batch %d done with %d workers and batch size %d" % (batch_id, size-1, batch_size))
                batch_id += 1

            tweet_idx += 1

        for i in range(num_batches*(size-1)):
            rank_id = 1 + i % (size - 1)

            process_result = comm.recv(source=rank_id)
            scores = process_result[0]
            counts = process_result[1]

            # merge result
            for region, score in scores.items():
                score_board[region] += score
            for region, count in counts.items():
                count_board[region] += count

        # process the remaining tweets
        if tweets:
            batch_size = len(tweets)

            print("Batch %d done with %d workers and batch size %d" % (batch_id, size-1, batch_size))

            # assign tasks to workers
            for rank_idx in range(1, size):
                split_size = batch_size // (size - 1)
                starting = (rank_idx - 1) * split_size
                ending = rank_idx * split_size if rank_idx < size - 1 else batch_size
                comm.send(tweets[starting: ending], dest=rank_idx)

            # collect results from workers
            for rank_idx in range(1, size):
                process_rest = comm.recv(source=rank_idx)
                scores = process_rest[0]
                counts = process_rest[1]

                # merge results
                for region, score in scores.items():
                    score_board[region] += score
                for region, count in counts.items():
                    count_board[region] += count

        # send stop message to worker to break the while loop
        for rank_idx in range(1, size):
            comm.send(None, dest=rank_idx)
        end = time.time()

        print(score_board, count_board, end - start)
        f.close()

    # Master process executes code below
    if rank == 0:
        master()
    else:
        # Worker processes execute code below
        worker()

if __name__ == "__main__":
    main()

