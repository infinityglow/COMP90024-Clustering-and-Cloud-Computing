#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  8 12:35:39 2021

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
    AFINN = "../data/AFINN.txt"
    f = open(AFINN, "r")
    words = str(f.read()).split('\n')
    f.close()

    from collections import Counter
    scores = Counter()
    for word in words:
        word_score = word.split('\t')
        scores[word_score[0]] = int(word_score[1])

    # print("There are {} semtiment words.".format(len(scores)))
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
    melbGrid = "../data/melbGrid.json"

    # loading boundary information from json
    import json
    f = open(melbGrid, "r")
    melbGrid = json.loads(f.read())
    f.close()

    grid_x = [0, 1, 2, 3, 4, 5]
    grid_y = ['0', 'A', 'B', 'C', 'D']

    x_boundaries = {}
    y_boundaries = {}
    x_boundary_id = [0, 1, 2, 3, 12, 12]
    y_boundary_id = [0, 0, 4, 8, 13]

    for i in range(len(grid_x)):
        if i != 5:
            x_boundaries[grid_x[i]] = melbGrid["features"][x_boundary_id[i]]['properties']["xmin"]
        else:
            x_boundaries[grid_x[i]] = melbGrid["features"][x_boundary_id[i]]['properties']["xmax"]
    # print(x_boundaries)

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
    x_id, y_id = 0, 0
    for k, v in x_boundaries.items():
        if x_coordinate > v:
            continue
        else:
            x_id = k
            break

    for k, v in y_boundaries.items():
        if y_coordinate <= v:
            continue
        else:
            y_id = k
            break
    region = str(y_id) + str(x_id)

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
    # calculate the sentiment score for each tweets
    text = tweet["value"]["properties"]["text"]
    score = 0
    tokens = text.lower().split(" ")

    symbols = ['!', ',', '?', '.', '"', '\'']

    for token in tokens:

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


def __main__():
    import time
    from mpi4py import MPI

    start = time.time()

    # Define MPI message tags
    tags = enum('READY', 'DONE', 'EXIT', 'START')

    # Initializations and preliminaries
    comm = MPI.COMM_WORLD  # get MPI communicator object
    size = comm.Get_size()  # total number of processes
    rank = comm.Get_rank()  # rank of this process
    status = MPI.Status()  # get MPI status object

    def worker():
        while True:
            comm.send(None, dest=0, tag=tags.READY)
            task = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
            tag = status.Get_tag()

            if tag == tags.START:  # worker receives "start" signal and starts calculation
                sentiment = get_senti_map()
                result = process_batch_tweets(task, sentiment)  # task is a list format of multiple tweets content
                comm.send(result, dest=0, tag=tags.DONE)
            elif tag == tags.EXIT:  # worker receives "exit" signal and terminates
                break

        comm.send(None, dest=0, tag=tags.EXIT)

    def master():

        # result placeholder
        from collections import Counter
        score_board = Counter()  # for calculating the total scores
        count_board = Counter()  # for counting the number of tweets

        # load data
        import json
        tweetData = "../data/smallTwitter.json"
        f = open(tweetData, "r")
        offsets = get_offset("line2offset_small.txt")  # get offset table

        # parameters
        batch_size = 330  # user defined

        num_workers = size - 1
        closed_workers = 0
        num_of_tweets = len(offsets)
        num_of_tasks = int(num_of_tweets / batch_size)
        task_index = 0
        if num_of_tweets % batch_size != 0:
            num_of_tasks += 1

        print("Total of {} tweets and run in {} a batch".format(num_of_tweets, batch_size))
        print("Master starting with {} workers".format(num_workers))

        # iterate all tasks
        task = []
        tweet_idx = 0
        while closed_workers < num_workers:

            # load a batch of data from file
            while len(task) < batch_size and tweet_idx < num_of_tweets:
                offset = offsets[tweet_idx]  # get the offset of i-th tweet in the file
                f.seek(offset)
                tweet = json.loads(txt2dict(f.readline()))  # preprocessing tweet and convert to json format
                task.append(tweet)
                tweet_idx += 1

            # receive vacant signal and assign task
            data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
            source = status.Get_source()
            tag = status.Get_tag()
            if tag == tags.READY:
                # Worker is ready, assign task

                if task_index < num_of_tasks:
                    comm.send(task, dest=source, tag=tags.START)
                    # reset task batch after sent
                    del task
                    task = []
                    task_index += 1
                else:
                    # no task remains, call exit to worker
                    comm.send(None, dest=source, tag=tags.EXIT)

            elif tag == tags.DONE:
                # worker is done with last batch, collect results from workers
                results = data
                scores = results[0]
                counts = results[1]

                # merge result
                for region, score in scores.items():
                    score_board[region] += score
                for region, count in counts.items():
                    count_board[region] += count

            # worker is terminated
            elif tag == tags.EXIT:
                closed_workers += 1

        print("Master finishing with \nscores {} and \ncounts {}".format(score_board, count_board))
        print('Time used: {}'.format(time.time() - start))

    # Master process executes code below
    if rank == 0:
        master()
    else:
        # Worker processes execute code below
        worker()


__main__()


