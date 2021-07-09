#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Apr  8 13:48:14 2021

@author: Yixuan Liang & Hongzhi Fu
"""

def __main__():
    
    import time
    import util 
    from mpi4py import MPI

    start = time.time()
    tweetData = "tweets.json"
    offsetfile = "line2offset.txt"

    # Define MPI message tags
    tags = util.enum('READY', 'DONE', 'EXIT', 'START')
    
    # Define region names
    grid_names = ['A1', 'A2', 'A3', 'A4', 'B1', 'B2', 'B3', 'B4', 
      'C1', 'C2', 'C3', 'C4', 'C5', 'D3', 'D4', 'D5']

    # Initializations and preliminaries
    comm = MPI.COMM_WORLD   # get MPI communicator object
    size = comm.Get_size()  # total number of processes
    rank = comm.Get_rank()  # rank of this process
    status = MPI.Status()   # get MPI status object

    def worker():
        import json
        f = open(tweetData, "r")
        offsets = util.get_offset(offsetfile)  # get offset table
        
        while True:
            comm.send(None, dest=0, tag=tags.READY)
            paras = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
            tag = status.Get_tag()
            
    
            if tag == tags.START: # worker receives "start" signal and starts calculation
                sentiment = util.get_senti_map()
                start, end = paras
                
                task = []
                # load a batch of data from file
                while start < end:
                    offset = offsets[start]  # get the offset of i-th tweet in the file
                    f.seek(offset)
                    tweet = json.loads(util.txt2dict(f.readline()))  # preprocessing tweet and convert to json format
                    task.append(tweet)
                    start +=1
                    
                result = util.process_batch_tweets(task, sentiment) # task is a list format of multiple tweets content
                del task
                
                comm.send(result, dest=0, tag=tags.DONE) 
            elif tag == tags.EXIT: # worker receives "exit" signal and terminates
                break
            
        comm.send(None, dest=0, tag=tags.EXIT) 
        
    def master():

        # result placeholder
        from collections import Counter
        score_board = Counter() # for calculating the total scores
        count_board = Counter() # for counting the number of tweets
        
        # load offsets 
        offsets = util.get_offset(offsetfile)  # get offset table

        # parameters
        batch_size = 300  # user defined
        
        num_workers = size - 1
        closed_workers = 0
        num_of_tweets = len(offsets)
        num_of_tasks = int(num_of_tweets / batch_size)
        task_index = 0
        if num_of_tweets % batch_size != 0:
            num_of_tasks += 1

        if size == 1:
            import json
            f = open(tweetData, "r")
            offsets = util.get_offset(offsetfile)  # get offset table
            sentiment = util.get_senti_map()

            while task_index < num_of_tasks:

                starting = task_index * batch_size
                ending = min((task_index + 1) * batch_size, num_of_tweets)
                task = []

                while starting < ending:
                    offset = offsets[starting]  # get the offset of i-th tweet in the file
                    f.seek(offset)
                    tweet = json.loads(util.txt2dict(f.readline()))  # preprocessing tweet and convert to json format
                    task.append(tweet)
                    starting += 1

                result = util.process_batch_tweets(task, sentiment)  # task is a list format of multiple tweets content
                del task

                scores = result[0]
                counts = result[1]

                # merge result
                for region, score in scores.items():
                    score_board[region] += score
                for region, count in counts.items():
                    count_board[region] += count
                task_index += 1

        else:
            # iterate all tasks
            while closed_workers < num_workers:

                # receive vacant signal and assign task
                data = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                source = status.Get_source()
                tag = status.Get_tag()
                if tag == tags.READY:
                    # Worker is ready, assign task
                    if task_index < num_of_tasks:
                        starting = task_index * batch_size
                        ending = min((task_index + 1) * batch_size, num_of_tweets)
                        indexes = [starting, ending]
                        comm.send(indexes, dest=source, tag=tags.START)
                        # reset task batch after sent
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
            
        print("Cell \t#Total Tweets\t#Overall Sentiment Score")
        for region in grid_names:
            count = count_board[region]
            score = score_board[region]
            if score > 0:
                score = "+" + str(score)
            
            print("{}\t{}\t\t{}".format(region, count, score))
            
        print('Time used: {}'.format(time.time() - start))
   
    # Master process executes code below

    if rank == 0:
        master()
    else:
        # Worker processes execute code below
        worker()
        
__main__()


