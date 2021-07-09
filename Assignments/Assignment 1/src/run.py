import time
import json
from mpi4py import MPI


comm = MPI.COMM_WORLD
comm_rank = comm.Get_rank()
comm_size = comm.Get_size()

def compute_score(sentiment_map, text):
    text = text.split()
    score = 0
    for word in text:
        word = word.lower()
        symbols = ['!', ',', '?', '.', '"' ,'\'']

        for char in word:
            if char in symbols:
                word = word.split(char)[0]
                break

        if word in sentiment_map.keys():
            score += sentiment_map[word]
    return score

def get_senti_map():
    sentiment_map = {}
    with open("../data/AFINN.txt", 'r') as f:
        for line in f.readlines():
            word, score = line.strip().split("\t")
            sentiment_map[word] = int(score)
    return sentiment_map

def get_region_map(rows):
    sentiment_map = get_senti_map()
    with open("../data/melbGrid.json", 'r') as f:
        grid = json.load(f)

    regions = grid['features']
    region_map = {}
    for region in regions:
        cell = region['properties']['id']

        x_min = region['properties']['xmin']
        x_max = region['properties']['xmax']
        y_min = region['properties']['ymin']
        y_max = region['properties']['ymax']

        region_map[cell] = {}

        region_map[cell]['x_min'] = x_min
        region_map[cell]['x_max'] = x_max
        region_map[cell]['y_min'] = y_min
        region_map[cell]['y_max'] = y_max

        region_map[cell]['total_tweets'] = region_map[cell].get('total_tweet', 0)
        region_map[cell]['score'] = region_map[cell].get('score', 0)

    for row in rows:
        x, y = row['value']['geometry']['coordinates']
        text = row['value']['properties']['text']
        score = compute_score(sentiment_map, text)
        for cell, info in region_map.items():
            x_min = info['x_min']
            x_max = info['x_max']
            y_min = info['y_min']
            y_max = info['y_max']

            if x_min < x < x_max and y_min < y < y_max:
                region_map[cell]['total_tweets'] += 1
                region_map[cell]['score'] += score
    return region_map

def txt2dict(txt):
    txt = txt.strip('\n')
    if txt.endswith(','):
        txt = txt[:-1]
    elif txt.endswith(']}'):
        txt = txt[:-2]
    return json.loads(txt)

def combine(region_maps):
    result = {}
    for region_map in region_maps:
        for k, v in region_map.items():
            if k not in result:
                result[k] = {}
            total_tweets = v['total_tweets']
            score = v['score']
            result[k]['total_tweets'] = result[k].get('total_tweets', 0) + total_tweets
            result[k]['score'] = result[k].get('score', 0) + score
    return result

def line2offset():
    f = open("../data/smallTwitter.json")
    with open("../mpi/line2offset.txt", 'w') as fw:
        fw.write("%d " % 0)
        line = f.readline()
        while line:
            fw.write("%d " % f.tell())
            line = f.readline()
    f.close()
if comm_rank == 0:
    start = time.time()
    f = open("../data/smallTwitter.json")
    # line2offset()
    fr = open("../mpi/line2offset.txt", 'r')
    lines = fr.readlines()[0]
    lines = list(map(int, lines.split()))
    data = [[] for i in range(comm_size)]
    max_entry = 1000
    num_entry_part = len(lines[1:-1]) // comm_size + 1

    cnt = 0
    for i, offset in enumerate(lines[1:-1]):
        f.seek(offset)
        line = f.readline()
        try:
            line = txt2dict(line)
            data[cnt].append(line)
        except:
            pass
        if (i+1) % num_entry_part == 0:
            cnt += 1

else:
    data = None
local_data = comm.scatter(data, root=0)
print('rank %d, got: %d rows' % (comm_rank, len(local_data)))
region_map = get_region_map(local_data)

# print("Cell\t#Total Tweets\t#Overal Sentiment Score")
# for cell, info in region_map.items():
#     total_tweets = info['total_tweets']
#     score = info['score']
#     print("%s\t%11d\t%20d" % (cell, total_tweets, score))

combined_region_map = comm.gather(region_map, root=0)
if comm_rank == 0:
    region_maps = combine(combined_region_map)
    print("Cell\t#Total Tweets\t#Overal Sentiment Score")
    for cell, info in region_maps.items():
        total_tweets = info['total_tweets']
        score = info['score']

        print("%s\t%11d\t%20d" % (cell, total_tweets, score))
    end = time.time()
    print(end - start)