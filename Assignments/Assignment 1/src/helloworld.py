from mpi4py import MPI
import json
import linecache

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

# if rank == 0:
#     data = {'a': 7, 'b': 3.14}
#     # comm.send(data, dest=1, tag=11)
#     print("data ", data, "from ", rank)
# elif rank == 1:
#     # data = comm.recv(source=0, tag=11)
#     print("data ", data, "from ", rank)

# if rank == 0:
#     data = {'a': 7, 'b': 3.14}
#     req = comm.isend(data, dest=1, tag=11)
#     print("data ", data, "from ", rank)
#     req.wait()
# elif rank == 1:
#     req = comm.irecv(source=0, tag=11)
#     print("data ", data, "from ", rank)
#     data = req.wait()

# if rank == 0:
#     data = {'key1' : [7, 2.72, 2+3j],
#             'key2' : ( 'abc', 'xyz')}
# else:
#     data = None
# data = comm.bcast(data, root=0)
# print("data ", data, "from ", rank)


# File offset for the begin of each line
f = open("../data/smallTwitter.json", 'r')
def line2offset():
    with open("../mpi/line2offset.txt", 'w') as fw:
        fw.write("%d " % 0)
        line = f.readline()
        while line:
            fw.write("%d " % f.tell())
            line = f.readline()

def seekline(linenum):
    f.seek(0)
    f.seek(lines[linenum])
    line = f.readline()
    # line = line.strip('\n')
    return line

# line2offset()
fr = open("../mpi/line2offset.txt", 'r')
lines = fr.readlines()[0]
lines = list(map(int, lines.split()))

def txt2dict(txt):
    txt = txt.strip('\n')
    if txt.endswith(','):
        txt = txt[:-1]
    elif txt.endswith(']}'):
        txt = txt[:-2]
    return json.loads(txt)

for i, line in enumerate(lines[1:-1]):
    f.seek(line)
    try:
        txt = f.readline()
        txt2dict(txt)
    except:
        pass
print(len(lines[1:-1]))
# max_lines = 5
# for i in range(rank*max_lines, (rank+1)*max_lines):
#     print(rank, i)

f.close()