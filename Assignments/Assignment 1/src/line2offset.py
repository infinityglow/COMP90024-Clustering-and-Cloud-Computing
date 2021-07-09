def line2offset():
    f = open("../data/smallTwitter.json")
    with open("../mpi/line2offset.txt", 'w') as fw:
        fw.write("%d " % 0)
        line = f.readline()
        while line:
            fw.write("%d " % f.tell())
            line = f.readline()
    f.close()

line2offset()