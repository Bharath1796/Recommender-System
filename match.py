import codecs
ratings=open("PATH","r")
ratmovie=[]
ratmovie=ratings.readlines()
print( ratmovie[0])

c0=[]
ratings.close()
try:
    with codecs.open("PATH","r",encoding="utf8") as c20m:
        for line in c20m:
            if(line.startswith("(")):
                l=line.split(',')
                mov=l[0]
                c0.append(mov[1:len(mov)])
except UnicodeDecodeError:
    pass
clus_0_rat=open("PATH","w")
for line in ratmovie:
    l=line.split(",")
    for line1 in c0:
        if(line1==l[1]):
            #print(l[0]+""+l[1]+"    "+l[2])
            clus_0_rat.write(l[0]+"\t"+l[1]+"\t"+l[2]+"\n")
clus_0_rat.close()
