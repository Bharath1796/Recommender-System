import codecs
ratings=open("/run/media/bdam/USB STICK/Mini Project/ml-100k-output/Cluster0.txt","r")
ratmovie=[]
ratmovie=ratings.readlines()
print( ratmovie[0])
"""c0=open("/run/media/bdam/USB STICK/Mini Project/Cluster0-20m.txt","r")
clus0=c0.readlines()
c1=open("/run/media/bdam/USB STICK/Mini Project/Cluster1-20m.txt","r")
clus1=c1.readlines()
c2=open("/run/media/bdam/USB STICK/Mini Project/Cluster2-20m.txt","r")
clus2=c2.readlines()
c3=open("/run/media/bdam/USB STICK/Mini Project/Cluster3-20m.txt","r")
clus3=c3.readlines()
c4=open("/run/media/bdam/USB STICK/Mini Project/Cluster0-20m.txt","r")
clus4=c4.readlines()
ratings.close()
c0.close()
c1.close()
c2.close()
c3.close()
c4.close()"""
#print(clus0[0])
c0=[]
ratings.close()
try:
    with codecs.open("/run/media/bdam/USB STICK/Mini Project/Cluster0-20m.txt","r",encoding="utf8") as c20m:
        for line in c20m:
            if(line.startswith("(")):
                l=line.split(',')
                mov=l[0]
                c0.append(mov[1:len(mov)])
except UnicodeDecodeError:
    pass
clus_0_rat=open("/run/media/bdam/USB STICK/Mini Project/clus_0_rat.txt","w")
for line in ratmovie:
    l=line.split(",")
    for line1 in c0:
        if(line1==l[1]):
            #print(l[0]+""+l[1]+"    "+l[2])
            clus_0_rat.write(l[0]+"\t"+l[1]+"\t"+l[2]+"\n")
clus_0_rat.close()
