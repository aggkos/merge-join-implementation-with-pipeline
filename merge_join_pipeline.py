def result(line1,line2,fields1,fields2):
    word_count=-1
    word2_count=-1
    fields_count=0
    fields2_count=0
    string_to_yield=""
    for word in line1.split("\t"):
        word_count+=1
        if word.startswith("tt") and word_count==0:
            string_to_yield+=word.rstrip('\n')
        if word_count in fields1:
            fields_count+=1
            string_to_yield+="\t"+word.rstrip('\n')
            if fields_count==len(fields1):
                for word2 in line2.split("\t"):
                    word2_count+=1
                    if word2_count in fields2:
                        fields2_count+=1
                        string_to_yield+="\t"+word2.rstrip('\n')+"\t"
                        if fields2_count==len(fields2):
                            return (string_to_yield)
                               
    
def id_return(line):
    word_count=-1
    for word in line.split("\t"):
            word_count+=1
            cid=""
            if word.startswith("tt") and word_count==0:
                for c in word:
                    if c!="t":
                        cid+=c
                return(int(cid))
            
def mj(f1_gen,f2_gen,fields1,fields2):
    line1=next(f1_gen)
    if (line1.startswith("tt")==False):
        line1=next(f1_gen)
    line2=next(f2_gen)
    if (line2.startswith("tt")==False):
        line2=next(f2_gen)
    id1=id_return(line1)
    id2=id_return(line2)
    buffer1=[]
    buffer2=[]
    state=0
    while(1):
        id1=id_return(line1)
        id2=id_return(line2)
        while id1>id2:
            if len(buffer2)==0:
                try:
                    line2=next(f2_gen)
                except:
                    state=1
                    break
                id2=id_return(line2)
            else:
                line2=buffer2[0]
                id2=id_return(line2)
                buffer2.pop(0)
        while id1<id2:
            if len(buffer1)==0:
                try:
                    line1=next(f1_gen)
                except:
                    state=1
                    break
                id1=id_return(line1)
            else:
                line1=buffer1[0]
                id1=id_return(line1)
                buffer1.pop(0)
        if id1==id2:
            yield(result(line1,line2,fields1,fields2))
            l2=line2
            buffer2=[]
            try:
                line2=next(f2_gen)
            #######################    
            except:
                for i in buffer1:
                    yield(result(i,line2,fields1,fields2))
                state=1
                try:
                    line1=next(f1_gen)
                    id1=id_return(line1)
                except:
                    break
                while(id1==id2):
                    yield(result(line1,line2,fields1,fields2))
                    try:
                        line1=next(f1_gen)
                        id1=id_return(line1)
                    except:
                        break
                break
            #########################
            id2_2=id_return(line2)
            buffer2.append(line2)
            while id1==id2_2:
                yield(result(line1,line2,fields1,fields2))
                try:
                    line2=next(f2_gen)
                except:
                    #################################################
                    for i in buffer1:
                        yield(result(i,line2,fields1,fields2))
                    state=1
                    try:
                        line1=next(f1_gen)
                        id1=id_return(line1)
                    except:
                        break
                    while(id1==id2):
                        yield(result(line1,line2,fields1,fields2))
                        try:
                            line1=next(f1_gen)
                            id1=id_return(line1)
                        except:
                            break
                    break
                    #######################################################
                buffer2.append(line2)
                id2_2=id_return(line2)
            buffer1=[]
            try:
                line1=next(f1_gen)
            except:
                for i in buffer2:
                    yield(result(line1,i,fields1,fields2))
                break
            id1_1=id_return(line1)
            buffer1.append(line1)
            while id1_1==id2:
                yield(result(line1,l2,fields1,fields2))
                try:
                    line1=next(f1_gen)
                except:
                    for i in buffer2:
                        yield(result(line1,i,fields1,fields2))
                    state=1
                    break
                buffer1.append(line1)
                id1_1=id_return(line1)
            line1=buffer1[0]
            buffer1.pop(0)
            line2=buffer2[0]
            buffer2.pop(0)
        if state==1:
            break


def scan(file):
    for line in file:
        yield line

f1=open("sm_basics.tsv",encoding="utf8")
f2=open("small_ratings.tsv",encoding="utf8")
f3=open("small_principals.tsv",encoding="utf8")
f = open("results.txt", "w", encoding="utf-8")
                    
                    #give:
                    #filenames to scan
                    #fields to array
                
                    #uncomment one to take the results 

#results=mj(scan(f1),scan(f2),[1,2],[1,2])
#results=mj(scan(f1),scan(f3),[2],[2])
results=mj(mj(scan(f1),scan(f3),[2],[2]),scan(f2),[1,2],[1])

for i in results:
    #print(i)
    f.write(i+"\n")

f1.close()
f2.close()
f3.close()
f.close()