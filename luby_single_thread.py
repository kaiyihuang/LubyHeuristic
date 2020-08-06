import time
from multiprocess import Pool
import random
import copy
from igraph import *
import pdb
import pandas
import sys, os
import timeit
from multiprocess import set_start_method




CurrGraph = Graph.Read_Ncol("C:/dev/facebook/107.edges",directed=False)

#framey = pandas.read_csv("C:/dev/facebook_clean_data/artist_edges.csv",header=None)
#CurrGraph = Graph.TupleList(framey.itertuples(index=False),directed=False,weights=False)


CurrGraph.simplify(multiple=True, loops=True, combine_edges=None)



def annihilate(WorkSet, list_of_vertices): #deletes a set of vertices and their neighbors
    if len(list_of_vertices) == 0:
        return
    checky = []
    neigh = WorkSet.neighborhood(list(list_of_vertices),order=1)
    for n in neigh:
        checky += n
    checky = checky + list(list_of_vertices)
    checky = set(checky)
    WorkSet.delete_vertices(list_of_vertices)

def heur_lubys_func(heurstic):
    TopWorkSet = CurrGraph.copy()
    AnsSet = set()

    check = []
    Z = []

    for Y in heurstic:
        Ys = TopWorkSet.vs.find(name=Y)  
        AnsSet.add(Ys['name'])
        Z.append(Ys)
    neigh = TopWorkSet.neighborhood(list(Z),order=1)
    for n in neigh:
        check += n

    try:
        TopWorkSet.delete_vertices(check)
    except:
        pass

    normy_vs = []
    normy_es = [] 
    for v in TopWorkSet.vs:
        normy_vs.append(v.index)
    for e in TopWorkSet.es:
        normy_es.append(e.index)
    #Timer Starts here
    while len(TopWorkSet.vs) > 0:
        Z = []
        check = []
        outputSety = set()
        for v in TopWorkSet.vs:
            vertex = v
            choke = TopWorkSet.degree(vertex)
            if choke == 0:
                outputSety.add(vertex.index)
                deathflag = True
            elif random.randint(0,choke) == 0:
                outputSety.add(vertex.index)

        for e in TopWorkSet.es:
            edge = e
            if edge.target in outputSety and edge.source in outputSety:
                u = TopWorkSet.degree(edge.source)
                v = TopWorkSet.degree(edge.target)
                if random.randint(1,u+v) < u:
                    outputSety.remove(edge.source)
                else:
                    outputSety.remove(edge.target)

        X = outputSety

        for Y in X:
            AnsSet.add(TopWorkSet.vs[Y]['name'])

        neigh = TopWorkSet.neighborhood(list(X),order=1)

        for n in neigh:
            check += n

        TopWorkSet.delete_vertices(check)
    return AnsSet

def heurstic_wrapper(limit):
    ans_set = set()
    #heurstic_hist = []
    max_heurstic_hist = 10
    heurstic_set = set()
    old_heurstic = set()
    if limit < 0:
        check = -1
        while check < len(ans_set):
            ans = heur_lubys_func(heurstic_set)
            check = len(ans_set)
            ans_set.add(frozenset(ans))

            if not verifyMIS(CurrGraph,ans):
                print("Invalid set detected")

            if check >= len(ans_set):
                print (len(ans_set))
                return ans_set
            new_heur = set(random.sample(ans, int(len(ans)/4)) ) #Sample a fourth of the solution set into a set entity
            

            commons = set()
            if len(old_heurstic) > 0:
                commons = old_heurstic.intersection(ans)
                commons = set(random.sample(commons, int(len(commons)/2)) )
                ans = new_heur.union(commons)
            else:
                ans = new_heur

            old_heurstic = heurstic_set
            heurstic_set = ans

    while len(ans_set) < limit:
        ans = heur_lubys_func(heurstic_set)
        ans_set.add(frozenset(ans) )
        print(len(ans))
        new_heur = set(random.sample(ans, int(len(ans)/4)) ) #Sample a fourth of the solution set into a set entity
        

        commons = set()
        if len(old_heurstic) > 0:
            commons = old_heurstic.intersection(ans)
            commons = set(random.sample(commons, int(len(commons)/2)) )
            ans = new_heur.union(commons)
        else:
            ans = new_heur
    
        old_heurstic = heurstic_set
        heurstic_set = ans
    return ans_set



def workFunc1(workobj,x,y):
    deathflag = False
    outputSety = set()
    for v in workobj.vs[x:y]:
        vertex = v
        choke = workobj.degree(vertex)
        if choke == 0:
            outputSety.add(vertex.index)
            deathflag = True
        elif random.randint(0,choke) == 0:
            outputSety.add(vertex.index)
    return outputSety

def workFunc2(outSet,workobj,x,y):
    outputSet2 = outSet.copy()
    for e in workobj.es[x:y]:
        edge = e
        if edge.target in outputSet2 and edge.source in outputSet2:
            u = workobj.degree(edge.source)
            v = workobj.degree(edge.target)
            if random.randint(1,u+v) < u:
                outputSet2.remove(edge.source)
            else:
                outputSet2.remove(edge.target)
    return outputSet2

def verifyMIS(graph, indSet):
    setcull = set()
    for v in indSet: #Verify if set is independent
        if v in setcull:
            print("Set is not independent")
            return False
        setcull.add(graph.vs.find(name=v))
        neigh = graph.neighbors(graph.vs.find(name=v))
        for n in neigh:
            setcull.add(graph.vs[n]['name'])

    if(len(graph.vs) == len(setcull)):
        return True
    else:
        return False
    

def lubys_func():
    #pool = Pool(4)
    TopWorkSet = CurrGraph.copy()
    AnsSet = set()
    normy_vs = []
    normy_es = [] 

    for v in TopWorkSet.vs:
        normy_vs.append(v.index)
    for e in TopWorkSet.es:
        normy_es.append(e.index)

    while len(TopWorkSet.vs) > 0:
        Z = []
        check = []
        outputSety = set()
        for v in TopWorkSet.vs:
            vertex = v
            choke = TopWorkSet.degree(vertex)
            if choke == 0:
                outputSety.add(vertex.index)
                deathflag = True
            elif random.randint(0,choke) == 0:
                outputSety.add(vertex.index)

        for e in TopWorkSet.es:
            edge = e
            if edge.target in outputSety and edge.source in outputSety:
                u = TopWorkSet.degree(edge.source)
                v = TopWorkSet.degree(edge.target)
                if random.randint(1,u+v) < u:
                    outputSety.remove(edge.source)
                else:
                    outputSety.remove(edge.target)

        X = outputSety
        chkset = set()
        for Y in X:
            AnsSet.add(TopWorkSet.vs[Y]['name'])
            
        neigh = TopWorkSet.neighborhood(list(X),order=1)

        for n in neigh:
            check += n

        TopWorkSet.delete_vertices(check)


        
    print (len(AnsSet))
    return AnsSet

def lubys_wrap(limit):
    ans_set = set()
    while len(ans_set) < limit:
        ans = lubys_func()
        print(verifyMIS(CurrGraph,ans))
        ans_set.add(frozenset(ans))
    return ans_set

def cellular_automata():
    global TopWorkSet 
    checkList = set()
    returnList = []
    TopWorkSet = CurrGraph.copy()
    
     
    while(TopWorkSet.vcount() > 0):
        output = []
        CellZone = dict()

        output = []
        for n in TopWorkSet.vs:
            rand = random.random()
            CellZone[n.index] = rand


        for n in TopWorkSet.vs:
            if(TopWorkSet.degree(n) == 0):
                output.append(n.index)
                continue
            l = []
            for neigh in TopWorkSet.neighbors(n):
                l.append((neigh, CellZone[neigh]))
            l.sort(key = lambda x: x[1])
            if l[0][1] > CellZone[n.index]: #You are the minimum random value generated
                output.append(n.index)

        for o in output:
            returnList.append(TopWorkSet.vs[o]['name'])

        neighborz = TopWorkSet.neighborhood(list(output),order=1) #Unfortunately, as the third step theoretically involves simultaneous mutation of an entity, it cannot be replicated here
        totes = []
        for neigh in neighborz:
            totes += neigh
        TopWorkSet.delete_vertices(totes)
    

    return returnList

def cellular_wrap(limit):
    ans_set = set()
    while len(ans_set) < limit:
        ans = cellular_automata()
        ans_set.add(frozenset(ans))
        if(len(ans_set) % 2 == 0):
            print(len(ans))

    for ans in ans_set:
        if not verifyMIS(CurrGraph,ans):
            print("Invalid set detected")
    return ans_set


if __name__ == '__main__':
    print(CurrGraph.vcount())
    #temp = heurstic_wrapper(10)

    #print(timeit.timeit('heurstic_wrapper(1)',setup='from __main__ import heurstic_wrapper',number=1))

    #print("Cellular Implementation: " + str(timeit.timeit('cellular_wrap(1)',setup='from __main__ import cellular_wrap',number=1)) )

    print( "Non Heuristic: " + str(timeit.timeit('lubys_wrap(5)',setup='from __main__ import lubys_wrap',number=1)) )
    #print(timeit.timeit('lubys_func()',setup='from __main__ import lubys_func',number=10))
    #temp = heurstic_wrapper(10)
    #print(timeit.timeit('heurstic_wrapper(10)',setup='from __main__ import heurstic_wrapper',number=10)/40)

    #Timer ends here





            #heurstic_hist.pop()









    




