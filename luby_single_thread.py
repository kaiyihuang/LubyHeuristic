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




#CurrGraph = Graph.Read_Ncol("C:/dev/facebook/107.edges",directed=False)

framey = pandas.read_csv("C:/dev/facebook_clean_data/artist_edges.csv",header=None)
CurrGraph = Graph.TupleList(framey.itertuples(index=False),directed=False,weights=False)
CurrGraph.simplify(multiple=True, loops=True, combine_edges=None)
TopWorkSet = Graph()
TopWorkSet = CurrGraph.copy()
outputSet = set()
CellZone = []


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
    global TopWorkSet 
    global outputSet
    TopWorkSet = CurrGraph.copy()
    AnsSet = set()

    for v in heurstic:
        AnsSet.add(v)

    annihilate(TopWorkSet,heurstic)

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
        #pool.map(workFunc1,normy_vs)
        for v in normy_vs:
            workFunc1(v)
        for e in normy_es:
            workFunc2(e)
       # pool.map(workFunc2,normy_es)
        X = outputSet

        for Y in X:
            AnsSet.add(Y['name'])
            Z.append(Y.index)
        neigh = TopWorkSet.neighborhood(list(X),order=1)

        for n in neigh:
            check += n
        check = check + Z
        check = set(check)
        try:
            TopWorkSet.delete_vertices(check)
        except:
            pdb.set_trace()
            continue

        outputSet = set()

        normy_vs = []
        normy_es = [] 
        for v in TopWorkSet.vs:
            normy_vs.append(v.index)
        for e in TopWorkSet.es:
            normy_es.append(e.index)
    TopWorkSet = None
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
        print(verifyMIS(CurrGraph,ans))
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



def workFunc1(v):
    #for v in inSet:
    global outputSet
    global TopWorkSet 
    vertex = TopWorkSet.vs[v]
    choke = TopWorkSet.degree(vertex)

    if choke == 0:
        outputSet.add(vertex)
    elif random.randint(0,choke) == 0:
        outputSet.add(vertex)
def workFunc2(e):
    #for e in edgeSet:
    global outputSet
    global TopWorkSet 
    edge =  TopWorkSet.es[e]
    if edge.target in outputSet and edge.source in outputSet:
        u = TopWorkSet.degree(edge.source)
        v = TopWorkSet.degree(edge.target)
        if random.randint(1,u+v) < u:
            outputSet.remove(u)
        else:
            outputSet.remove(v)

def verifyMIS(graph, indSet):
    setcull = set()
    for v in indSet: #Verify if set is independent
        if v in setcull:
            print("Set is not independent")
            return False
        setcull.add(v)
        neigh = graph.neighbors(v)
        for n in neigh:
            if(n in indSet):
                print("Something's gone horribly wrong")
            setcull.add(n)

    if(len(graph.vs) == len(setcull)):
        return True
    else:
        print("Set is not maximal")
        return False
def lubys_func():
    #pool = Pool(4)
    global outputSet
    global TopWorkSet 
    TopWorkSet = CurrGraph.copy()
    AnsSet = set()
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
        #pool.map(workFunc1,normy_vs)
        for v in normy_vs:
            workFunc1(v)
        for e in normy_es:
            workFunc2(e)
        #pool.map(workFunc2,normy_es)
        X = outputSet

        for Y in X:
            AnsSet.add(Y['name'])
            Z.append(Y.index)
        neigh = TopWorkSet.neighborhood(list(X),order=1)

        for n in neigh:
            check += n
        check = check + Z
        check = set(check)
        try:
            TopWorkSet.delete_vertices(check)
        except:
            pdb.set_trace()
            continue

        outputSet = set()

        normy_vs = []
        normy_es = [] 
        for v in TopWorkSet.vs:
            normy_vs.append(v.index)
        for e in TopWorkSet.es:
            normy_es.append(e.index)
    TopWorkSet = None
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
    global CellZone
    checkList = set()
    returnList = []
    TopWorkSet = CurrGraph.copy()
    
    while(TopWorkSet.maxdegree() > 0):
        CellZone = [[] for i in range(TopWorkSet.vcount())]
        n = TopWorkSet.vcount()
        for v in TopWorkSet.vs:
            randNum = random.randint(1,TopWorkSet.vcount()**4)
            for neigh in TopWorkSet.neighbors(v):
                CellZone[neigh].append((v.index, randNum))
        for v in TopWorkSet.vs:
            if TopWorkSet.degree(v) != 0:
                CellZone[v.index].sort(key = lambda x: x[1])
                winrar = CellZone[v.index][-1][0]
                checkList.add(winrar)
                CellZone[v.index] = []

        neighborz = TopWorkSet.neighborhood(list(checkList),order=1)
        totes = []
        returnList += list(checkList)
        for neigh in neighborz:
            totes += neigh
        totes += list(checkList)
        totes = set(totes)
        TopWorkSet.delete_vertices(totes)
        checkList = set()
    return returnList

def cellular_wrap(limit):
    ans_set = set()
    while len(ans_set) < limit:
        ans = cellular_automata()
        ans_set.add(frozenset(ans))
        if(len(ans_set) % 2 == 0):
            print(len(ans))
    return ans_set


if __name__ == '__main__':
    print(CurrGraph.vcount())
    #temp = heurstic_wrapper(10)

    #print(timeit.timeit('heurstic_wrapper(10)',setup='from __main__ import heurstic_wrapper',number=1))

    #print("Cellular Implementation: " + str(timeit.timeit('cellular_wrap(10)',setup='from __main__ import cellular_wrap',number=1)) )

    print( "Non Heuristic: " + str(timeit.timeit('lubys_wrap(5)',setup='from __main__ import lubys_wrap',number=1)) )
    #print(timeit.timeit('lubys_func()',setup='from __main__ import lubys_func',number=10))
    #temp = heurstic_wrapper(10)
    #print(timeit.timeit('heurstic_wrapper(10)',setup='from __main__ import heurstic_wrapper',number=10)/40)

    #Timer ends here





            #heurstic_hist.pop()









    




