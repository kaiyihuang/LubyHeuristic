import time
from multiprocess import Pool
import random
import copy
from igraph import *
import pdb
import pandas
import sys, os
import timeit
import ray
from multiprocess import set_start_method
from multiprocess import shared_memory, Lock, Manager
from multiprocess.managers import SharedMemoryManager
import ast



CurrGraph = Graph.Read_Ncol("C:/dev/facebook/107.edges",directed=False)
lock_g = Lock()


framey = pandas.read_csv("C:/dev/facebook_clean_data/new_sites_edges.csv",header=None)
CurrGraph = Graph.TupleList(framey.itertuples(index=False),directed=False,weights=False)


CurrGraph.simplify(multiple=True, loops=True, combine_edges=None)

def chunks(l, n):
    n = max(1, n)
    return (l[i:i+n] for i in range(0, len(l), n))

def naive_MIS():
    WorkSet = CurrGraph.copy()
    AnsSet = set()
    while len(WorkSet.vs) > 0:
        vertex = WorkSet.vs[random.randint(0,WorkSet.vcount()-1)]
        AnsSet.add(vertex['name'])
        neigh = WorkSet.neighborhood(vertex,order=1)
        WorkSet.delete_vertices(neigh)
    return AnsSet

def naive_wrap(limit):
    ans_set = set()
    while len(ans_set) < limit:
        ans = naive_MIS()
        verifyMIS(CurrGraph,ans)
        print(len(ans))
        ans_set.add(frozenset(ans))
    return ans_set

def annihilate(WorkSet, list_of_vertices): #deletes a set of vertices and their neighbors
    if len(list_of_vertices) == 0:
        return
    checky = []
    neigh = WorkSet.neighborhood(list(list_of_vertices),order=1)
    for n in neigh:
        checky += n
    checky = set(checky)
    WorkSet.delete_vertices(list_of_vertices)

def lubys_func():
    TopWorkSet = Graph()
    TopWorkSet = CurrGraph.copy()
    outputSet = set()
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
        TopWorkShared = ray.put(TopWorkSet)
        chunk = len(TopWorkSet.vs)
        
        c1 = workFunc1.remote(TopWorkShared,0,int(chunk/4))
        c2 = workFunc1.remote(TopWorkShared,int(chunk/4),int(2*chunk/4))
        c3 = workFunc1.remote(TopWorkShared,int(2*chunk/4),int(3*chunk/4))
        c4 = workFunc1.remote(TopWorkShared,int(3*chunk/4),int(chunk))

        r1 = ray.get(c1)
        r2 = ray.get(c2)
        r3 = ray.get(c3)
        r4 = ray.get(c4)

        outputSet = r1.union(r2,r3,r4)
        outputSetShared = ray.put(outputSet)


        chunk2 = len(TopWorkSet.es)
        c1 = workFunc2.remote(outputSetShared,TopWorkShared,0,int(chunk2/4))
        c2 = workFunc2.remote(outputSetShared,TopWorkShared,int(chunk2/4),int(2*chunk2/4))
        c3 = workFunc2.remote(outputSetShared,TopWorkShared,int(2*chunk2/4),int(3*chunk2/4))
        c4 = workFunc2.remote(outputSetShared,TopWorkShared,int(3*chunk2/4),int(chunk2))

        r1 = ray.get(c1)
        r2 = ray.get(c2)
        r3 = ray.get(c3)
        r4 = ray.get(c4)

        outputSet = r1.intersection(r2,r3,r4)

        X = outputSet #outputSet is a list of indices

        for Y in X:
            Ys = TopWorkSet.vs[Y]
            AnsSet.add(Ys['name'])
            Z.append(Y)
        neigh = TopWorkSet.neighborhood(list(X),order=1)


        for n in neigh:
            check += n

        try:
            TopWorkSet.delete_vertices(check)
        except:
            pdb.set_trace()
            continue

        outputSet = set()

    TopWorkSet = None
    print(len(AnsSet))
    return AnsSet
    #Timer ends here

def lubys_wrap(limit):
    ans_set = set()
    if limit < 0:
        check = -1
        while check < len(ans_set):
            ans = lubys_func()
            check = len(ans_set)
            ans_set.add(frozenset(ans))
            if check >= len(ans_set):
                print (len(ans_set))
                return ans_set
    while len(ans_set) < limit:
        ans = lubys_func()
        ans_set.add(frozenset(ans))
    for ans in ans_set:
        if not verifyMIS(CurrGraph,ans):
            print("Invalid set detected")
    return ans_set

def heur_lubys_func(heurstic):
    global TopWorkSet 
    global outputSet
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
        TopWorkShared = ray.put(TopWorkSet)
        chunk = len(TopWorkSet.vs)
        
        c1 = workFunc1.remote(TopWorkShared,0,int(chunk/4))
        c2 = workFunc1.remote(TopWorkShared,int(chunk/4),int(2*chunk/4))
        c3 = workFunc1.remote(TopWorkShared,int(2*chunk/4),int(3*chunk/4))
        c4 = workFunc1.remote(TopWorkShared,int(3*chunk/4),int(chunk))

        r1 = ray.get(c1)
        r2 = ray.get(c2)
        r3 = ray.get(c3)
        r4 = ray.get(c4)

        outputSet = r1.union(r2,r3,r4)
        outputSetShared = ray.put(outputSet)


        chunk2 = len(TopWorkSet.es)
        c1 = workFunc2.remote(outputSetShared,TopWorkShared,0,int(chunk2/4))
        c2 = workFunc2.remote(outputSetShared,TopWorkShared,int(chunk2/4),int(2*chunk2/4))
        c3 = workFunc2.remote(outputSetShared,TopWorkShared,int(2*chunk2/4),int(3*chunk2/4))
        c4 = workFunc2.remote(outputSetShared,TopWorkShared,int(3*chunk2/4),int(chunk2))

        r1 = ray.get(c1)
        r2 = ray.get(c2)
        r3 = ray.get(c3)
        r4 = ray.get(c4)

        outputSet = r1.intersection(r2,r3,r4)

        #print(outputSet)

        X = outputSet

        for Y in X:
            Ys = TopWorkSet.vs[Y]
            AnsSet.add(Ys['name'])
            Z.append(Ys.index)
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


    TopWorkSet = None
    
    return AnsSet

@ray.remote
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

@ray.remote
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
    for ans in ans_set:
        if not verifyMIS(CurrGraph,ans):
            print("Invalid set detected")
    return ans_set


def merge_dols(dol1, dol2, dol3, dol4):
    keys = set(dol1).union(dol2,dol3,dol4)
    no = []
    return dict((k, dol1.get(k, no) + dol2.get(k, no) + dol3.get(k, no) + dol4.get(k, no)) for k in keys)

def cellular_automata():
    global TopWorkSet 
    checkList = set()
    returnList = []
    TopWorkSet = CurrGraph.copy()
    DeadList = dict()
    iter = 0
    
    while(TopWorkSet.vcount() > 0):
        n = TopWorkSet.vcount()
        TopWorkShared = ray.put(TopWorkSet)
        chunk = len(TopWorkSet.vs)
        CellZone = dict()
        '''
        
        c1 = cell_work_1.remote(TopWorkShared,0,int(chunk/4))
        c2 = cell_work_1.remote(TopWorkShared,int(chunk/4),int(2*chunk/4))
        c3 = cell_work_1.remote(TopWorkShared,int(2*chunk/4),int(3*chunk/4))
        c4 = cell_work_1.remote(TopWorkShared,int(3*chunk/4),int(chunk))
        r1 = ray.get(c1)
        r2 = ray.get(c2)
        r3 = ray.get(c3)
        r4 = ray.get(c4)
        '''
        for n in TopWorkSet.vs:
            rand = random.random()
            CellZone[n.index] = rand #Generate a random number and store it in your own register

        #CellZone = r1.update(r2.update(r3.update(r4)))
        CellZoneShared = ray.put(CellZone)

        c1 = cell_work_2.remote(TopWorkShared,CellZoneShared,0,int(chunk/4))
        c2 = cell_work_2.remote(TopWorkShared,CellZoneShared,int(chunk/4),int(2*chunk/4))
        c3 = cell_work_2.remote(TopWorkShared,CellZoneShared,int(2*chunk/4),int(3*chunk/4))
        c4 = cell_work_2.remote(TopWorkShared,CellZoneShared,int(3*chunk/4),int(chunk))

        r1 = ray.get(c1)
        r2 = ray.get(c2)
        r3 = ray.get(c3)
        r4 = ray.get(c4)

        checkList = r1+r2+r3+r4

        tempList = []
        for j in checkList:
            tempList.append(TopWorkSet.vs[j])
        if not verifyInd(TopWorkSet,tempList):
            print(iter)
            pdb.set_trace()

        neighborz = TopWorkSet.neighborhood(list(checkList),order=1) #Unfortunately, as the third step theoretically involves simultaneous mutation of an entity, it cannot be replicated here
        totes = []

        for j in checkList:
            returnList.append( TopWorkSet.vs[j]['name'])

        for neigh in neighborz:
            totes += neigh
        totes += list(checkList)
        totes = set(totes)
        TopWorkSet.delete_vertices(totes)
        checkList = set()
        iter = iter+1
    return returnList

@ray.remote
def cell_work_1(workset,x,y): #Broadcast the number to neighbors
    output = dict()
    for n in workset.vs[x:y]:
        rand = random.random()
        output[n.index] = rand #Generate a random number and store it in your own register
    return output



@ray.remote
def cell_work_2(workset,cellzone,x,y):
    output = []
    for n in workset.vs[x:y]:
        if(workset.degree(n) == 0):
            output.append(n.index)
            continue
        l = []
        for neigh in workset.neighbors(n):
            l.append((neigh, cellzone[neigh]))
        l.sort(key = lambda x: x[1])      
        if l[0][1] > cellzone[n.index]: #You are the minimum random value generated
            output.append(n.index)
    return output

def cellular_wrap(limit):
    ans_set = set()
    while len(ans_set) < limit:
        ans = cellular_automata()
        ans_set.add(frozenset(ans))
        print(len(ans))
    for ans in ans_set:
        if not verifyMIS(CurrGraph,ans):
            print("Invalid set detected")
    return ans_set


def verifyInd(graph, indSet):
    setcull = set()
    for v in indSet: #Verify if set is independent
        if v in setcull:
            print("Set is not independent")
            return False
        setcull.add(v)
        neigh = graph.neighbors(v)
        for n in neigh:
            setcull.add(n)
    return True

def verifyMIS(graph, indSet):
    setcull = set()
    for v in indSet: #Verify if set is independent
        if v in setcull:
            print("Set is not independent")
            return False
        setcull.add(v)
        neigh = graph.neighbors(graph.vs.find(name=v))
        for n in neigh:
            setcull.add(graph.vs[n])

    if(len(graph.vs) == len(setcull)):
        return True
    else:
        return False


def ivs_wrapper(count):
    for i in [0,1]:
        print(len(CurrGraph.maximal_independent_vertex_sets()))

if __name__ == '__main__':
    Worset = CurrGraph.copy()
    ray.init(num_cpus=4)
    print("Size of graph: " + str(CurrGraph.vcount()) )
    print( "Spinup: " + str(timeit.timeit('lubys_wrap(1)',setup='from __main__ import lubys_wrap',number=1)) ) # Always run this so Ray can initialize


    #temp = heurstic_wrapper(10)
    print( "Heuristic: " + str(timeit.timeit('heurstic_wrapper(10)',setup='from __main__ import heurstic_wrapper',number=1)) )
    print( "Non Heuristic: " + str(timeit.timeit('lubys_wrap(10)',setup='from __main__ import lubys_wrap',number=1)) )


    #print(timeit.timeit('ivs_wrapper(1) ',setup='from __main__ import ivs_wrapper',number=1))
    #print(timeit.timeit('naive_MIS()',setup='from __main__ import naive_MIS',number=1)/1)
    #print(timeit.timeit('naive_wrap(1)',setup='from __main__ import naive_wrap',number=1)/1)
    print(timeit.timeit('cellular_wrap(1)',setup='from __main__ import cellular_wrap',number=1))

    #print(len(temp))
