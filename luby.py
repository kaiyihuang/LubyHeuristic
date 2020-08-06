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



#CurrGraph = Graph.Read_Ncol("C:/dev/facebook/107.edges",directed=False)
lock_g = Lock()
framey = pandas.read_csv("C:/dev/facebook_clean_data/new_sites_edges.csv",header=None)
CurrGraph = Graph.TupleList(framey.itertuples(index=False),directed=False,weights=False)
CurrGraph.simplify(multiple=True, loops=True, combine_edges=None)


#CellZone = []


#shm_a = shared_memory.SharedMemory(create=True, size=1073741824)
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

        X = outputSet #outputSet is a list of indices

        for Y in X:
            Ys = TopWorkSet.vs[Y]
            AnsSet.add(Ys['name'])
            Z.append(Y)
        neigh = TopWorkSet.neighborhood(list(X),order=1)

        #if(len(outputSet) == 0):
            #return AnsSet

        for n in neigh:
            check += n
        #check = check + Z
        #check = set(check)
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
    while len(ans_set) < limit:
        ans = lubys_func()
        print(verifyMIS(CurrGraph,ans))
        ans_set.add(frozenset(ans))
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
    while len(ans_set) < limit:
        #if(len(heurstic_hist) > 0):
            #heurstic_set = heurstic_hist[-1]
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

        #if not verifyMIS(CurrGraph,ans):
            #continue
    
        old_heurstic = heurstic_set
        heurstic_set = ans
    return ans_set

def init(l):
    global lock
    lock = l

def cellular_automata():
    global TopWorkSet 
    #global CellZone
    checkList = set()
    returnList = []
    TopWorkSet = CurrGraph.copy()
    #shm_a = shared_memory.SharedMemory(create=True, size=CurrGraph.vcount()*CurrGraph.vcount())
    CellZone = shm_a.buf
    #m = Manager()
    #lock = m.Lock()
    l = Lock()
    pool = Pool(initializer=init, initargs=(l,))
    with SharedMemoryManager() as smm:
        while(TopWorkSet.maxdegree() > 0):
            CellZone = ['x' * CurrGraph.vcount() * 40] * CurrGraph.vcount()
            sync_list = []
            n = TopWorkSet.vcount()
            CellZoneShared = smm.ShareableList(CellZone)
            #CellZoneShared = smm.SharedMemory(CurrGraph.vcount() * CurrGraph.vcount() * 5)
            TopWorkSetShared = ray.put(TopWorkSet)

            for v in TopWorkSet.vs: #Broadcast the number to neighbors
                sync_list.append(cell_work_1.remote(TopWorkSetShared,CellZoneShared,v.index))

            for s in sync_list:
                ray.get(s)

            sync_list.clear()

            for v in TopWorkSet.vs: #Each node fetches the top most 
                sync_list.append(cell_work_2.remote(TopWorkSetShared,CellZoneShared,v.index))

            for s in sync_list:
                checkList.add(ray.get(s))

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


@ray.remote
def cell_work_1(workset,cellzone,v): #Broadcast the number to neighbors
    anList = []
    randNum = random.randint(1,workset.vcount()**4)
    #shm_b = shared_memory.SharedMemory(name=cellzone, create=False)
    for neigh in workset.neighbors(v):
        lock.acquire()
        cellzone[neigh] = cellzone[neigh].strip('x')
        lock.release()
        checkmate = (v, randNum)
        try:
            lock.acquire()
            cellzone[neigh]  = cellzone[neigh] + ";" + str(checkmate)
            lock.release()
        except:
            print(cellzone[neigh])

@ray.remote
def cell_work_2(workset,cellzone,v):
    #shm_b = shared_memory.SharedMemory(name=cellzone, create=False)
    if workset.degree(v) != 0:
        sl = cellzone[v].split(";")
        sl2 = []
        for nuym in sl:
            if nuym != '':
                try:
                    sl2.append(ast.literal_eval(nuym))
                except:
                    print(nuym)
        sl2.sort(key = lambda x: x[1])
        winrar = sl2[-1][0]
    return winrar

def cellular_wrap(limit):
    ans_set = set()
    while len(ans_set) < limit:
        ans = cellular_automata()
        ans_set.add(frozenset(ans))
    return ans_set

    
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
        return False


def ivs_wrapper(count):
    for i in [0,1]:
        print(len(CurrGraph.maximal_independent_vertex_sets()))

if __name__ == '__main__':
    Worset = CurrGraph.copy()
    ray.init(num_cpus=8)
    print(CurrGraph.vcount())
    print( "Spinup: " + str(timeit.timeit('lubys_wrap(1)',setup='from __main__ import lubys_wrap',number=1)) ) # Always run this so Ray can initialize
    #temp = heurstic_wrapper(10)
    print( "Heuristic: " + str(timeit.timeit('heurstic_wrapper(50)',setup='from __main__ import heurstic_wrapper',number=1)) )
    print( "Non Heuristic: " + str(timeit.timeit('lubys_wrap(50)',setup='from __main__ import lubys_wrap',number=1)) )


    #print(timeit.timeit('ivs_wrapper(1) ',setup='from __main__ import ivs_wrapper',number=1))
    #print(timeit.timeit('naive_MIS()',setup='from __main__ import naive_MIS',number=1)/1)
    #print(timeit.timeit('naive_wrap(1)',setup='from __main__ import naive_wrap',number=1)/1)
    #print(timeit.timeit('cellular_wrap(1)',setup='from __main__ import cellular_wrap',number=1))

    #print(len(temp))
