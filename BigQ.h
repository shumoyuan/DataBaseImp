#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Comparison.h"

#include <vector>
#include <queue>
#include <algorithm>

class BigQ
{

public:

	BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
    
    static void* workerThread(void* arg);
    
private:
    
    Pipe& in;
    Pipe& out;
    OrderMaker& sortorder;
    int runlen;
    
    File file;  // Used to store sorted run.
    int run_num = 0;  // Number of runs.
    
    vector<off_t> curOffset;  // Start offset of each run.
    vector<off_t> endOffset;  // End offset of each run.
    
    pthread_t worker_thread;
    
    void sortAndSaveRun(std::vector<Record*>& sorting);  // Used for internal sorting and storing run to file
    string randomStrGen(int length);
    
    // Used for internal sort.
    class Compare
    {
        ComparisonEngine CmpEng;
        OrderMaker& CmpOrder;
        
    public:
        
        Compare(OrderMaker& sortorder): CmpOrder(sortorder) {}
        
        bool operator()(Record* a, Record* b)
        {
            return CmpEng.Compare(a, b, &CmpOrder) < 0;
        }
    };
    
    // When removing an element from priority queue, we need to know which run it belongs to.
    class SortRec
    {
        
    public:
        
        Record rec;
        int run_index;
    };
    
    // Used for priority queue.
    class SortRecCmp
    {
        ComparisonEngine CmpEng;
        OrderMaker& CmpOrder;
        
    public:
        
        SortRecCmp(OrderMaker& sortorder): CmpOrder(sortorder) {}
        
        bool operator()(SortRec* a, SortRec* b)
        {
            return CmpEng.Compare(&(a->rec), &(b->rec), &CmpOrder) > 0;
        }
    };
};


#endif
