#include "BigQ.h"

using namespace std;

BigQ::BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen): in(in), out(out), sortorder(sortorder), runlen(runlen)
{
    pthread_create(&worker_thread, NULL, workerThread, (void*)this);
}

void* BigQ::workerThread(void* arg)
{
    BigQ* bq = (BigQ*) arg;
    
    string tmpSortFileName = "tmp/" + bq->randomStrGen(10);
    // read data from in pipe sort them into runlen pages
    bq->file.Open(0, tmpSortFileName.c_str());  // Used to store sorted run.
    bq->file.AddPage(new Page(), -1);
    
    
    // Used to temporarily store records read from pipe and measure runlen before sorting.
    vector<Record*> sorting;  //  Used to temporarily store all records and then sort.
    int page_index = 0;  // Used to indicate which page to store next record read from pipe.
    
    Record record;  // Used to record read from pipe.
    Page page;  // Used to temporarily store records.
    
    while (bq->in.Remove(&record))
    {
        Record* rec = new Record();
        rec->Copy(&record);  // Pushed into vector to sort
        
        // Keep reading records from pipe and append to current page until current page is full.
        if (!page.Append(&record))
        {
            // If this is the last page, then start sorting process.
            if (++page_index == bq->runlen)
            {
                bq->sortAndSaveRun(sorting);
                
                // Restore default states.
                page_index = 0;
            }
            page.EmptyItOut();
            page.Append(&record);
        }
        
        sorting.emplace_back(rec);
    }
    
    // If sorting is empty, there isn't any record read from input pipe. Exit immediately
    if (sorting.empty()) {
        bq->out.ShutDown();
        bq->file.Close();
        remove(tmpSortFileName.c_str());
        pthread_exit(NULL);
    }
    // Sort last records that don't fill up a page.
    bq->sortAndSaveRun(sorting);
    
    // construct priority queue over sorted runs and dump sorted data
    // into the out pipe
    
    vector<Page> tmpPage(bq->run_num);  // Used to store current page of each run.
    priority_queue<SortRec*, vector<SortRec*>, SortRecCmp> sortPriQueue(bq->sortorder);
    
    // Get the first record of each run and push them into priority queue.
    for(int i = 0; i < bq->run_num; i++)
    {
        //Get the first page of run
        bq->file.GetPage(&tmpPage[i], bq->curOffset[i]++);
        //Get the first record of page
        SortRec* sortRec = new SortRec();
        sortRec->run_index = i;
        tmpPage[i].GetFirst(&(sortRec->rec));
        sortPriQueue.emplace(sortRec);
    }
    
    // pop top element from priority queue and insert it into output pipe. Take record from the same run as popped element
    while(!sortPriQueue.empty())
    {
        //Read first record of page and insert
        SortRec* sortRec = sortPriQueue.top();
        sortPriQueue.pop();
        int run_index = sortRec->run_index;
        bq->out.Insert(&(sortRec->rec));
        
        //Read other records from page
        if(tmpPage[run_index].GetFirst(&(sortRec->rec)))
        {
            sortPriQueue.emplace(sortRec);
        }
        else if(bq->curOffset[run_index] < bq->endOffset[run_index])
        {
            //Finsh this page, Get record from another page
            bq->file.GetPage(&tmpPage[run_index], bq->curOffset[run_index]++);
            tmpPage[run_index].GetFirst(&(sortRec->rec));
            sortPriQueue.emplace(sortRec);
        }
        else
        {
            delete sortRec;
        }
    }
    
    // finally shut down the out pipe
    bq->out.ShutDown();
    bq->file.Close();
    remove(tmpSortFileName.c_str());
    pthread_exit(NULL);
}

void BigQ::sortAndSaveRun(vector<Record*> &sorting)
{
    sort(sorting.begin(), sorting.end(), Compare(sortorder));
    
    Page outPage;
    curOffset.push_back(file.GetLength() - 1);  // Offset of a new page in file is the start offset of next run.
    for (auto rec : sorting)
    {
        if (!outPage.Append(rec))
        {
            file.AddPage(&outPage, file.GetLength() - 1);
            outPage.EmptyItOut();
            outPage.Append(rec);
            delete rec;
            rec = nullptr;
        }
    }
    file.AddPage(&outPage, file.GetLength() - 1);
    endOffset.push_back(file.GetLength() - 1);  // Offset of a new page in file is the end offset of just writen run.
    ++run_num;
    sorting.clear();
}

string BigQ::randomStrGen(int length)
{
    static string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    string result;
    result.resize(length);
    
    for (int i = 0; i < length; i++) {
        result[i] = charset[rand() % charset.length()];
    }
    
    return result;
}
