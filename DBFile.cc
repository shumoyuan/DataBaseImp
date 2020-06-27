#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

// stub file .. replace it with your own DBFile.cc

int DBFile::Create (const char *f_path, fType f_type, void *startup) {
    
    if (f_type == heap) {
        myInternalVar = new DBFile::Heap();
    }
    else if (f_type == sorted) {
        myInternalVar = new DBFile::Sorted();
    }
    
    return myInternalVar->Create(f_path, startup);
}

void DBFile::Load (Schema &f_schema, const char *loadpath) {
    myInternalVar->Load(f_schema, loadpath);
}

int DBFile::Open (const char *f_path) {
    // Firstly open .header file and read the first line to make sure whether it's a heap file or sorted file.
    string header_path = f_path;
    header_path += ".header";
    ifstream f;
    f.open(header_path);
    
    string type = "";
    f >> type;
    
    if (type == "heap") {
        myInternalVar = new Heap();
    }
    else if (type == "sorted") {
        myInternalVar = new Sorted();
    }
    f.close();
    
    return myInternalVar->Open(f_path);
}

void DBFile::MoveFirst () {
    myInternalVar->MoveFirst();
}

int DBFile::Close () {
    return myInternalVar->Close();
}

void DBFile::Add (Record &rec) {
    myInternalVar->Add(rec);
}

int DBFile::GetNext (Record &fetchme) {
    return myInternalVar->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return myInternalVar->GetNext(fetchme, cnf, literal);
}


int DBFile::Heap::Create (const char *f_path, void *startup) {
    string header_path = f_path;
    header_path += ".header";
    ofstream f(header_path);
    if (!f) return 0;
    
    f << "heap" << endl;
    f.close();
    
    // Create DBFile
    file.Open(0, f_path);
    readingPage.EmptyItOut();
    writingPage.EmptyItOut();
    file.AddPage(&writingPage, -1);
    MoveFirst();
    return 1;
}

int DBFile::Heap::Open (const char *fpath) {
    file.Open(1, fpath);  // Open existing DBFile
    readingPage.EmptyItOut();
    writingPage.EmptyItOut();
    MoveFirst();
    return 1;
}

int DBFile::Heap::Close () {
    readingMode();
    return file.Close() ? 1 : 0;
}

void DBFile::Heap::Load (Schema &myschema, const char *loadpath) {
    // Open file with R mode, and load the file into record.
    FILE *textFile = fopen(loadpath,"r");
    int numofRecords = 0;
    if(textFile == NULL){
        cout << "Can not open the file!" << endl;
        exit(1);
    }
    else{
        Record record = Record();
        while(record.SuckNextRecord(&myschema,textFile)) {
            // Get record from file.
            Add(record);
            ++numofRecords;
        }
        // Add page into file.
        
        fclose(textFile);
        // Close tht file.
        cout << "Load " << numofRecords << " records" << endl;
    }
}

void DBFile::Heap::MoveFirst () {
    readingMode();
    readingPage.EmptyItOut();
    page_index = -1;  // Move pointer to the first page.
}

void DBFile::Heap::Add (Record &addme) {
    writingMode();
    // Append record to the end of writing buffer. In case of full, write records in
    // the writing buffer to File and append record after cleaned.
    if (!writingPage.Append(&addme)) {
        file.AddPage(&writingPage, file.GetLength() - 1);
        writingPage.EmptyItOut();
        writingPage.Append(&addme);
    }
}

int DBFile::Heap::GetNext (Record &fetchme) {
    // Get record from reading buffer. In case of empty, read a new page to buffer.
    // If meets the end of file, return failure.
    while (!readingPage.GetFirst(&fetchme)) {
        if (page_index == file.GetLength() - 2) {
            return 0;
        }
        else {
            file.GetPage(&readingPage, ++page_index);
        }
    }
    return 1;
}

int DBFile::Heap::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    int fetchState = 0;
    
    while(GetNext(fetchme)) {
        if(compEng.Compare(&fetchme, &literal, &cnf)){
            // Accepted record and break while.
            fetchState = 1;
            break;
        }
    }
    
    return fetchState;
}

void DBFile::Heap::readingMode() {
    if (mode == reading) return;
    mode = reading;
    file.AddPage(&writingPage, file.GetLength() - 1);
    readingPage.EmptyItOut();
    writingPage.EmptyItOut();
}

void DBFile::Heap::writingMode() {
    if (mode == writing) return;
    mode = writing;
}



int DBFile::Sorted::Create (const char *f_path, void *startup) {
    // The first line of .header file is type of DBFile, which is "sorted" here.
    // From the second line, we write information of OrderMaker, each line is a tuple combined with whichAtts and whichTypes and they are separated by ' '.
    fpath = f_path;
    string header_path  = fpath + ".header";
    ofstream f(header_path.c_str());
    if (!f) return 0;
    
    f << "sorted" << endl;
    myOrder = *((SortInfo*)startup)->myOrder;
    for (int i = 0; i < myOrder.numAtts; ++i) {
        f << myOrder.whichAtts[i] << " " << myOrder.whichTypes[i] << endl;
    }
    f.close();
    
    // Create DBFile
    file.Open(0, f_path);
    readingPage.EmptyItOut();
    writingPage.EmptyItOut();
    file.AddPage(&writingPage, -1);  // First page of every File is empty accord to comments of File class.
    return 1;
}

void DBFile::Sorted::Load (Schema &myschema, const char *loadpath) {
    // Open file with R mode, and load the file into record.
    FILE *textFile = fopen(loadpath,"r");
    int numofRecords = 0;
    if(textFile == NULL){
        cout << "Can not open the file!" << endl;
        exit(1);
    }
    else{
        Record record = Record();
        while(record.SuckNextRecord(&myschema,textFile)) {
            // Get record from file.
            Add(record);
            ++numofRecords;
        }
        // Add page into file.
        
        fclose(textFile);
        // Close tht file.
        cout << "Load " << numofRecords << " records" << endl;
    }
}

int DBFile::Sorted::Open (const char *f_path) {
    // Just like different direction of create, we read from .header and retrieve information of OrderMaker and reconstruct it.
    fpath = f_path;
    string header_path = fpath + ".header";
    ifstream f(header_path.c_str());
    if (!f) return 0;
    
    string input = "";
    myOrder.numAtts = 0;
    getline(f, input);
    while (f >> myOrder.whichAtts[myOrder.numAtts] && !f.eof()) {
        int type = 0;
        f >> type;
        if (type == 0) myOrder.whichTypes[myOrder.numAtts] = Int;
        else if (type == 1) myOrder.whichTypes[myOrder.numAtts] = Double;
        else myOrder.whichTypes[myOrder.numAtts] = String;
        ++myOrder.numAtts;
    }
    f.close();
    file.Open(1, f_path);
    writingPage.EmptyItOut();
    readingPage.EmptyItOut();
    return 1;
}

void DBFile::Sorted::MoveFirst () {
    // cout << "MoveFirst" << endl;
    readingMode();
    readingPage.EmptyItOut();
    page_index = -1;
}

int DBFile::Sorted::Close () {
    // cout << "Close" << endl;
    readingMode();
    return file.Close() ? 1 : 0;
}

void DBFile::Sorted::Add (Record &rec) {
    // cout << "Add" << endl;
    writingMode();
    in.Insert(&rec);
}

int DBFile::Sorted::GetNext (Record &fetchme) {
    // cout << "GetNext" << endl;
    if (mode == writing) {
        readingMode();
    }
    // Get record from reading buffer. In case of empty, read a new page to buffer.
    // If meets the end of file, return failure.
    while (!readingPage.GetFirst(&fetchme)) {
        if (page_index == file.GetLength() - 2) {
            return 0;
        }
        else {
            file.GetPage(&readingPage, ++page_index);
        }
    }
    return 1;
}

int DBFile::Sorted::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    // cout << "GetNextByCNF" << endl;
    if (mode != query) {
        readingMode();
        mode = query;
        
        // Construct query OrderMaker
        delete myQueryOrder;
        delete literalQueryOrder;
        myQueryOrder = new OrderMaker();
        literalQueryOrder = new OrderMaker();
        for (int i = 0; i < myOrder.numAtts; ++i) {
            bool found = false;
            int literalIndex = 0;
            for (int j = 0; j < cnf.numAnds && !found; ++j) {
                for (int k = 0; k < cnf.orLens[j] && !found; ++k) {
                    if (cnf.orList[j][k].operand1 == Literal) {
                        if (cnf.orList[j][k].operand2 == Left && myOrder.whichAtts[i] == cnf.orList[j][k].whichAtt2 && cnf.orList[j][k].op == Equals) {
                            found = true;
                            
                            myQueryOrder->whichAtts[myQueryOrder->numAtts] = myOrder.whichAtts[i];
                            myQueryOrder->whichTypes[myQueryOrder->numAtts] = myOrder.whichTypes[i];
                            ++myQueryOrder->numAtts;
                            
                            literalQueryOrder->whichAtts[literalQueryOrder->numAtts] = literalIndex;
                            literalQueryOrder->whichTypes[literalQueryOrder->numAtts] = cnf.orList[j][k].attType;
                            ++literalQueryOrder->numAtts;
                        }
                        ++literalIndex;
                    }
                    else if (cnf.orList[j][k].operand2 == Literal) {
                        if (cnf.orList[j][k].operand1 == Left && myOrder.whichAtts[i] == cnf.orList[j][k].whichAtt1 && cnf.orList[j][k].op == Equals) {
                            found = true;
                            
                            myQueryOrder->whichAtts[myQueryOrder->numAtts] = myOrder.whichAtts[i];
                            myQueryOrder->whichTypes[myQueryOrder->numAtts] = myOrder.whichTypes[i];
                            ++myQueryOrder->numAtts;
                            
                            literalQueryOrder->whichAtts[literalQueryOrder->numAtts] = literalIndex;
                            literalQueryOrder->whichTypes[literalQueryOrder->numAtts] = cnf.orList[j][k].attType;
                            ++literalQueryOrder->numAtts;
                        }
                        ++literalIndex;
                    }
                }
            }
            if (!found) break;
        }
        
        // Binary search
        if (file.GetLength() == 1) return 0;
        else if (myQueryOrder->numAtts == 0) {
            page_index = 0;
            file.GetPage(&readingPage, 0);
        }
        else {
            off_t left = 0, right = file.GetLength() - 2;
            while (left < right - 1) {
                page_index = left + (right - left) / 2;
                file.GetPage(&readingPage, page_index);
                GetNext(fetchme);
                if (compEng.Compare(&fetchme, myQueryOrder, &literal, literalQueryOrder) == 0) {
                    right = page_index;
                }
                else if (compEng.Compare(&fetchme, myQueryOrder, &literal, literalQueryOrder) > 0) {
                    right = page_index - 1;
                }
                else {
                    left = page_index;
                }
            }
            page_index = left;
            file.GetPage(&readingPage, page_index);
            while (GetNext(fetchme)) {
                if (compEng.Compare(&fetchme, myQueryOrder, &literal, literalQueryOrder) == 0) {
                    if(compEng.Compare(&fetchme, &literal, &cnf)){
                        // Accepted record and break while.
                        return 1;
                    }
                }
                else if (compEng.Compare(&fetchme, myQueryOrder, &literal, literalQueryOrder) > 0) {
                    return 0;
                }
            }
        }
    }
    
    int fetchState = 0;
    while(GetNext(fetchme)) {
        if (myQueryOrder->numAtts > 0 && compEng.Compare(&fetchme, myQueryOrder, &literal, literalQueryOrder) != 0) {
            break;
        }
        if(compEng.Compare(&fetchme, &literal, &cnf)){
            // Accepted record and break while.
            fetchState = 1;
            break;
        }
    }
    
    return fetchState;
}

// Switch to writing mode by initilize a BigQ instance and preparing to add new record to in pipe.
void DBFile::Sorted::writingMode() {
    // cout << "writingMode" << endl;
    if (mode == writing) return;
    mode = writing;
    bigQ = new BigQ(in, out, myOrder, runLength);
}

void DBFile::Sorted::readingMode() {
    // cout << "readingMode" << endl;
    if (mode == reading) return;
    if (mode == query) {
        mode = reading;
        return;
    }
    mode = reading;
    MoveFirst();
    // Shutdown in pipe so that BigQ will start the second stage which will merge all runs into a sorded order, and we can retrieve sorted records from out pipe.
    in.ShutDown();
    File tmpFile;
    tmpFile.Open(0, "mergingFile.tmp");
    writingPage.EmptyItOut();
    tmpFile.AddPage(&writingPage, -1);
    
    bool pipeEmpty = false, fileEmpty = false; // Help determine which queue run out first.
    
    Record left;
    if (!out.Remove(&left)) pipeEmpty = true;
    
    Record right;
    if (!GetNext(right)) fileEmpty = true;
    
    // Run two way merge only if two input queues are not empty.
    while (!pipeEmpty && !fileEmpty) {
        if (compEng.Compare(&left, &right, &myOrder) <= 0) {
            // Write smaller record to writing buffer. If writing buffer if full, add this page to file and start to write to a new empty page.
            if (!writingPage.Append(&left)) {
                tmpFile.AddPage(&writingPage, tmpFile.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&left);
            }
            // If one queue is empty, end merging.
            if (!out.Remove(&left)) pipeEmpty = true;
        }
        else {
            if (!writingPage.Append(&right)) {
                tmpFile.AddPage(&writingPage, tmpFile.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&right);
            }
            if (!GetNext(right)) fileEmpty = true;
        }
    }
    // Determine which queue runs out and append records of the other queue to the end or new file.
    if (pipeEmpty && !fileEmpty) {
        if (!writingPage.Append(&right)) {
            tmpFile.AddPage(&writingPage, tmpFile.GetLength() - 1);
            writingPage.EmptyItOut();
            writingPage.Append(&right);
        }
        while (GetNext(right)) {
            if (!writingPage.Append(&right)) {
                tmpFile.AddPage(&writingPage, tmpFile.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&right);
            }
        }
    }
    else if (!pipeEmpty && fileEmpty) {
        if (!writingPage.Append(&left)) {
            tmpFile.AddPage(&writingPage, tmpFile.GetLength() - 1);
            writingPage.EmptyItOut();
            writingPage.Append(&left);
        }
        while (out.Remove(&left)) {
            if (!writingPage.Append(&left)) {
                tmpFile.AddPage(&writingPage, tmpFile.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&left);
            }
        }
    }
    // If and only if not both queues are empty, append the last probably not full page to the end of file. Because if two queues are empty, there is no need to append an empty page to the file.
    if (!pipeEmpty || !fileEmpty) tmpFile.AddPage(&writingPage, tmpFile.GetLength() - 1);
    
    tmpFile.Close();
    remove(fpath.c_str()); // Delete old DBFile
    rename("mergingFile.tmp", fpath.c_str());  // Rename tmp file to new DBFile with the same name as the old one.
    
    delete bigQ;
}
