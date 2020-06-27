#include "RelOp.h"

void SelectFile::Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    this->inFile = &inFile;
    this->outPipe = &outPipe;
    this->selOp = &selOp;
    this->literal = &literal;
    
	pthread_create(&worker, NULL, workerFun, (void*)this);
}

void *SelectFile::workerFun(void* arg) {
    SelectFile* sf = (SelectFile*)arg;
	Record tmpRec;
	ComparisonEngine comEng;

	while (sf->inFile->GetNext(tmpRec)) {
		if (comEng.Compare(&tmpRec, sf->literal, sf->selOp)) {
			sf->outPipe->Insert(&tmpRec);
		}
	}
	sf->outPipe->ShutDown();
    sf->inFile->Close();
    pthread_exit(NULL);
}

void SelectFile::WaitUntilDone () {
	pthread_join (worker, NULL);
}

void SelectFile::Use_n_Pages(int runlen) {
	this->n_pages = runlen;
}

void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	pthread_create(&worker, NULL, workerFun, (void*)this);
}

void *SelectPipe::workerFun(void *arg) {
    SelectPipe* sp = (SelectPipe*)arg;
	Record tmpRec;
	ComparisonEngine comEng;

	while (sp->inPipe->Remove(&tmpRec)) {
		if (comEng.Compare(&tmpRec, sp->literal, sp->selOp)) {
			sp->outPipe->Insert(&tmpRec);
		}
	}
	sp->outPipe->ShutDown();
    pthread_exit(NULL);
}


void SelectPipe::WaitUntilDone() {
	pthread_join(worker, NULL);
}

void SelectPipe::Use_n_Pages(int runlen) {
	this->n_pages = runlen;
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->keepMe = keepMe;
	this->numAttsInput = numAttsInput;
	this->numAttsOutput = numAttsOutput;
	pthread_create(&worker, NULL, workerFun, (void*)this);
}

void *Project::workerFun(void *arg) {
	Project* p = (Project*)arg;
	Record tmpRec;

	while (p->inPipe->Remove(&tmpRec)) {
		tmpRec.Project(p->keepMe, p->numAttsOutput, p->numAttsInput);
		p->outPipe->Insert(&tmpRec);
	}
    
	p->outPipe->ShutDown();
    pthread_exit(NULL);
}

void Project::WaitUntilDone() {
	pthread_join(worker, NULL);
}

void Project::Use_n_Pages(int runlen) {
	this->n_pages = runlen;
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
	this->inPipeL = &inPipeL;
	this->inPipeR = &inPipeR;
	this->outPipe = &outPipe;
	this->selOp = &selOp;
	this->literal = &literal;
	pthread_create(&worker, NULL, workerFun, this);
}

void *Join::workerFun(void *arg) {
	Join* j = (Join*)arg;
    ComparisonEngine cmpEng;
	Record *tmpRecL = new Record(), *tmpRecR = new Record();
    vector<Record*> leftEq, rightEq;
    OrderMaker leftOrder, rightOrder;
    
    if (j->selOp->GetSortOrders(leftOrder, rightOrder)) {
        
        int numAttsLeft = 0, numAttsRight = 0, numAttsToKeep = 0;
        int* attsToKeep = nullptr;
        Pipe outSortL(200), outSortR(200);
        BigQ leftBQ = BigQ(*(j->inPipeL), outSortL, leftOrder, j->n_pages);
        BigQ rightBQ = BigQ(*(j->inPipeR), outSortR, rightOrder, j->n_pages);
        bool leftEmpty = false, rightEmpty = false;
        
        if (!outSortL.Remove(tmpRecL)) {
            j->outPipe->ShutDown();
            pthread_exit(NULL);
        }
        else {
            numAttsLeft = ((int *) tmpRecL->bits)[1] / sizeof(int) - 1;\
        }
        
        if (!outSortR.Remove(tmpRecR)) {
            j->outPipe->ShutDown();
            pthread_exit(NULL);
        }
        else {
            numAttsRight = ((int *) tmpRecR->bits)[1] / sizeof(int) - 1;
        }
        
        numAttsToKeep = numAttsLeft + numAttsRight;
        attsToKeep = new int[numAttsToKeep];
        for (int i = 0; i < numAttsLeft; ++i) {
            attsToKeep[i] = i;
        }
        for (int i = numAttsLeft; i < numAttsToKeep; ++i) {
            attsToKeep[i] = i - numAttsLeft;
        }
        
        leftEq.emplace_back(tmpRecL);
        tmpRecL = new Record();
        rightEq.emplace_back(tmpRecR);
        tmpRecR = new Record();
        
        if (!outSortL.Remove(tmpRecL)) {
            leftEmpty = true;
        }
        if (!outSortR.Remove(tmpRecR)) {
            rightEmpty = true;
        }
        
        bool leftBigger = false, rightBigger = false;
        
        while (!leftEmpty && !rightEmpty) {
            
            if (!leftBigger) {
                while (!cmpEng.Compare(leftEq.back(), tmpRecL, &leftOrder)) {
                    leftEq.emplace_back(tmpRecL);
                    tmpRecL = new Record();
                    if (!outSortL.Remove(tmpRecL)) {
                        leftEmpty = true;
                        break;
                    }
                }
            }
            
            if (!rightBigger) {
                while (!cmpEng.Compare(rightEq.back(), tmpRecR, &rightOrder)) {
                    rightEq.emplace_back(tmpRecR);
                    tmpRecR = new Record();
                    if (!outSortR.Remove(tmpRecR)) {
                        rightEmpty = true;
                        break;
                    }
                }
            }
            
            if (cmpEng.Compare(leftEq.back(), &leftOrder, rightEq.back(), &rightOrder) > 0) {
                j->vectorCleanup(rightEq);
                leftBigger = true;
                rightBigger = false;
            }
            else if (cmpEng.Compare(leftEq.back(), &leftOrder, rightEq.back(), &rightOrder) < 0) {
                j->vectorCleanup(leftEq);
                leftBigger = false;
                rightBigger = true;
            }
            else {
                for (auto recL : leftEq) {
                    for (auto recR : rightEq) {
                        if (cmpEng.Compare(recL, recR, j->literal, j->selOp)) {
                            Record joinedRec, cpRecR;
                            cpRecR.Copy(recR);
                            joinedRec.MergeRecords(recL, &cpRecR, numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, numAttsLeft);
                            j->outPipe->Insert(&joinedRec);
                        }
                    }
                }
                
                j->vectorCleanup(leftEq);
                j->vectorCleanup(rightEq);
                leftBigger = false;
                rightBigger = false;
            }
            
            if (!leftEmpty && !leftBigger) {
                leftEq.emplace_back(tmpRecL);
                tmpRecL = new Record();
                if (!outSortL.Remove(tmpRecL)) {
                    leftEmpty = true;
                }
            }
            if (!rightEmpty && !rightBigger) {
                rightEq.emplace_back(tmpRecR);
                tmpRecR = new Record();
                if (!outSortR.Remove(tmpRecR)) {
                    rightEmpty = true;
                }
            }
        }
        
        if (!leftEq.empty() && !rightEq.empty()) {
            while (!leftEmpty) {
                if (!leftBigger) {
                    while (!cmpEng.Compare(leftEq.back(), tmpRecL, &leftOrder)) {
                        leftEq.emplace_back(tmpRecL);
                        tmpRecL = new Record();
                        if (!outSortL.Remove(tmpRecL)) {
                            leftEmpty = true;
                            break;
                        }
                    }
                }
                if (cmpEng.Compare(leftEq.back(), &leftOrder, rightEq.back(), &rightOrder) > 0) {
                    j->vectorCleanup(rightEq);
                    leftBigger = true;
                    rightBigger = false;
                    break;
                }
                else if (cmpEng.Compare(leftEq.back(), &leftOrder, rightEq.back(), &rightOrder) < 0) {
                    j->vectorCleanup(leftEq);
                    leftBigger = false;
                    rightBigger = true;
                }
                else {
                    for (auto recL : leftEq) {
                        for (auto recR : rightEq) {
                            if (cmpEng.Compare(recL, recR, j->literal, j->selOp)) {
                                Record joinedRec, cpRecR;
                                cpRecR.Copy(recR);
                                joinedRec.MergeRecords(recL, &cpRecR, numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, numAttsLeft);
                                j->outPipe->Insert(&joinedRec);
                            }
                        }
                    }
                    
                    j->vectorCleanup(leftEq);
                    j->vectorCleanup(rightEq);
                    leftBigger = false;
                    rightBigger = false;
                    break;
                }
                
                if (!leftEmpty && !leftBigger) {
                    leftEq.emplace_back(tmpRecL);
                    tmpRecL = new Record();
                    if (!outSortL.Remove(tmpRecL)) {
                        leftEmpty = true;
                    }
                }
            }
            while (!rightEmpty) {
                if (!rightBigger) {
                    while (!cmpEng.Compare(rightEq.back(), tmpRecR, &rightOrder)) {
                        rightEq.emplace_back(tmpRecR);
                        tmpRecR = new Record();
                        if (!outSortR.Remove(tmpRecR)) {
                            rightEmpty = true;
                            break;
                        }
                    }
                }
                if (cmpEng.Compare(leftEq.back(), &leftOrder, rightEq.back(), &rightOrder) > 0) {
                    j->vectorCleanup(rightEq);
                    leftBigger = true;
                    rightBigger = false;
                }
                else if (cmpEng.Compare(leftEq.back(), &leftOrder, rightEq.back(), &rightOrder) < 0) {
                    j->vectorCleanup(leftEq);
                    leftBigger = false;
                    rightBigger = true;
                    break;
                }
                else {
                    for (auto recL : leftEq) {
                        for (auto recR : rightEq) {
                            if (cmpEng.Compare(recL, recR, j->literal, j->selOp)) {
                                Record joinedRec, cpRecR;
                                cpRecR.Copy(recR);
                                joinedRec.MergeRecords(recL, &cpRecR, numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, numAttsLeft);
                                j->outPipe->Insert(&joinedRec);
                            }
                        }
                    }
                    
                    j->vectorCleanup(leftEq);
                    j->vectorCleanup(rightEq);
                    leftBigger = false;
                    rightBigger = false;
                    break;
                }
                
                if (!rightEmpty && !rightBigger) {
                    rightEq.emplace_back(tmpRecR);
                    tmpRecR = new Record();
                    if (!outSortR.Remove(tmpRecR)) {
                        rightEmpty = true;
                    }
                }
            }
        }
    }
    else {
        int numAttsLeft = 0, numAttsRight = 0, numAttsToKeep = 0;
        int* attsToKeep = nullptr;
        
        if (!j->inPipeL->Remove(tmpRecL)) {
            j->outPipe->ShutDown();
            pthread_exit(NULL);
        }
        else {
            numAttsLeft = *((int *) tmpRecL->bits);
            leftEq.emplace_back(tmpRecL);
            tmpRecL = new Record();
        }
        
        if (!j->inPipeR->Remove(tmpRecR)) {
            j->outPipe->ShutDown();
            pthread_exit(NULL);
        }
        else {
            numAttsRight = *((int *) tmpRecR->bits);
            rightEq.emplace_back(tmpRecR);
            tmpRecR = new Record();
        }
        
        numAttsToKeep = numAttsLeft + numAttsRight;
        attsToKeep = new int[numAttsToKeep];
        
        for (int i = 0; i < numAttsLeft; ++i) {
            attsToKeep[i] = i;
        }
        for (int i = numAttsLeft; i < numAttsToKeep; ++i) {
            attsToKeep[i] = i - numAttsLeft;
        }
        
        while (j->inPipeL->Remove(tmpRecL)) {
            leftEq.emplace_back(tmpRecL);
            tmpRecL = new Record();
        }
        while (j->inPipeR->Remove(tmpRecR)) {
            rightEq.emplace_back(tmpRecR);
            tmpRecR = new Record();
        }
        for (auto recL : leftEq) {
            for (auto recR : rightEq) {
                if (cmpEng.Compare(recL, recR, j->literal, j->selOp)) {
                    Record joinedRec;
                    joinedRec.MergeRecords(tmpRecL, tmpRecR, numAttsLeft, numAttsRight, attsToKeep, numAttsToKeep, numAttsLeft);
                    j->outPipe->Insert(&joinedRec);
                }
            }
        }
    }
    
    j->vectorCleanup(leftEq);
    j->vectorCleanup(rightEq);
    delete tmpRecL;
    tmpRecL = nullptr;
    delete tmpRecR;
    tmpRecR = nullptr;
	
	j->outPipe->ShutDown();
    pthread_exit(NULL);
}

void Join::WaitUntilDone() {
	pthread_join(worker, NULL);
}

void Join::Use_n_Pages(int runlen) {
	this->n_pages = runlen;
}

void Join::vectorCleanup(vector<Record*>& v){
    for (auto r : v) {
        delete r;
        r = nullptr;
    }
    v.clear();
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->mySchema = &mySchema;
	pthread_create(&worker, NULL, workerFun, this);
}

void *DuplicateRemoval::workerFun(void *arg) {
    DuplicateRemoval* dr = (DuplicateRemoval*)arg;
    OrderMaker order = OrderMaker(dr->mySchema);
    ComparisonEngine cmpEng;
    Record tmpRec;
    Record nextRec;
    
    Pipe pipe(200);
    BigQ bigQ = BigQ(*(dr->inPipe), pipe, order, dr->n_pages);
    
    
    if (pipe.Remove(&tmpRec)) {
        while (pipe.Remove(&nextRec)) {
            if (cmpEng.Compare(&tmpRec, &nextRec, &order)) {
                dr->outPipe->Insert(&tmpRec);
                tmpRec.Consume(&nextRec);
            }
        }
        dr->outPipe->Insert(&tmpRec);
    }
    
    dr->outPipe->ShutDown();
    pthread_exit(NULL);
}

void DuplicateRemoval::WaitUntilDone() {
	pthread_join(worker, NULL);
}

void DuplicateRemoval::Use_n_Pages(int runlen) {
	this->n_pages = runlen;
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->computeMe = &computeMe;
	pthread_create(&worker, NULL, workerFun, this);
}

void *Sum::workerFun(void *arg) {
    Sum *s = (Sum*)arg;
    Record outRec, tmpRec;
    int intSum = 0;
    int intVal = 0;
    double doubleSum = 0.0;
    double doubleVal = 0.0;
    
    Attribute attr;
    attr.name = "SUM";
    stringstream output;
    
    if (s->computeMe->returnsInt == 1) {
        Type valType = Int;
        while (s->inPipe->Remove(&tmpRec)) {
            valType = s->computeMe->Apply(tmpRec, intVal, doubleVal);
            intSum = intSum + intVal;
        }
        output << intSum << "|";
    }
    else if (s->computeMe->returnsInt == 0) {
        Type valType = Double;
        while (s->inPipe->Remove(&tmpRec)) {
            valType = s->computeMe->Apply(tmpRec, intVal, doubleVal);
            doubleSum = doubleSum + doubleVal;
        }
        attr.myType = Double;
        output << doubleSum << "|";
    }
    else {
        cerr << "Error: Invalid type in Sum operation." << endl;
        exit(1);
    }
    
    Schema outSch("out_shema", 1, &attr);
    outRec.ComposeRecord(&outSch, output.str().c_str());
    s->outPipe->Insert(&outRec);
    
    s->outPipe->ShutDown();
    pthread_exit(NULL);
}

void Sum::WaitUntilDone() {
	pthread_join(worker, NULL);
}

void Sum::Use_n_Pages(int runlen) {
	this->n_pages = runlen;
}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
	this->inPipe = &inPipe;
	this->outPipe = &outPipe;
	this->groupAtts = &groupAtts;
	this->computeMe = &computeMe;
	pthread_create(&worker, NULL, workerFun, this);
}

void *GroupBy::workerFun(void *arg) {
    GroupBy* gb = (GroupBy*)arg;
    ComparisonEngine cmpEng;
    Record outRec, tmpRec, lastRec;
    int intSum = 0;
    int intVal = 0;
    double doubleSum = 0.0;
    double doubleVal = 0.0;
    Type valType = String;
    
    Pipe outSort(100);
    BigQ bq = BigQ(*(gb->inPipe), outSort, *(gb->groupAtts), gb->n_pages);
    
    if (!outSort.Remove(&tmpRec)) {
        gb->outPipe->ShutDown();
        pthread_exit(NULL);
    }
    
    valType = gb->computeMe->Apply(tmpRec, intVal, doubleVal);
    if (valType == Int) {
        intSum = intSum + intVal;
    }
    else if (valType == Double) {
        doubleSum = doubleSum + doubleVal;
    }
    lastRec.Consume(&tmpRec);
    
    while (outSort.Remove(&tmpRec)) {
        if (cmpEng.Compare(&lastRec, &tmpRec, gb->groupAtts)) {
            Attribute* attrs = new Attribute[gb->groupAtts->numAtts + 1];
            attrs[0].name = "SUM";
            stringstream output;
            if (valType == Int) {
                attrs[0].myType = Int;
                output << intSum << "|";
            }
            else if (valType == Double) {
                attrs[0].myType = Double;
                output << doubleSum << "|";
            }
            
            for (int i = 0; i < gb->groupAtts->numAtts; ++i) {
                Type curAttType = gb->groupAtts->whichTypes[i];
                if (curAttType == Int) {
                    attrs[i + 1].name = "int";
                    attrs[i + 1].myType = Int;
                    int val = *((int*)(lastRec.bits + ((int *) lastRec.bits)[gb->groupAtts->whichAtts[i] + 1]));
                    output << val << "|";
                }
                else if (curAttType == Double) {
                    attrs[i + 1].name = "double";
                    attrs[i + 1].myType = Double;
                    double val = *((double*)(lastRec.bits + ((int *) lastRec.bits)[gb->groupAtts->whichAtts[i] + 1]));
                    output << val << "|";
                }
                else {
                    attrs[i + 1].name = "string";
                    attrs[i + 1].myType = String;
                    string val = lastRec.bits + ((int *) lastRec.bits)[gb->groupAtts->whichAtts[i] + 1];
                    output << val << "|";
                }
            }
            
            Schema outSch("out_shema", gb->groupAtts->numAtts + 1, attrs);
            outRec.ComposeRecord(&outSch, output.str().c_str());
            gb->outPipe->Insert(&outRec);
            
            intSum = 0;
            intVal = 0;
            doubleSum = 0.0;
            doubleVal = 0.0;
        }
        
        valType = gb->computeMe->Apply(tmpRec, intVal, doubleVal);
        if (valType == Int) {
            intSum = intSum + intVal;
        }
        else if (valType == Double) {
            doubleSum = doubleSum + doubleVal;
        }
        lastRec.Consume(&tmpRec);
    }
    
    Attribute* attrs = new Attribute[gb->groupAtts->numAtts + 1];
    attrs[0].name = "SUM";
    stringstream output;
    if (valType == Int) {
        attrs[0].myType = Int;
        output << intSum << "|";
    }
    else if (valType == Double) {
        attrs[0].myType = Double;
        output << doubleSum << "|";
    }
    
    for (int i = 0; i < gb->groupAtts->numAtts; ++i) {
        Type curAttType = gb->groupAtts->whichTypes[i];
        if (curAttType == Int) {
            attrs[i + 1].name = "int";
            attrs[i + 1].myType = Int;
            int val = *((int*)(lastRec.bits + ((int *) lastRec.bits)[gb->groupAtts->whichAtts[i] + 1]));
            cout << "[i]: " << val << "|";
            output << val << "|";
        }
        else if (curAttType == Double) {
            attrs[i + 1].name = "double";
            attrs[i + 1].myType = Double;
            double val = *((double*)(lastRec.bits + ((int *) lastRec.bits)[gb->groupAtts->whichAtts[i] + 1]));
            cout << "[i]: " << val << "|";
            output << val << "|";
        }
        else {
            attrs[i + 1].name = "string";
            attrs[i + 1].myType = String;
            string val = lastRec.bits + ((int *) lastRec.bits)[gb->groupAtts->whichAtts[i] + 1];
            cout << "[i]: " << val << "|";
            output << val << "|";
        }
    }
    
    Schema outSch("out_shema", gb->groupAtts->numAtts + 1, attrs);
    outRec.ComposeRecord(&outSch, output.str().c_str());
    gb->outPipe->Insert(&outRec);
    
    gb->outPipe->ShutDown();
    pthread_exit(NULL);
}

void GroupBy::WaitUntilDone() {
	pthread_join(worker, NULL);
}

void GroupBy::Use_n_Pages(int runlen) {
	this->n_pages = runlen;
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
	this->inPipe = &inPipe;
	this->outFile = outFile;
	this->mySchema = &mySchema;
	pthread_create(&worker, NULL, workerFun, this);
}

void *WriteOut::workerFun(void *arg) {
    WriteOut *wo = (WriteOut*)arg;
    Record tmpRec;
    
    while (wo->inPipe->Remove(&tmpRec)) {
        wo->writeOut(tmpRec);
    }
    pthread_exit(NULL);
}

void WriteOut::writeOut(Record &rec) {
    //Add: from print
    
    int numAtts = mySchema->GetNumAtts();
    Attribute *atts = mySchema->GetAtts();
    
    // loop through all of the attributes
    for (int i = 0; i < numAtts; i++) {
        
        int pointer = ((int *)rec.bits)[i + 1];
        
        // first is integer
        if (atts[i].myType == Int) {
            int *myInt = (int *) &(rec.bits[pointer]);
            fprintf(outFile, "%d", *myInt);
            
            // then is a double
        }
        else if (atts[i].myType == Double) {
            double *myDouble = (double *) &(rec.bits[pointer]);
            fprintf(outFile, "%f", *myDouble);
            
            // then is a character string
        }
        else if (atts[i].myType == String) {
            char *myString = (char *) &(rec.bits[pointer]);
            fprintf(outFile, "%s", myString);
        }
        fprintf(outFile, "|");
    }
    
    fprintf(outFile, "\n");
}

void WriteOut::WaitUntilDone() {
	pthread_join(worker, NULL);
}

void WriteOut::Use_n_Pages(int runlen) {
	this->n_pages = runlen;
}
