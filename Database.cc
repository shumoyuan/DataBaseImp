//
//  Database.cpp
//  COP6726_5
//
//  Created by Yihao Wu on 5/5/19.
//  Copyright © 2019 Yihao Wu. All rights reserved.
//

#include <stdio.h>
#include "Database.h"

using namespace std;

extern "C" {
    int yyparse(void);   // defined in y.tab.c
}

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

extern int sqlType; // 0 for create, 1 for insert, 2 for drop, 3 for set, 4 for select, 5 for update
extern int dbfileType; // 0 for heap file, 1 for sorted file, 2 for B plus tree file
extern string tablename;
extern string loadFileName; // save the insert file name
extern string outputFile; // save filename of output, it could be none, string or stdout
extern struct AttrList *attrList;
extern struct NameList *sortList;

void Database :: shuffleOrderHelper(vector<string> &seenTable, int index, vector<vector<string>> &res, vector<string> &tmpres) {
    if(index == seenTable.size())
    {
        res.push_back(tmpres);
        return;
    }
    for(int i = index; i < seenTable.size(); i++)
    {
        swap(seenTable[index], seenTable[i]);
        tmpres.push_back(seenTable[index]);
        shuffleOrderHelper(seenTable, index + 1, res, tmpres);
        tmpres.pop_back();
        swap(seenTable[index], seenTable[i]);
    }
}

vector<vector<string>> Database :: shuffleOrder(vector<string> &seenTable) {
    vector<vector<string>> res;
    vector<string> tmpres;
    shuffleOrderHelper(seenTable, 0, res, tmpres);
    return res;
}

void Database :: traverse(OpTreeNode *currNode, int mode) {
    if (!currNode)
        return;
    switch (currNode->getType()) {
        case SELECTFILE:
            if (mode == 0) ((SelectFileNode*)currNode)->print();
            else ((SelectFileNode*)currNode)->run();
            break;
        case SELECTPIPE:
            traverse(((SelectPipeNode*)currNode)->left, mode);
            if (mode == 0) ((SelectPipeNode*)currNode)->print();
            else ((SelectPipeNode*)currNode)->run();
            break;
        case PROJECT:
            traverse(((ProjectNode*)currNode)->left, mode);
            if (mode == 0) ((ProjectNode*)currNode)->print();
            else ((ProjectNode*)currNode)->run();
            break;
        case GROUPBY:
            traverse(((GroupByNode*)currNode)->left, mode);
            if (mode == 0) ((GroupByNode*)currNode)->print();
            else ((GroupByNode*)currNode)->run();
            break;
        case SUM:
            traverse(((SumNode*)currNode)->left, mode);
            if (mode == 0) ((SumNode*)currNode)->print();
            else ((SumNode*)currNode)->run();
            break;
        case DUPLICATEREMOVAL:
            traverse(((DuplicateRemovalNode*)currNode)->left, mode);
            if (mode == 0) ((DuplicateRemovalNode*)currNode)->print();
            else ((DuplicateRemovalNode*)currNode)->run();
            break;
        case JOIN:
            traverse(((JoinNode*)currNode)->left, mode);
            traverse(((JoinNode*)currNode)->right, mode);
            if (mode == 0) ((JoinNode*)currNode)->print();
            else ((JoinNode*)currNode)->run();
            break;
        default:
            cerr << "ERROR: Unspecified node!" << endl;
            exit(-1);
    }
    if (mode == 0) cout << endl << "*******************************************************" << endl;
}

void Database :: copySchema(unordered_map<string, Schema*> &aliasSchemas, char* oldName, char* newName) {
    
    Attribute *oldAtts = schemas[oldName]->GetAtts();
    int numAtts = schemas[oldName]->GetNumAtts();
    Attribute *newAtts = new Attribute[numAtts];
    size_t relLength = strlen(newName);
    
    for (int i = 0; i < numAtts; ++i) {
        size_t attLength = strlen(oldAtts[i].name);
        newAtts[i].name = new char[attLength + relLength + 2];
        strcpy(newAtts[i].name, newName);
        strcat(newAtts[i].name, ".");
        strcat(newAtts[i].name, oldAtts[i].name);
        newAtts[i].myType = oldAtts[i].myType;
    }
    aliasSchemas[newName] = new Schema(newName, numAtts, newAtts);
}

void Database :: selectSQL() {
    
    s.initStatistics();
    
    vector<string> seenTable;
    unordered_map<string, string> aliasName;
    unordered_map<string, Schema*> aliasSchemas;
    
    TableList *cur = tables;
    while (cur) {
        if (schemas.count(cur->tableName) == 0) {
            cerr << "Error: Table hasn't been created!" << endl;
            return;
        }
        s.CopyRel(cur->tableName, cur->aliasAs);
        copySchema(aliasSchemas, cur->tableName, cur->aliasAs);
        seenTable.push_back(cur->aliasAs);
        aliasName[cur->aliasAs] = cur->tableName;
        cur = cur->next;
    }
    
    s.Write(statistics_path.c_str());
    
    vector<vector<string>> joinOrder = shuffleOrder(seenTable);
    int indexofBestChoice = 0;
    double minRes = DBL_MAX;
    size_t numofRels = joinOrder[0].size();
    if (numofRels == 1) {
        char **relNames = new char*[1];
        relNames[0] = new char[joinOrder[0][0].size() + 1];
        strcpy(relNames[0], joinOrder[0][0].c_str());
        minRes = s.Estimate(boolean, relNames, 1);
    }
    else {
        for (int i = 0; i < joinOrder.size(); ++i) {
            s.Read(statistics_path.c_str());
            
            double result = 0;
            char **relNames = new char*[numofRels];
            for (int j = 0; j < numofRels; ++j) {
                relNames[j] = new char[joinOrder[i][j].size() + 1];
                strcpy(relNames[j], joinOrder[i][j].c_str());
            }
            
            for (int j = 2; j <= numofRels; ++j) {
                result += s.Estimate(boolean, relNames, j);
                s.Apply(boolean, relNames, j);
            }
            
            if (result < minRes) {
                minRes = result;
                indexofBestChoice = i;
            }
        }
    }
    
    vector<string> chosenJoinOrder = joinOrder[indexofBestChoice];
    
    /*
     * Generate opTree
     */
    OpTreeNode *left = new SelectFileNode(boolean, aliasSchemas[chosenJoinOrder[0]], aliasName[chosenJoinOrder[0]]);
    OpTreeNode *root = left;
    for (int i = 1; i < numofRels; ++i) {
        OpTreeNode *right = new SelectFileNode(boolean, aliasSchemas[chosenJoinOrder[i]], aliasName[chosenJoinOrder[i]]);
        root = new JoinNode(left, right, boolean);
        left = root;
    }
    if (distinctAtts == 1 || distinctFunc == 1) {
        root = new DuplicateRemovalNode(left);
        left = root;
    }
    if (groupingAtts) {
        root = new GroupByNode(left, groupingAtts, finalFunction);
        left = root;
        NameList* sum = new NameList();
        sum->name = "SUM";
        sum->next = attsToSelect;
        root = new ProjectNode(left, sum);
    }
    else if (finalFunction) {
        root = new SumNode(left, finalFunction);
        left = root;
    }
    else if (attsToSelect) {
        root = new ProjectNode(left, attsToSelect);
    }
    
    if (outputSet == 0) {
        cout << endl << "Selected Query Plan:" << endl;
        cout << endl << "*******************************************************" << endl;
        traverse(root, 0);
    }
    else if (outputSet == 1){
        traverse(root, 1);
        Record rec;
        while (root->outpipe.Remove(&rec)) {
            rec.Print(root->getSchema());
        }
    }
    else {
        traverse(root, 2);
        WriteOut wo;
        string outputPath = db_dir + outputFile;
        FILE *outFile = fopen(outputPath.c_str(), "w");
        wo.Run(root->outpipe, outFile, *root->getSchema());
        wo.WaitUntilDone();
        fclose(outFile);
    }
}

void Database :: Execute() {
    
    cout << "Please input SQL (Then press return and Control-D): " << endl;
    if (yyparse()) {
        cerr << "Error: can't parse your SQL!" << endl;
        return;
    };
    
    // cout << "Parse" << endl;
    
    switch (sqlType) {
        case 0: {
            if (tablesInDB.count(tablename) != 0) {
                cerr << "Error: Table name has already existed in current database!" << endl;
                return;
            }
            
            int numAtts = 0;
            AttrList *cur = attrList;
            while (cur) {
                ++numAtts;
                cur = cur->next;
            }
            cur = attrList;
            Attribute *atts = new Attribute[numAtts];
            for (int i = 0; i < numAtts; ++i) {
                atts[i].name = cur->name;
                switch (cur->type) {
                    case 0: {
                        atts[i].myType = Int;
                    }
                        break;
                    case 1: {
                        atts[i].myType = Double;
                    }
                        break;
                    case 2: {
                        atts[i].myType = String;
                    }
                        break;
                    default: {
                        cerr << "Error: Invalid data type!" << endl;
                    }
                }
                cur = cur->next;
            }
            Schema *mySchema = new Schema(tablename.c_str(), numAtts, atts);
            schemas[tablename] = mySchema;
            
            if (dbfileType == 0) {
                DBFile dbfile;
                string filepath = "db/" + tablename + ".bin";
                dbfile.Create(filepath.c_str(), heap, nullptr);
                dbfile.Close();
                tablesInDB[tablename] = heap;
            }
            else {
                OrderMaker myOrder;
                int numSortAtts = 0;
                NameList *tmp = sortList;
                while (tmp) {
                    myOrder.whichAtts[numSortAtts] = mySchema->Find(tmp->name);
                    myOrder.whichTypes[numSortAtts] = mySchema->FindType(tmp->name);
                    ++numSortAtts;
                    tmp = tmp->next;
                }
                myOrder.numAtts = numSortAtts;
                SortInfo sortInfo;
                sortInfo.myOrder = &myOrder;
                sortInfo.runLength = 5;
                
                DBFile dbfile;
                string filepath = "db/" + tablename + ".bin";
                dbfile.Create(filepath.c_str(), sorted, &sortInfo);
                dbfile.Close();
                tablesInDB[tablename] = sorted;
            }
            ++numofTables;
        }
            break;
        case 1: {
            if (tablesInDB.count(tablename) == 0) {
                cerr << "Error: Table hasn't been created yet!" << endl;
                return;
            }
            DBFile dbfile;
            string filepath = "db/" + tablename + ".bin";
            dbfile.Open(filepath.c_str());
            string loadPath = tpch_dir + loadFileName;
            dbfile.Load(*schemas[tablename], loadPath.c_str());
            dbfile.Close();
        }
            break;
        case 2: {
            if (tablesInDB.count(tablename) == 0) {
                cerr << "Error: Table hasn't been created yet!" << endl;
                return;
            }
            tablesInDB.erase(tablename);
            schemas.erase(tablename);
            --numofTables;
        }
            break;
        case 3: {
            if (outputFile == "NONE") {
                outputSet = 0;
            }
            else if (outputFile == "STDOUT") {
                outputSet = 1;
            }
            else {
                outputSet = 2;
            }
        }
            break;
        case 4: {
            selectSQL();
        }
            break;
        default: {
            cerr << "Error: Invalid sql type!" << endl;
        }
    }
}

void Database :: Create() {
    ofstream file(DBInfo, ios::trunc);
    file << 0;
    file.close();
    file.open(catalog_path, ios::trunc);
    file.close();
    numofTables = 0;
    tablesInDB.clear();
    schemas.clear();
}

void Database :: Open() {
    ifstream file(DBInfo);
    file >> numofTables;
    for (int i = 0; i < numofTables; ++i) {
        string name = "";
        file >> name;
        int type = 0;
        file >> type;
        tablesInDB[name] = type;
        schemas[name] = new Schema(catalog_path.c_str(), name.c_str());
    }
    file.close();
}

void Database :: Close() {
    ofstream file(DBInfo, ios::trunc);
    file << numofTables << endl;
    for (auto iter = tablesInDB.begin(); iter != tablesInDB.end(); ++iter) {
        file << iter->first << " " << iter->second << endl;
    }
    file.close();
    
    file.open(catalog_path, ios::trunc);
    for (auto iter = schemas.begin(); iter != schemas.end(); ++iter) {
        file << endl;
        file << "BEGIN" << endl;
        file << iter->first << endl;
        file << iter->first + ".tbl" << endl;
        int numAtts = iter->second->GetNumAtts();
        Attribute *atts = iter->second->GetAtts();
        for (int i = 0; i < numAtts; ++i) {
            switch (atts[i].myType) {
                case Int: {
                    file << atts[i].name << " Int" << endl;
                }
                    break;
                case Double: {
                    file << atts[i].name << " Double" << endl;
                }
                    break;
                case String: {
                    file << atts[i].name << " String" << endl;
                }
                    break;
                default: {
                    cerr << "Error: Invalid type!" << endl;
                    exit(-1);
                }
            }
        }
        file << "END" << endl;
    }
    file.close();
    
    numofTables = 0;
    tablesInDB.clear();
    schemas.clear();
}
