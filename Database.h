//
//  Database.h
//  COP6726_5
//
//  Created by Yihao Wu on 5/5/19.
//  Copyright © 2019 Yihao Wu. All rights reserved.
//

#ifndef Database_h
#define Database_h

#include <iostream>
#include <float.h>

#include "ParseTree.h"
#include "Statistics.h"
#include "Schema.h"
#include "OpTreeNode.h"
#include "string.h"

class Database {
    
private:
    const string statistics_path = "db/Statistics";
    const string DBInfo = "db/DBInfo";
    const string catalog_path = "db/catalog";
    const string db_dir = "db/";
    const string tpch_dir = "/cise/tmp/dbi_sp11/DATA/10M/";
    
    int numofTables = 0;
    unordered_map<string, int> tablesInDB;
    unordered_map<string, Schema*> schemas;
    int outputSet = 1; // 0 for NONE, 1 for STDOUT, 2 for 'myfile'
    Statistics s;
    
    void shuffleOrderHelper(vector<string> &seenTable, int index, vector<vector<string>> &res, vector<string> &tmpres);
    vector<vector<string>> shuffleOrder(vector<string> &seenTable);
    void traverse(OpTreeNode *currNode, int mode);
    void copySchema(unordered_map<string, Schema*> &aliasSchemas, char* oldName, char* newName);
    void selectSQL();
    
public:
    Database() {};
    ~Database() {};
    void Create();
    void Open();
    void Execute();
    void Close();
};

#endif /* Database_h */
