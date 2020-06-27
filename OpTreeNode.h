#ifndef NODE_H
#define NODE_H

#include <iostream>
#include <string>
#include <vector>

#include <stdio.h>
#include "ParseTree.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Schema.h"
#include "Function.h"
#include "Pipe.h"
#include "RelOp.h"

enum NodeType {BASE, SELECTFILE, SELECTPIPE, PROJECT, JOIN, GROUPBY, DUPLICATEREMOVAL, SUM};

static int PIPEID = 0;

class OpTreeNode {
protected:
	int pipeID = PIPEID++;
    NodeType myType = BASE;
    Schema *outputSchema = nullptr;
    
public:
    OpTreeNode *left = nullptr, *right = nullptr;
    Pipe outpipe = Pipe(50);
    
    virtual void run() = 0;
    virtual void print() = 0;
    
    int getPipeID() { return pipeID; };
	Schema* getSchema() { return outputSchema; };
	NodeType getType() { return myType; }
};

class SelectFileNode : public OpTreeNode {
private:
    string dbfilePath;
    DBFile dbfile;
    CNF cnf;
    Record literal;
    SelectFile sf;

public:
    
	SelectFileNode(AndList *selectList, Schema *schema, string relName);
    
    void run();

	void print();
};

class SelectPipeNode : public OpTreeNode {
private:
    CNF cnf;
    Record literal;
    SelectPipe sp;

public:
    
	SelectPipeNode(OpTreeNode *child, AndList *selectList);
    
    void run();

	void print();
};

class ProjectNode : public OpTreeNode {
private:
    int *keepMe;
    NameList *attsLeft;
    Project p;
    
public:
    
	ProjectNode(OpTreeNode *child, NameList* attrsLeft);
    
    void run();

    void print();
};

class JoinNode : public OpTreeNode {
private:
    CNF cnf;
	Record literal;
    Join j;
    
    void joinSchema();
    
public:
    
    JoinNode(OpTreeNode *leftChild, OpTreeNode *rightChild, AndList *joinList);
    
    void run();

    void print();
};

class DuplicateRemovalNode : public OpTreeNode {
private:
    DuplicateRemoval dr;
    
public:
    
    DuplicateRemovalNode(OpTreeNode *child);
    
    void run();

    void print();
};

class SumNode : public OpTreeNode {
private:
    FuncOperator *func;
    Function computeMe;
    Sum s;
    
    void sumSchema();
    
public:
    
    SumNode(OpTreeNode *child, FuncOperator *func);
    
    void run();

    void print();

    string joinFunc(FuncOperator *func);
    
};


class GroupByNode : public OpTreeNode {
private:
    FuncOperator *func;
    OrderMaker groupOrder;
    Function computeMe;
    GroupBy gb;
    
    void getOrder(NameList* groupingAtts);
    void groupBySchema();
    string joinFunc(FuncOperator *func);
    
public:
    
    GroupByNode(OpTreeNode *child, NameList *groupingAtts, FuncOperator *func);
    
    void run();

    void print();
};

#endif
