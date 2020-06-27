#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"

#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <fstream>
#include <iostream>
#include <string.h>

class Statistics
{
private:
    std::unordered_map<std::string, int> rels;
    std::unordered_map<std::string, std::pair<std::string, int>> relAttrs;

public:
    Statistics() {};
	Statistics(Statistics &copyMe);	 // Performs deep copy


    void initStatistics();
    
    void AddRel(const char *relName, int numTuples);
	void AddAtt(const char *relName, const char *attName, int numDistincts);
	void CopyRel(const char *oldName, const char *newName);
	
	void Read(const char *fromWhere);
	void Write(const char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

};

#endif
