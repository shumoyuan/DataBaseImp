#include "Statistics.h"

using namespace std;

Statistics::Statistics(Statistics &copyMe)
{
    rels = unordered_map<string, int>(copyMe.rels);
    relAttrs = unordered_map<string, pair<string, int>>(copyMe.relAttrs);
}

void Statistics::initStatistics() {
    
    rels.clear();
    relAttrs.clear();
    
    const char *supplier = "supplier";
    const char *partsupp = "partsupp";
    const char *part = "part";
    const char *nation = "nation";
    const char *customer = "customer";
    const char *orders = "orders";
    const char *region = "region";
    const char *lineitem = "lineitem";
    
    AddRel(region,5);
    AddRel(nation,25);
    AddRel(part,200000);
    AddRel(supplier,10000);
    AddRel(partsupp,800000);
    AddRel(customer,150000);
    AddRel(orders,1500000);
    AddRel(lineitem,6001215);
    
    // region
    AddAtt(region, "r_regionkey",5); // r_regionkey=5
    AddAtt(region, "r_name",5); // r_name=5
    AddAtt(region, "r_comment",5); // r_comment=5
    // nation
    AddAtt(nation, "n_nationkey",25); // n_nationkey=25
    AddAtt(nation, "n_name",25);  // n_name=25
    AddAtt(nation, "n_regionkey",5);  // n_regionkey=5
    AddAtt(nation, "n_comment",25);  // n_comment=25
    // part
    AddAtt(part, "p_partkey",200000); // p_partkey=200000
    AddAtt(part, "p_name",200000); // p_name=199996
    AddAtt(part, "p_mfgr",200000); // p_mfgr=5
    AddAtt(part, "p_brand",200000); // p_brand=25
    AddAtt(part, "p_type",200000); // p_type=150
    AddAtt(part, "p_size",200000); // p_size=50
    AddAtt(part, "p_container",200000); // p_container=40
    AddAtt(part, "p_retailprice",200000); // p_retailprice=20899
    AddAtt(part, "p_comment",200000); // p_comment=127459
    // supplier
    AddAtt(supplier,"s_suppkey",10000);
    AddAtt(supplier,"s_name",10000);
    AddAtt(supplier,"s_address",10000);
    AddAtt(supplier,"s_nationkey",25);
    AddAtt(supplier,"s_phone",10000);
    AddAtt(supplier,"s_acctbal",9955);
    AddAtt(supplier,"s_comment",10000);
    // partsupp
    AddAtt(partsupp,"ps_partkey",200000);
    AddAtt(partsupp,"ps_suppkey",10000);
    AddAtt(partsupp,"ps_availqty",9999);
    AddAtt(partsupp,"ps_supplycost",99865);
    AddAtt(partsupp,"ps_comment",799123);
    // customer
    AddAtt(customer,"c_custkey",150000);
    AddAtt(customer,"c_name",150000);
    AddAtt(customer,"c_address",150000);
    AddAtt(customer,"c_nationkey",25);
    AddAtt(customer,"c_phone",150000);
    AddAtt(customer,"c_acctbal",140187);
    AddAtt(customer,"c_mktsegment",5);
    AddAtt(customer,"c_comment",149965);
    // orders
    AddAtt(orders,"o_orderkey",1500000);
    AddAtt(orders,"o_custkey",99996);
    AddAtt(orders,"o_orderstatus",3);
    AddAtt(orders,"o_totalprice",1464556);
    AddAtt(orders,"o_orderdate",2406);
    AddAtt(orders,"o_orderpriority",5);
    AddAtt(orders,"o_clerk",1000);
    AddAtt(orders,"o_shippriority",1);
    AddAtt(orders,"o_comment",1478684);
    // lineitem
    AddAtt(lineitem,"l_orderkey",1500000);
    AddAtt(lineitem,"l_partkey",200000);
    AddAtt(lineitem,"l_suppkey",10000);
    AddAtt(lineitem,"l_linenumber",7);
    AddAtt(lineitem,"l_quantity",50);
    AddAtt(lineitem,"l_extendedprice",933900);
    AddAtt(lineitem,"l_discount",11);
    AddAtt(lineitem,"l_tax",9);
    AddAtt(lineitem,"l_returnflag",3);
    AddAtt(lineitem,"l_linestatus",2);
    AddAtt(lineitem,"l_shipdate",2526);
    AddAtt(lineitem,"l_commitdate",2466);
    AddAtt(lineitem,"l_receiptdate",2554);
    AddAtt(lineitem,"l_shipinstruct",4);
    AddAtt(lineitem,"l_shipmode",7);
    AddAtt(lineitem,"l_comment",4501941);
}

void Statistics::AddRel(const char *relName, int numTuples)
{
	string RelName(relName);
	rels[RelName] = numTuples;
}

void Statistics::AddAtt(const char *relName, const char *attName, int numDistincts)
{
	string RelName(relName);
	string AttName(attName);

	if (numDistincts == -1) {
        relAttrs[AttName].first = RelName;
        relAttrs[AttName].second = rels[RelName];
	}
	else {
        relAttrs[AttName].first = RelName;
        relAttrs[AttName].second = numDistincts;
	}
}

void Statistics::CopyRel(const char *oldName, const char *newName)
{
	string oldNameStr(oldName);
	string newNameStr(newName);

	rels[newNameStr] = rels[oldNameStr];
    unordered_map<string, pair<string, int>> tmp;
    
    for (auto iter = relAttrs.begin(); iter != relAttrs.end(); ++iter) {
        if (iter->second.first == oldNameStr) {
            string newAttrName = newNameStr + "." + iter->first;
            tmp[newAttrName].first = newNameStr;
            tmp[newAttrName].second = iter->second.second;
            // AddAtt(newName, newAttrName.c_str(), iter->second.second);
        }
    }
    for (auto iter = tmp.begin(); iter != tmp.end(); ++iter) {
        AddAtt(newName, iter->first.c_str(), iter->second.second);
    }
}
	
void Statistics::Read(const char *fromWhere)
{
    ifstream file(fromWhere);
    
    if (!file) {
        cerr << "Statistics file doesn't exist!" << endl;
        exit(-1);
    }
    
    rels.clear();
    relAttrs.clear();
    
    int attrNum;
    file >> attrNum;
    
    for (int i = 0; i < attrNum; i++) {
        string attr;
        file >> attr;
        
        string::size_type index1 = attr.find_first_of(":");
        string::size_type index2 = attr.find_last_of(":");
        string attrName = attr.substr(0, index1);
        string relName = attr.substr(index1 + 1, index2 - index1 - 1);
        int numDistincts = atoi(attr.substr(index2 + 1).c_str());
        relAttrs[attrName].first = relName;
        relAttrs[attrName].second = numDistincts;
    }
    
    int relNum;
    file >> relNum;
    
    for (int i = 0; i < relNum; i++) {
        string rel;
        file >> rel;
        
        string::size_type index = rel.find_first_of(":");
        string relName = rel.substr(0, index);
        int NumTuples = atoi(rel.substr(index + 1).c_str());
        rels[relName] = NumTuples;
    }
    
    file.close();
}
void Statistics::Write(const char *fromWhere)
{
    ofstream file(fromWhere);
    
    file << relAttrs.size() << endl;
    
    for (auto iter1 = relAttrs.begin(); iter1 != relAttrs.end(); ++iter1) {
        file << iter1->first << ":" << iter1->second.first << ":" << iter1->second.second << endl;
    }
    
    file << rels.size() << endl;
    
    for (auto iter = rels.begin(); iter != rels.end(); ++iter) {
        file << iter->first << ":" << iter->second << endl;
    }
    
    file.close();
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    // cout << "APPLY!!!" << endl;
    double andRes = 1.0;
    int64_t modifier = 1;
    vector<string> joinRels(2);
    string joinedRelName = "";
    for (int i = 0; i < numToJoin - 1; ++i) {
        joinedRelName += relNames[i];
    }
    if (rels.count(joinedRelName) > 0) {
        joinRels[0] = joinedRelName;
        joinRels[1] = relNames[numToJoin - 1];
    }
    else {
        joinedRelName += relNames[numToJoin - 1];
        joinRels[0] = joinedRelName;
    }
    
    if (!parseTree) {
        return;
    }
    
    /*
     * Add code to check whether parseTree is legal here.
     */
    
    AndList *curAnd = parseTree;
    while (curAnd) {
        OrList *curOr = curAnd->left;
        
        /*
         * Should figure out whether attrs in this orList appear more than onece.
         * If they are, the ratio of tuples left should be sum of each comparion's possibility.
         * If they aren't, the ratio of tuples left should be 1 - product of (1 - each possibility)
         */
        
        unordered_map<string, double> orPairList;
        bool isAlwaysTrue = false;
        double orRes = 1.0;
        
        while (curOr) {
            ComparisonOp *curComp = curOr->left;
            Operand *left = curComp->left;
            Operand *right = curComp->right;
            
            if (curComp->code == EQUALS) {
                // Join. The ratio of tuples left should be 1 / max(leftDistinct, rightDistinct).
                if (left->code == NAME && right->code == NAME) {
                    // cout << "JOIN!!!" << endl;
                    string leftAttrName = left->value;
                    string rightAttrName = right->value;
                    
                    string leftRel = relAttrs[leftAttrName].first;
                    // cout << "leftRel: " << leftRel << endl;
                    string rightRel = relAttrs[rightAttrName].first;
                    // cout << "rightRel: " << rightRel << endl;
                    
                    if ((leftRel != joinRels[0] && leftRel != joinRels[1]) || (rightRel != joinRels[0] && rightRel != joinRels[1])) {
                        isAlwaysTrue = true;
                        break;
                    }
                    
                    int leftDistinct = relAttrs[leftAttrName].second;
                    // cout << "leftDistinct: " << leftDistinct << endl;
                    int rightDistinct = relAttrs[rightAttrName].second;
                    // cout << "rightDistinct: " << rightDistinct << endl;
                    
                    relAttrs[leftAttrName].second = min(leftDistinct, rightDistinct);
                    relAttrs[rightAttrName].second = min(leftDistinct, rightDistinct);
                    
                    modifier = max(leftDistinct, rightDistinct);
                    orRes = 1.0 / modifier;
                    // cout << "orRes: " << orRes << endl;
                }
                // Equal Selection. Use different formula according to situation mentioned above.
                else {
                    if (left->code == NAME || right->code == NAME) {
                        // cout << "Equal selection!!!" << endl;
                        string attrName = (left->code == NAME) ? left->value : right->value;
                        string relName = relAttrs[attrName].first;
                        // cout << attrName << endl;
                        // cout << relAttrs[attrName].first << endl;
                        if (relName != joinRels[0] && relName != joinRels[1]) {
                            isAlwaysTrue = true;
                            break;
                        }
                        int distinct = relAttrs[attrName].second;
                        
                        orPairList[attrName] += 1.0 / distinct;
                        if (orPairList[attrName] > 1.0) {
                            orPairList[attrName] = 1.0;
                        }
                    }
                    else if (left->code == right->code && left->value == right->value) {
                        // cout << "Constant selection!!!" << endl;
                        isAlwaysTrue = true;
                        break;
                    }
                }
            }
            /*
             * Less or greater than. Actually I don't know how to get the possibility of this operation.
             * According to testcases we can assume possibility of equal, greater than and less than are
             * equal, which is 1 / 3 each.
             * Also need to figure out whether the same attrs or not then use different formula.
             */
            else {
                if (left->code == NAME || right->code == NAME) {
                    // cout << "Greater or less selection!!!" << endl;
                    string attrName = (left->code == NAME) ? left->value : right->value;
                    string relName = relAttrs[attrName].first;
                    // cout << attrName << endl;
                    // cout << relAttrs[attrName].first << endl;
                    if (relName != joinRels[0] && relName != joinRels[1]) {
                        isAlwaysTrue = true;
                        break;
                    }
                    orPairList[attrName] += 1.0 / 3.0;
                    if (orPairList[attrName] > 1.0) {
                        orPairList[attrName] = 1.0;
                    }
                }
                else if (left->code == right->code && ((curComp->code == GREATER_THAN && left->value > right->value) || (curComp->code == LESS_THAN && left->value < right->value))) {
                    // cout << "Constant selection!!!" << endl;
                    isAlwaysTrue = true;
                    break;
                }
            }
            
            curOr = curOr->rightOr;
        }
        
        if (!isAlwaysTrue && !orPairList.empty()) {
            // cout << "I am here!" << endl;
            double reveRes = 1;
            for (auto iter = orPairList.begin(); iter != orPairList.end(); ++iter) {
                reveRes *= (1.0 - iter->second);
            }
            orRes = 1 - reveRes;
        }
        andRes *= orRes;
        // cout << "andRes: " << andRes << endl;
        
        curAnd = curAnd->rightAnd;
    }
    
    double numofTuples = andRes;
    numofTuples *= rels[joinRels[0]];
    rels.erase(joinRels[0]);
    if (joinRels[1] != "") {
        numofTuples *= rels[joinRels[1]];
        rels.erase(joinRels[1]);
    }
    string newRelName = joinRels[0] + joinRels[1];
    rels[newRelName] = numofTuples;
    andRes *= modifier;
    for (auto iter = relAttrs.begin(); iter != relAttrs.end(); ++iter) {
        if (iter->second.first == joinRels[0] || iter->second.first == joinRels[1]) {
            string attrName = iter->first;
            int numofDistinct = iter->second.second * andRes;
            relAttrs[attrName].first = newRelName;
            relAttrs[attrName].second = numofDistinct;
        }
    }
    // cout << "newRelName: " << newRelName << endl;
    // cout << "numofTuples: " << rels[newRelName] << endl;
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    double andRes = 1.0;
    vector<string> joinRels(2);
    string joinedRelName = "";
    for (int i = 0; i < numToJoin - 1; ++i) {
        joinedRelName += relNames[i];
    }
    if (rels.count(joinedRelName) > 0) {
        joinRels[0] = joinedRelName;
        joinRels[1] = relNames[numToJoin - 1];
    }
    else {
        joinedRelName += relNames[numToJoin - 1];
        joinRels[0] = joinedRelName;
    }
    
    if (!parseTree) {
        return 0;
    }
    
    /*
     * Add code to check whether parseTree is legal here.
     */
    
    AndList *curAnd = parseTree;
    while (curAnd) {
        OrList *curOr = curAnd->left;
        double orRes = 1.0;
        
        /*
         * Should scan the orList first to figure out whether attrs in this orList are the same.
         * If they are, the ratio of tuples left should be sum of each comparion's possibility.
         * If they aren't, the ratio of tuples left should be 1 - product of (1 - each possibility)
         */
        
        unordered_map<string, double> orPairList;
        bool isAlwaysTrue = false;
        
        while (curOr) {
            ComparisonOp *curComp = curOr->left;
            Operand *left = curComp->left;
            Operand *right = curComp->right;
            
            if (curComp->code == EQUALS) {
                // Join. The ratio of tuples left should be 1 / max(leftDistinct, rightDistinct).
                if (left->code == NAME && right->code == NAME) {
                    // cout << "JOIN!!!" << endl;
                    string leftAttrName = left->value;
                    string rightAttrName = right->value;
                    
                    string leftRel = relAttrs[leftAttrName].first;
                    // cout << "leftRel: " << leftRel << endl;
                    string rightRel = relAttrs[rightAttrName].first;
                    // cout << "rightRel: " << rightRel << endl;
                    
                    if ((leftRel != joinRels[0] && leftRel != joinRels[1]) || (rightRel != joinRels[0] && rightRel != joinRels[1])) {
                        isAlwaysTrue = true;
                        break;
                    }
                    
                    int leftDistinct = relAttrs[leftAttrName].second;
                    // cout << "leftDistinct: " << leftDistinct << endl;
                    int rightDistinct = relAttrs[rightAttrName].second;
                    // cout << "rightDistinct: " << rightDistinct << endl;
                    
                    // cout << "leftNumofTuples: " << rels[leftRel] << endl;
                    // cout << "rightNumofTuples: " << rels[rightRel] << endl;
                    
                    // cout << "result: " << result << endl;
                    orRes = 1.0 / max(leftDistinct, rightDistinct);
                    // cout << "result: " << result << endl;
                }
                // Equal Selection. Use different formula according to situation mentioned above.
                else {
                    // cout << "EQUAL COMPARISON!!!" << endl;
                    if (left->code == NAME || right->code == NAME) {
                        string attrName = (left->code == NAME) ? left->value : right->value;
                        string relName = relAttrs[attrName].first;
                        // cout << "relName: " << relName << endl;
                        if (relName != joinRels[0] && relName != joinRels[1]) {
                            isAlwaysTrue = true;
                            break;
                        }
                        int distinct = relAttrs[attrName].second;
                        
                        orPairList[attrName] += 1.0 / distinct;
                        if (orPairList[attrName] > 1.0) {
                            orPairList[attrName] = 1.0;
                        }
                    }
                    else if (left->code == right->code && left->value == right->value) {
                        isAlwaysTrue = true;
                        break;
                    }
                }
            }
            // Less or greater than. Actually I don't know how to get the possibility of this operation.
            // First we can simply try equal possibility of equal, greater than and less than. 1 / 3 each.
            // Also need to figure out whether the same attrs or not then use different formula.
            else {
                if (left->code == NAME || right->code == NAME) {
                    string attrName = (left->code == NAME) ? left->value : right->value;
                    string relName = relAttrs[attrName].first;
                    // cout << "relName: " << relName << endl;
                    // cout << "joinRels[0]: " << joinRels[0] << endl;
                    // cout << "joinRels[1]: " << joinRels[1] << endl;
                    if (relName != joinRels[0] && relName != joinRels[1]) {
                        isAlwaysTrue = true;
                        break;
                    }
                    orPairList[attrName] += 1.0 / 3.0;
                    if (orPairList[attrName] > 1.0) {
                        orPairList[attrName] = 1.0;
                    }
                }
                else if (left->code == right->code && ((curComp->code == GREATER_THAN && left->value > right->value) || (curComp->code == LESS_THAN && left->value < right->value))) {
                    isAlwaysTrue = true;
                    break;
                }
            }
            
            curOr = curOr->rightOr;
        }
        if (!isAlwaysTrue && !orPairList.empty()) {
            // cout << "I am here!" << endl;
            double reveRes = 1.0;
            for (auto iter = orPairList.begin(); iter != orPairList.end(); ++iter) {
                reveRes *= (1.0 - iter->second);
            }
            orRes = 1 - reveRes;
        }
        // cout << "orRes: " << orRes << endl;
		andRes *= orRes;
        
        curAnd = curAnd->rightAnd;
    }
    
    double result = 1.0;
    result *= rels[joinRels[0]];
    if (joinRels[1] != "") result *= rels[joinRels[1]];
    result *= andRes;

    // cout << "result: " << result << endl;
    return result;
}

