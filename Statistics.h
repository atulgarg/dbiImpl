#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <string>
#include <map>
#include <set>
#include <iostream>
#include <fstream>
#include <vector>
#include <stdlib.h>
#include <algorithm>
#include <cctype>
#include <stdio.h>
#include <tr1/unordered_map>
#include <iomanip>
#include <locale>
#include <sstream>

using namespace std;

class RelationAttribute
{
	public:
	long int numtuples;
	int partition_number;
	map<string , int> AttributeMap;
	RelationAttribute()
	{
		numtuples=0;
		partition_number =0 ;  
	};
};
class Statistics
{
	private:
	struct EstimateMetaInfo
	{
		int attributeCount;  
		long double estimatedtuples;   
	};
	public:
	map<string , RelationAttribute> statMap;
	map<int , vector<string> > JoinMap;
	map<string , int> RelationJoinMap;
	map<string , string> attribute_lookup;					//map for reverse mapping attribute 
	set <string> joined_relation_set;

	
	Statistics();
	Statistics(Statistics &copyMe);	 
	~Statistics();

	void AddRel(char *relName, int numTuples);
	void AddAtt(char *relName, char *attName, int numDistincts);
	void CopyRel(char *oldName, char *newName);

	void Read(char *fromWhere);
	void Write(char *fromWhere);

	void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
	double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

	bool isValid(struct AndList *parseTree, char *relNames[], int numToJoin,vector<string> &AttsToEstimate);
	bool checkValidPartition(struct AndList *parseTree, char *relNames[], int numToJoin);

	bool valid_string(string t);
	double getResult(set <string> &jointableset,vector<double> &estimates);
};

#endif
