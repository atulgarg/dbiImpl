#include "Statistics.h"
using namespace std;
RelationAttribute::RelationAttribute(int numTuples)
{
    this->numTuples = numTuples; 
}
Statistics::Statistics()
{
}
Statistics::Statistics(Statistics &copyMe)
{
}
Statistics::~Statistics()
{
}

void Statistics::AddRel(char *relName, int numTuples)
{
    RelationAttribute relationObj(numTuples);
    statMap.insert(std::make_pair(relName, relationObj)); 
}
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
    (((statMap.find(relName))->second).attribNames).push_back(attName); 
    numDistincts = numDistincts != -1 ? numDistincts : ((statMap.find(relName)->second).numTuples);
    (((statMap.find(relName))->second).numDistincts).push_back(numDistincts);
}
void Statistics::CopyRel(char *oldName, char *newName)
{

}
	
void Statistics::Read(char *fromWhere)
{
}
void Statistics::Write(char *fromWhere)
{
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
}

