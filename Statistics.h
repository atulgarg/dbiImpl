#ifndef STATISTICS_
#define STATISTICS_
#include "ParseTree.h"
#include <map>
#include<string>
#include<fstream>
class RelationAttribute
{
    public:
    int numTuples;
    std::map<std::string,int> attribMap;
    RelationAttribute(int numTuples);
    void ReadRelation(std::ifstream& file);
    void WriteRelation(std::ofstream& file);
    RelationAttribute(const  RelationAttribute& copyme);
    RelationAttribute();
};

class Statistics
{
    public:
        std::map<std::string, RelationAttribute> statMap;

        Statistics();
        Statistics(Statistics &copyMe);	 // Performs deep copy
        ~Statistics();


        void AddRel(char *relName, int numTuples);
        void AddAtt(char *relName, char *attName, int numDistincts);
        void CopyRel(char *oldName, char *newName);

        void Read(char *fromWhere);
        void Write(char *fromWhere);

        void  Apply(struct AndList *parseTree, char *relNames[], int numToJoin);
        double Estimate(struct AndList *parseTree, char **relNames, int numToJoin);

};

#endif
