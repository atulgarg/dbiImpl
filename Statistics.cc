#include "Statistics.h"
using namespace std;
RelationAttribute::RelationAttribute(int numTuples)
{
    this->numTuples = numTuples; 
}
RelationAttribute::RelationAttribute()
{
}
RelationAttribute::RelationAttribute(const  RelationAttribute& copyme)
{
    this->numTuples = copyme.numTuples;
    map<string, int> copyMap = copyme.attribMap;
    for(map<string,int>::iterator it = copyMap.begin();it!=copyMap.end();++it)
    {
        this->attribMap.insert(make_pair(string(it->first), it->second));
    }
}
/**
 * @method ReadRelation to read a relation from ifstream file reference specified.
 * @param file reference of type ifstream with file open to read next relation.
 */
void RelationAttribute::ReadRelation(ifstream& file)
{
    //read number of tuples in relation
    file>>this->numTuples;
    int numvalues;
    //read size of map.
    file>>numvalues;
    for(int i=0;i<numvalues;i++)
    {
        string attribName;
        int distinctCount;
        file>>attribName>>distinctCount;
        attribMap.insert(make_pair(attribName,distinctCount));
    }
}
/**
 * @method WriteRelation to write individual relation to output stream file specified.
 * @param file reference of ofstream to which relation needs to be written.
 *
 */
void RelationAttribute::WriteRelation(ofstream& file)
{
    //write num tuples first
    file<<this->numTuples<<endl;
    //also need to write number of entries in map.
    file<<attribMap.size()<<endl;
    for(map<string,int>::iterator it = attribMap.begin();it!=attribMap.end();++it)
    {
        file<<it->first<<endl<<it->second<<endl;
    }
}
Statistics::Statistics()
{
}
/**
 * @CopyConstructor for Statistics class
 */
Statistics::Statistics(Statistics &copyMe)
{
    map<string, RelationAttribute>::iterator it = (copyMe.statMap).begin();
    for(;it!=copyMe.statMap.end();++it)
    {
        RelationAttribute relAtt(it->second);
        this->statMap.insert(make_pair(string(it->first), relAtt));
    }
}
Statistics::~Statistics()
{
}
/**
 * @method AddRel to add new relation with number of tuples specified.
 * @param char* pointing to relation name.
 * @param int value for number of tuples in relation.
 *
 */
void Statistics::AddRel(char *relName, int numTuples)
{
    RelationAttribute relationObj(numTuples);
    statMap.insert(make_pair(string(relName), relationObj)); 
}
/**
 * @method AddAtt to add attribute to relation specified. 
 * @param relName char* for relation to which attribute needs to be added.
 * @param attName char* for name of the attribute to be added.
 * @param numDistincts int value for number of distinct value in attribute.
 */
void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
  RelationAttribute relObj =  (statMap.find(std::string(relName)))->second;
  numDistincts = numDistincts > 0? numDistincts : -1;
  relObj.attribMap.insert(make_pair(attName,numDistincts));
}
/**
 *
 *
 */
void Statistics::CopyRel(char *oldName, char *newName)
{
    RelationAttribute oldRel = statMap.find(string(oldName))->second;
    //copy constructor
    RelationAttribute newRel(oldRel);
    //remove the old entry
    statMap.erase(statMap.find(string(oldName)));
    statMap.insert(make_pair(string(newName), newRel)); 
}
/**
 * @method Read to read a Statistics class from file specified.
 * @param fromWhere char* pointing to file for opening for reading.
 */
void Statistics::Read(char *fromWhere)
{
    ifstream file(fromWhere, std::ifstream::in);   
    int num_relations;
    file>>num_relations;
    for(int i=0;i<num_relations;i++)
    {
        string rel_name;
        file>>rel_name;
        RelationAttribute relAtt;
        relAtt.ReadRelation(file);
        this->statMap.insert(make_pair(rel_name, relAtt));
    }
}
/**
 * @method Write to write Statistics class to file specified.
 * @param fromWhere char* pointing to file for writting class instance.
 */
void Statistics::Write(char *fromWhere)
{
    ofstream file(fromWhere);   
    //write number of relations.
    file<<statMap.size()<<endl;
    for(map<string, RelationAttribute>::iterator it = statMap.begin(); it!=statMap.end() ;++it)
    {
        //write relation name
       file<< it->first<<endl;
       it->second.WriteRelation(file);
    }
    file.close();
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
        
}
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{

}

