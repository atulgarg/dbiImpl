#include "Statistics.h"

Statistics::Statistics()
{

}
/**
 * @CopyConstructor for Statistics class
 */
Statistics::Statistics(Statistics &copyMe)
{
	//For each of the relation copy all the related parameters 
	for(map<string, RelationAttribute>::iterator iter = copyMe.statMap.begin(); iter != copyMe.statMap.end(); iter++)
	{
		RelationAttribute table;
		statMap[iter->first] = table;
		statMap[iter->first].partition_number = iter->second.partition_number;
		statMap[iter->first].numtuples = iter->second.numtuples;
		//For each of the relation copy all the attributes.
		for(map<string, int>::iterator it2 = iter->second.AttributeMap.begin(); it2 != iter->second.AttributeMap.end(); it2++)
		{
			statMap[iter->first].AttributeMap[it2->first] = it2->second;
		}

	}
}
Statistics::~Statistics()
{
}
/**
 * @method CopyRel to copy relation from old name to new name and do a deep copy.
 *
 */
void Statistics::CopyRel(char *oldName, char *newName)
{
    if(oldName == NULL || newName == NULL || statMap.find(oldName) == statMap.end())
    {
        cerr<<" Problem with relation name specified."<<endl;
        exit(0);
    }
    if(statMap.find(newName) == statMap.end())
    {
        RelationAttribute table;
        statMap[newName] = table;
        statMap[newName].numtuples =statMap[oldName].numtuples;
        for(map<string, int>::iterator it = statMap[oldName].AttributeMap.begin(); it != statMap[oldName].AttributeMap.end(); it++)
            statMap[newName].AttributeMap[it->first] = it->second;

    }

}
/**
 * @method AddRel to add new relation with number of tuples specified.
 * @param char* pointing to relation name.
 * @param int value for number of tuples in relation.
 *
 */

void Statistics::AddRel(char *relName, int numTuples)
{
	if(relName == NULL)
	{
		cerr<<"Invalid Relation Name"<<endl;
		exit(0);
	}
	if(statMap.find(relName) == statMap.end())
	{
		RelationAttribute table;
		statMap[relName] = table;
		statMap[relName].numtuples = numTuples;
	}
	else
		statMap[relName].numtuples = numTuples;
}
/**
 * @method AddAtt to add attribute to relation specified. 
 * @param relName char* for relation to which attribute needs to be added.
 * @param attName char* for name of the attribute to be added.
 * @param numDistincts int value for number of distinct value in attribute.
 */

void Statistics::AddAtt(char *relName, char *attName, int numDistincts)
{
  if(relName == NULL || attName == NULL || statMap.find(string(relName)) == statMap.end())
  {
      cerr<<"Either relation or Attribute is NULL."<<endl;
      exit(0);
  }
	if(statMap.find(relName) != statMap.end())
	{
		if(statMap[relName].AttributeMap.count(attName) == 0)
			statMap[relName].AttributeMap[attName] = numDistincts;
		else
			statMap[relName].AttributeMap[attName] = numDistincts;
	}
	//if relation did not exist for reverse look up add it for reverse mapping.
	if(attribute_lookup.find(attName)== attribute_lookup.end())
		attribute_lookup[attName] = relName;
	

}

/**
 * @method Read to read a Statistics class from file specified.
 * @param fromWhere char* pointing to file for opening for reading.
 */
void Statistics::Read(char *fromWhere)
{
    ifstream infile(fromWhere);

    int relation_size = 0;
    int attsize = 0;
    string relation_name;
    string attname;
    int disttuples = 0;
    infile>>relation_size;

    //READ statMap first.
    if(relation_size != 0)
    {
        for(int i=0;i<relation_size;i++)
        {
            RelationAttribute relAtt;
            infile>>relation_name;
            statMap[relation_name] = relAtt;
            infile>>statMap[relation_name].numtuples;
            infile>>statMap[relation_name].partition_number;
            infile>>attsize;
            for(int i=0;i<attsize;i++)
            {
                infile>>attname;
                infile>>disttuples;
                statMap[relation_name].AttributeMap[attname]=disttuples;
            }
        }
        //Initialise JoinMap.
        infile>>relation_size;
        for(int i=0; i<relation_size; i++)
        {
            int id;
            infile>>id;
            vector<string> relationlist;
            JoinMap[id] = relationlist;
            int vectsize = 0;
            infile>>vectsize;
            for(int j=0; j<vectsize; j++)
            {
                string relation_name;
                infile>>relation_name;
                JoinMap[id].push_back(relation_name);
            }
        }

        //Initialise RelationJoinMap
        infile>>relation_size;
        for(int i=0; i<relation_size; i++)
        {
            string tempstring;
            int value = 0;
            infile>>tempstring;
            infile>>value;
            RelationJoinMap[tempstring] = value;
        }
        //Initialise Reverse look up map
        infile>>relation_size;
        for(int i=0; i<relation_size; i++)
        {
            string attributeName;
            string relationName;
            infile>>attributeName;
            infile>>relationName;
            attribute_lookup[attributeName] = relationName;
        }
    }
    infile.close();
}
/**
 * @method Write to write Statistics class to file specified.
 * @param fromWhere char* pointing to file for writting class instance.
 */
void Statistics::Write(char *fromWhere)
{
    ofstream outfile(fromWhere);

    //Write statMap.
    outfile<<statMap.size()<<endl;
    for(map<string, RelationAttribute>::iterator it = statMap.begin(); it != statMap.end(); it++)
    {
        outfile<<it->first<<endl;
        outfile<<it->second.numtuples<<endl;
        outfile<<it->second.partition_number<<endl;
        outfile<<it->second.AttributeMap.size()<<endl;
        for(map<string, int>::iterator it2 = it->second.AttributeMap.begin(); it2 != it->second.AttributeMap.end(); it2++){
            outfile<<it2->first<<endl;
            outfile<<it2->second<<endl;
        }
    }
    //write Join Map.
    outfile<<JoinMap.size()<<endl;

    for(map<int, vector<string> >::iterator iter = JoinMap.begin(); iter != JoinMap.end(); iter++)
    {
        outfile<<iter->first<<endl;
        outfile<<iter->second.size()<<endl;
        for(vector<string>::iterator it = iter->second.begin(); it != iter->second.end(); it++)
            outfile<<*it<<endl;
    }

    //Write Relation Join Map
    outfile<<RelationJoinMap.size()<<endl;

    for(map<string, int>::iterator it5 = RelationJoinMap.begin(); it5 != RelationJoinMap.end(); it5++)
    {
        outfile<<it5->first<<endl;
        outfile<<it5->second<<endl;
    }

    //Write Attribute_lookup
    outfile<<attribute_lookup.size()<<endl;

    for(map<string, string>::iterator iter=attribute_lookup.begin(); iter != attribute_lookup.end(); iter++)
    {
        outfile<<iter->first<<endl;
        outfile<<iter->second<<endl;
    }

    outfile.close();
}
double Statistics:: getResult(set <string> &jointableset,vector<double> &estimates)
{
	unsigned long long int estimateN = 1;    

		set <string>::iterator it ;
		for (it = jointableset.begin(); it != jointableset.end(); it++)
		{
			string id = *it;
			if(statMap[id].partition_number==1)
			{
				int nextPartitionNumber = RelationJoinMap[id];
				stringstream convert;   
				convert << nextPartitionNumber;      
				id = convert.str();
			}

			estimateN *= statMap[id].numtuples;
		}
		double result = estimateN;
		for(int i = 0; i < estimates.size(); i++)   
		{
			result *= estimates[i];
		}
		return result;
}
/**
 * @method Apply
 * @param struct AndList
 * @param char*[] array of tables to join.
 * @param int number of tables to join
 */
void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    joined_relation_set.clear();
    vector<string> AttsToEstimate;
    if( !isValid(parseTree, relNames, numToJoin,AttsToEstimate) ||!checkValidPartition(parseTree, relNames, numToJoin))
    {
        cerr<<"Either of the relation or attribute is not valid"<<endl;
        exit(0);
    }

    double numTuples = Estimate(parseTree, relNames, numToJoin);    
    bool partition_exists = false;
    int nextPartitionNumber = 0;

    for(set<string>::iterator it = joined_relation_set.begin(); it != joined_relation_set.end(); it++)
    {
        int partition = statMap[*it].partition_number;
        if(partition == 1 )
        {
            partition_exists = true;
            istringstream convert(*it);
            if ( !(convert >> nextPartitionNumber) ) 
                nextPartitionNumber = 0;
            break;
        }
    }

    if(!partition_exists)
    {
        for(map<string, int>::iterator it2 = RelationJoinMap.begin(); it2 != RelationJoinMap.end(); it2++)
        {
            if(nextPartitionNumber<it2->second)
                nextPartitionNumber = it2->second;
        }
        nextPartitionNumber++;
    }

    for(set<string>::iterator it = joined_relation_set.begin(); it != joined_relation_set.end(); it++)
    {
        int partition = statMap[*it].partition_number;
        if(partition == 0 )
            RelationJoinMap[*it] = nextPartitionNumber;
    }

    if(JoinMap.count(nextPartitionNumber) == 0)
    {
        vector<string> tablenames;
        for(set<string>::iterator it = joined_relation_set.begin(); it != joined_relation_set.end(); it++)
            tablenames.push_back( *it);
        JoinMap[nextPartitionNumber]  = tablenames;
    }
    else
    {
        for(set<string>::iterator it = joined_relation_set.begin(); it != joined_relation_set.end(); it++)
        {
            string temp = string(*it);

            if(!valid_string(temp))
                JoinMap[nextPartitionNumber].push_back(temp);
            else
            {
                int temp1;
			    istringstream convert(*it);
                if (!(convert >> temp1))
                {
                    cerr<<"Unable to convert. Not a valid parameter."<<endl;
                    exit(0);
                }

                for(vector<string>::iterator it1 = JoinMap[temp1].begin(); it1 != JoinMap[temp1].end(); it1++)
                {
                    vector<string> tempvector;
                    for(vector<string>::iterator tempit = JoinMap[nextPartitionNumber].begin(); tempit != JoinMap[nextPartitionNumber].end(); tempit++)						
                        tempvector.push_back(*tempit);

                    string tempstring = *it1;
                    JoinMap[nextPartitionNumber] = tempvector;
                    RelationJoinMap[*it1] = nextPartitionNumber;

                }
            }
        }

    }

    ostringstream convert;
    convert << nextPartitionNumber;      
    string newkey = convert.str();
    RelationAttribute table;

    int temppartition;
    for(set<string>::iterator iter = joined_relation_set.begin(); iter != joined_relation_set.end(); iter++)
    {
        statMap[*iter].partition_number = 1;
        for(map<string, int>::iterator iter1 = statMap[*iter].AttributeMap.begin(); iter1 != statMap[*iter].AttributeMap.end(); iter1++)
            table.AttributeMap[iter1->first] = iter1->second;

        RelationAttribute table1;
        statMap[newkey] = table1;
        statMap[newkey].numtuples = (unsigned long int)numTuples;
        statMap[newkey].partition_number = 1;
        for(map<string, int>::iterator iter1 = table.AttributeMap.begin(); iter1 != table.AttributeMap.end(); iter1++)
            statMap[newkey].AttributeMap[iter1->first] = iter1->second;
    }

}
/*
 * @method valid_string to check if the string passed in argument has numeric digits or not.
 * @param string s to check for.
 * @returns true if number else false.
 *
 */
bool Statistics::valid_string(string s)
{
	string::iterator *it;
	int k = 0;
	while(k < s.size()){
		if(s[k] >= '0' && s[k] <= '9'){
			return true;
		}
		k++;
	}
	return false;
}
/**
 * @method Statistics::isValid to check if all the relation names specified are valid and exist.
 * method also checks if all the attributes in parsetree exist in some of the relation.
 * @param struct AndList *parseTree
 * @param char *relNames[] array of relation names.
 * @param int numToJoin number of relations in array.
 * @param vector<string> &AttsToEstimate this is passed by reference to initialise while reading all the attributes in the parse tree.
 *
 */
bool Statistics::isValid(struct AndList *parseTree, char *relNames[], int numToJoin,vector<string> &AttsToEstimate)
{
	for(int i=0;i<numToJoin;i++)
	{
		string rname = string(relNames[i]);
		if(statMap.find(rname) == statMap.end())
			return false;

	}
	string tblname;
	string colname;

	AndList* andtree = parseTree;
	while(andtree!=NULL)
	{
		OrList *ortree = andtree->left;
		while(ortree!=NULL)
		{
			ComparisonOp* compOp = ortree->left;
			if(compOp == NULL)
				break;
			int lcode = compOp->left->code;
			string lval = compOp->left->value;
			ostringstream convert;
			convert << lcode;
			string val = convert.str();

			AttsToEstimate.push_back(val);
			AttsToEstimate.push_back(lval);
			ostringstream convert1;
			convert1 << compOp->code;
			val="";
			int opcode = compOp->code;
			val = convert1.str();
			AttsToEstimate.push_back(val);
			if(lcode == NAME)
			{    
				int pos = lval.find(".");
				if(pos!=string::npos)
				{
					tblname = lval.substr(0, pos);
					colname  = lval.substr(pos+1);

				}
				else
				{
					colname = lval;

				}
				
				if(opcode == EQUALS)
				{

					string i=attribute_lookup[colname];
					if(attribute_lookup.count(colname)==0){
						return false;
					}
				}
			}
			int rcode = compOp->right->code;
			string rval = compOp->right->value;
			val="";
			ostringstream convert2;
			convert2 << rcode;
			val = convert2.str();

			AttsToEstimate.push_back(val);
			AttsToEstimate.push_back(rval);
			if(rcode == NAME)
			{   
				int pos = rval.find(".");
				if(pos!=string::npos)
				{
					tblname = rval.substr(0, pos);
					colname  = rval.substr(pos+1);

				}
				else
				{
					colname = rval;

				}
				if(opcode == EQUALS)
				{
					string i=attribute_lookup[colname];
					if(attribute_lookup.count(colname)==0)
					{
						return false;
					}
				}
			}
			if(ortree->rightOr != NULL)
				AttsToEstimate.push_back("OR");
			ortree = ortree->rightOr;

		}
		if(andtree->rightAnd != NULL)
			AttsToEstimate.push_back("AND");
		else
			AttsToEstimate.push_back("END");

		andtree = andtree->rightAnd;
	}
	return true;
}
/**
 * @method  Statistics::checkValidPartition to check if all the relations asked to join are valid partitions and 
 * result is a feasible output.
 * @param struct AndList *parseTree 
 * @param char *relNames[] array of relation name to join
 * @param int numToJoin number of relations to join
 *
 */
bool Statistics::checkValidPartition(struct AndList *parseTree, char *relNames[], int numToJoin)
{

	string temp;
	joined_relation_set.clear();
	for(int i=0; i<numToJoin; i++){
		ostringstream convert;   
		string tblname = string(relNames[i]);
		int singleton = statMap[tblname].partition_number;
		if(singleton != 0){
			bool found = false;
			int id = RelationJoinMap[tblname];
			convert << id;      
			temp = convert.str();
			joined_relation_set.insert(temp);
			vector<string>::iterator it;
			for(it = JoinMap[id].begin(); it != JoinMap[id].end(); it++){
				string st1 = *it;
				for(int k = 0; k<numToJoin; k++){
					string st2 = string(relNames[k]);
					if(st1.compare(st2) == 0){
						found = true;
						break;
					}
				}
				if(found == false){
					return false;
				}
			}
		}
		else
		{
			joined_relation_set.insert(tblname);
		}
	}
	return true;
}
/**
 * @method Statistics::Estimate
 * @param struct AndList *parseTree parse tree organised as form of each or as left and and as righ. Left Deep Tree.
 * @param char **relNames array of relation_name to join.
 * @param int numToJoin number of relations to join
 *
 */
double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{

	double estimate =0;
	joined_relation_set.clear();
	set <string> jointableset;
	map<string, EstimateMetaInfo> EstimateChk;
	vector<double> estimates;
	vector<string> AttsToEstimate;        
	string prev_andor = "";

	if(!isValid(parseTree, relNames, numToJoin,AttsToEstimate) || !checkValidPartition(parseTree, relNames, numToJoin))
	{
		cerr<<"Either of the relation or Attribute in valid"<<endl;
		exit(0);
	}

	int i=0;

	while(i<AttsToEstimate.size())
	{
		estimate =0;

		int att1code = atoi(AttsToEstimate.at(i++).c_str());
		string att1val = AttsToEstimate.at(i++);

		int opcode = atoi(AttsToEstimate.at(i++).c_str());

		int att2code = atoi(AttsToEstimate.at(i++).c_str());
		string att2val = AttsToEstimate.at(i++);

		string andor = AttsToEstimate.at(i++);

		string relation1, relation2;
	
		if(att1code==NAME)
		{
	
			int pos = att1val.find(".");
			if(pos!=string::npos)
			{
				relation1 = att1val.substr(0,pos);
				att1val = att1val.substr(pos+1);
				bool found = false;
				for(int i=0; i< numToJoin;i++)
				{
					if(relNames[i] == relation1)
					{
						found = true;
						break;
					}

				}
				if(!found)
					relation1 = attribute_lookup[att1val];

			}
			else
			{
				relation1 = attribute_lookup[att1val];
			}
			if(relation1.size()!=0)
				jointableset.insert(relation1);
		}
		if(att2code==NAME)
		{
			int pos = att2val.find(".");
			if(pos!=string::npos)
			{
				relation2 = att2val.substr(0,pos);
				att2val = att2val.substr(pos+1);
				att1val = att1val.substr(pos+1);
				bool found = false;
				for(int i=0; i< numToJoin;i++)
				{
					if(relNames[i] == relation2)
					{
						found = true;
						break;
					}

				}
				if(!found)
					relation2 = attribute_lookup[att2val];


			}
			else
			{
				relation2 = attribute_lookup[att2val];
			}
			if(relation2.size()!=0)
				jointableset.insert(relation2);
		}
		if(att1code==NAME && att2code == NAME)
		{
			double  maxval = max(statMap[relation1].AttributeMap[att1val],statMap[relation2].AttributeMap[att2val]);

			estimate = 1.0/maxval;
			estimates.push_back(estimate);
		}
		else if(att1code==NAME || att2code == NAME)
		{
			string attribute;
			RelationAttribute tableinfo;
			if(att1code == NAME)
			{
				tableinfo = statMap[relation1];
				attribute = att1val;
			}
			else
			{
				tableinfo = statMap[relation2];
				attribute = att2val;
			}
			if(opcode == EQUALS)
			{
				if(andor == "OR" || prev_andor=="OR")		
				{
					if(EstimateChk.find(attribute + "EQ") == EstimateChk.end())
					{
						EstimateMetaInfo *est = new EstimateMetaInfo();
						estimate = (1.0- 1.0/tableinfo.AttributeMap[attribute]);
						est->attributeCount = 1;
						est->estimatedtuples = estimate;
						EstimateChk[attribute + "EQ"] = *est;
					}
					else
					{
						estimate = 1.0/tableinfo.AttributeMap[attribute];
						EstimateMetaInfo *est = &(EstimateChk[attribute + "EQ"]);
						est->attributeCount += 1;
						est->estimatedtuples = est->attributeCount * estimate;
					}

					if(andor != "OR")
					{
						long double tempResult = 1.0;
						map<string, EstimateMetaInfo>::iterator it;
						for(it = EstimateChk.begin(); it !=EstimateChk.end(); it++)
						{
							if(it->second.attributeCount == 1)
								tempResult *= it->second.estimatedtuples;
							else
								tempResult *= (1 - it->second.estimatedtuples);
						}

						long double totalCurrentEstimate = 1.0 - tempResult;
						estimates.push_back(totalCurrentEstimate);
						EstimateChk.clear();
					}
				}

				else
				{
					estimate = 1.0/tableinfo.AttributeMap[attribute];
					estimates.push_back(estimate);
				}
			}
			else
			{
				if((andor == "OR") || (prev_andor == "OR"))
				{
					estimate = (1.0 - 1.0/3);

					EstimateMetaInfo *est = new EstimateMetaInfo();
					est->attributeCount = 1;
					est->estimatedtuples = estimate;
					EstimateChk[attribute] = *est;
					if(andor != "OR")
					{
						long double tempResult = 1.0;
						long double totalCurrentEstimate;
						map<string, EstimateMetaInfo>::iterator it ;
						for(it = EstimateChk.begin(); it != EstimateChk.end(); it++)
						{
							if(it->second.attributeCount == 1)
								tempResult *= it->second.estimatedtuples;	
							else
								tempResult *= (1 - it->second.estimatedtuples);
						}

						totalCurrentEstimate = 1.0 - tempResult;
						estimates.push_back(totalCurrentEstimate);
						EstimateChk.clear();
					}
				}
				else
				{
					estimate = (1.0/3);
					estimates.push_back(estimate);
				}

			}
		}
		prev_andor = andor;
	}

	double result = getResult( jointableset,estimates);
	return result;
}


