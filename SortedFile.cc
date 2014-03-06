#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedFile.h"
#include "DBFile.h"
#include "Defs.h"
/**
 *  * @method writeSortedOrdertoMetadata to write sortOrder to meta data file.
 *   */
void SortedFile::writeSortedOrdertoMetadata(FILE* metaFile,SortInfo* sortInfo)
{
	//Write the SortInfo Object to Meta Data File
	int status = fseek(metaFile, sizeof(int), SEEK_SET);
	fwrite(sortInfo,sizeof(char), sizeof(*sortInfo), metaFile);
}
/**
 * Method to initialise each pipe with adequate Buffer Size						//To Check
 */
SortedFile::SortedFile(): in(100),out(100) 
{

}
 /**
 *
 */
int SortedFile::Create (char *f_path, fType f_type, void *startup) 
{
	FILE* metaFile = fopen(DBFile::getMetaDataFileName(f_path),"wb");
	//write the  SortInfo to file.
	SortInfo*  sortInfo = (SortInfo*)startup;
	writeSortedOrdertoMetadata(metaFile, sortInfo);
	OrderMaker *sortOrder = sortInfo->myOrder;
	int runLength = sortInfo->runLength;
	//initialise BigQ instance.
	bigQ = new BigQ(in, out, *sortOrder, runLength);	
	//set the mode
	fmode = write;
	fclose(metaFile);
}
/**
 * @method Load to load all set of records from a text file to a binary file in sorted order. method internally calls
 * Add function which adds function to Input pipe associated with BigQ object.
 */
void SortedFile::Load (Schema &f_schema, char *loadpath) 
{
	FILE *txtFile = fopen(loadpath,"r");
	if(txtFile != NULL)
	{
		Record rec;
		while(rec.SuckNextRecord(&f_schema,txtFile) != 0)
		{
			Add(rec);
		}
	}
	else
	{
		cerr<<"No file with specified name "<< loadpath <<" exists. "<<"\n";
		exit(1);
	}
}

int SortedFile::Open (char *f_path) 
{
	//Get metadata file name
	char * metaDataFileName = DBFile::getMetaDataFileName(f_path);
	//read sortInfo struct and later extract SortOrder.
	SortInfo sortInfo;
	FILE* metaFile = fopen(metaDataFileName,"rb");
	fseek(metaFile, sizeof(int),SEEK_SET);
	fread(&sortInfo, sizeof(char), sizeof(SortInfo),metaFile);
	int runLength = sortInfo.runLength;
	OrderMaker *sortOrder = sortInfo.myOrder;
	//initialise BigQ object.
	bigQ = new BigQ(in, out, *sortOrder, runLength);
	fclose(metaFile);
}

void SortedFile::MoveFirst () {
}

int SortedFile::Close () 
{
}

void SortedFile::Add (Record &rec) 
{
	//add record to input pipe for BigQ instance.
}

int SortedFile::GetNext (Record &fetchme) {
}

int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}
