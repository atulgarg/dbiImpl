#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"
#include "string.h"
#include "stdlib.h"
#include <iostream>
/**
 * @method getMetaDataFileName to generate file name for metadata based on file input. Meta data will be created with name as
 * filename.meta.
 * @param f_path character pointer to file path including file name.
 */
char * DBFile:: getMetaDataFileName(char* f_path)
{
	char* metaFileName = (char*) malloc((strlen(f_path) + strlen(".meta"))*sizeof(char));
	//create meta file name as fpath + ".meta"
	strcat(metaFileName,f_path);
	strcat(metaFileName,".meta");
	return metaFileName;
}

DBFile::DBFile () {

}
/**
 * @method Create to create a new DBFile instance based on file type specified. If file type specified is heap method initialises 
 * instance of GenericDBFile to new Heap instance and writes "heap" to meta data file. If file type specified is sorted method 
 * initialises instance of GenericDBFile to new Sorted instance and writes "sorted" to meta data file. 
 * Method also initialises Meta data file with instance of OrderMaker class object.
 */
int DBFile::Create (char *f_path, fType f_type, void *startup) 
{
	int status = 1;
	//create a metadata file as filename.meta and record type of file and sortOrder
	char* metaFileName = getMetaDataFileName(f_path);
	metaFile = fopen(metaFileName,"wb");			
	if(metaFile == NULL)
	{
		cerr<<"Not able to create meta data file."<<endl;
		exit(0);
	}

	//write filetype to metadata file.
	int fileType = f_type;
	fwrite(&fileType,sizeof(char),sizeof(int),metaFile);

	//if File type spcified is heap type initialise variable with heap and call the instance's create method.
	if(f_type == heap)
	{
		dbFile = new HeapFile();
		status = dbFile->Create(f_path,f_type,startup);	

	}else if(f_type == sorted)
	{
		dbFile = new SortedFile();
		status = dbFile->Create(f_path, f_type, startup);	
	}
	return status;
}

void DBFile::Load (Schema &f_schema, char *loadpath) 
{

}

int DBFile::Open (char *f_path) 
{
	char* metaFileName = getMetaDataFileName(f_path);
	metaFile = fopen(metaFileName,"rb");
	if(metaFile == NULL)
	{
		cerr<<"Error Opening Metadata file"<<endl;
		exit(0);
	}
	//read type of file from Meta data
	int fileType;
	fread(&fileType,sizeof(char),sizeof(int),metaFile);
	if(heap == fileType)
		dbFile = new HeapFile();
	else if(sorted == fileType)
		dbFile = new SortedFile();
	return dbFile->Open(f_path);	
}

void DBFile::MoveFirst () 
{
	dbFile->MoveFirst();
}

int DBFile::Close () 
{
	//close metadata file and respective open DBFile.
	int metaStatus = fclose(metaFile);
	int dbfileStatus = dbFile->Close();
	return (metaStatus && dbfileStatus);
}

void DBFile::Add (Record &rec) 
{
	dbFile->Add(rec);
}

int DBFile::GetNext (Record &fetchme) 
{
	return dbFile->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
}
