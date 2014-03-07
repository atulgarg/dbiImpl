#ifndef SORTEDFILE_H
#define SORTEDFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"
#include "BigQ.h"

typedef enum {read,write} Mode;

class SortedFile: public virtual GenericDBFile
{
	private:
	Mode fmode; 	
	BigQ* bigQ;
	File* myfile;				//File instance to actually store sorted records.
	Page* readPage = NULL;			//Page instance used to read page.			
	Pipe* in;				//Input pipe
	Pipe* out;				//Output pipe

	Page* writePage = NULL;			//write Page used to write.
	off_t write_page_marker;	
	off_t read_page_marker;
	SortInfo* sortInfo;
	//method to write sortInfo struct to metadata file.
	void writeSortedOrdertoMetadata(FILE* metaFile,SortInfo *sortInfo);
	void initialiseForWrite();
	void initialiseForRead();
	bool isModeChanged(Mode m);
	void  cleanUp();
	//method to merge records from file and BigQ
	void mergeRecords();
	//method to add new record to file specified in argument.
	void addToTempFile(Record& record,File *file,off_t &writeMarker,Page* writePage);
	//method to initialise new file instance and open file instance based on fileLen
	int  initialiseSortedFile(int fileLen,char* fpath);
	public:
	SortedFile (); 

	int Create (char *fpath, fType file_type, void *startup);
	int Open (char *fpath);
	int Close ();

	void Load (Schema &myschema, char *loadpath);

	void MoveFirst ();
	void Add (Record &addme);
	int GetNext (Record &fetchme);
	int GetNext (Record &fetchme, CNF &cnf, Record &literal);
};
#endif
