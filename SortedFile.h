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
	File* myfile;
	Page* readPage;
	Pipe in;
	Pipe out;
	void writeSortedOrdertoMetadata(FILE* metaFile,SortInfo *sortInfo);
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
