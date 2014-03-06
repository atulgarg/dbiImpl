#ifndef HEAPFILE_H
#define HEAPFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "GenericDBFile.h"


class HeapFile : public virtual GenericDBFile
{
	Page* readPage;
	Page* writePage;
	off_t read_page_marker;
	off_t write_page_marker;		//keeps track of number of pages.
	void initialiseHeapFile(char* f_path,int fileLen);
	void cleanUp();
	protected:
	File* myHeap;
	public:
	HeapFile (); 

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
