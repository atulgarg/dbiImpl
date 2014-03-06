#ifndef GENERICDBFILE_H
#define GENERICDBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"

typedef enum {heap, sorted, tree} fType;
struct SortInfo
{
	OrderMaker *myOrder;
	int runLength;
};

class GenericDBFile {

public:
	GenericDBFile (); 

	int Create (char *fpath, fType file_type, void *startup) = 0;
	int Open (char *fpath) = 0;
	int Close () = 0;

	void Load (Schema &myschema, char *loadpath) = 0;

	void MoveFirst () = 0;
	void Add (Record &addme) = 0;
	int GetNext (Record &fetchme) = 0;
	int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;

};
#endif
