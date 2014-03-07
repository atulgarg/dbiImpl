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
	GenericDBFile ():mode(READ); 
    enum Mode { READ, WRITE } mode; 
	virtual int Create (char *fpath, fType file_type, void *startup) = 0;
	virtual int Open (char *fpath) = 0;
	virtual int Close () = 0;

<<<<<<< HEAD
	virtual int Create (char *fpath, fType file_type, void *startup) = 0;
	virtual int Open (char *fpath) = 0;
	virtual int Close () = 0;

	virtual void Load (Schema &myschema, char *loadpath) = 0;

=======
	virtual void Load (Schema &myschema, char *loadpath) = 0;

>>>>>>> 6f3edfc81dc5c172699b452c9bb5ccb0bed54274
	virtual void MoveFirst () = 0;
	virtual void Add (Record &addme) = 0;
	virtual int GetNext (Record &fetchme) = 0;
	virtual int GetNext (Record &fetchme, CNF &cnf, Record &literal) = 0;

};
#endif

