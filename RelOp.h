#ifndef REL_OP_H
#define REL_OP_H

#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"
#include "Function.h"

class RelationalOp {
    public:
	// blocks the caller until the particular relational operator 
	// has run to completion
	pthread_t thread;
    virtual void WaitUntilDone () = 0;
    
	// tell us how much internal memory the operation can use
	virtual void Use_n_Pages (int n) = 0;
};

class SelectFile : public RelationalOp { 

	private:
 	// Record *buffer;
    DBFile* inFile;
    Pipe* outPipe;
    CNF* selOp;
    Record* literal;
    public:
    friend void* runSelectFile(void * sf);
	void Run (DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n){}

};

class SelectPipe : public RelationalOp {
    private:
        Pipe* inPipe;
        Pipe* outPipe;
        CNF * selOp;
        Record* literal;
	public:
    friend void* runSelectPipe(void *);
	void Run (Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n){}
};
class Project : public RelationalOp {
    private:
        Pipe* inPipe;
        Pipe* outPipe;
        int* keepMe;
        int numAttsInput;
        int numAttsOutput;
    public:
        friend void* runProject(void*);
        void Run (Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput);
        void WaitUntilDone ();
        void Use_n_Pages (int n){}
};
class Join : public RelationalOp 
{
    private:
        Pipe* inPipeL;
        Pipe* inPipeR;
        Pipe* outPipe;
        CNF* selOp;
        Record* literal;
        int runlen;
	public:
    friend void* runJoin(void *); 
	void Run (Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class DuplicateRemoval : public RelationalOp 
{
    private:
        Pipe * inPipe;
        Pipe * outPipe;
        Schema *mySchema;
        int runlen;
	public:
    friend void * runDuplicateRemoval(void *);
	void Run (Pipe &inPipe, Pipe &outPipe, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};


class Sum : public RelationalOp {
    private:
        Pipe *inPipe;
        Pipe *outPipe;
        Function *computeMe;
        Attribute IA;
        Attribute DA;

    public:
        friend void* RunSum(void *sr);
        void Run (Pipe &inPipe, Pipe &outPipe, Function &computeMe);
        void WaitUntilDone ();
        void Use_n_Pages (int n){}
};
class GroupBy : public RelationalOp {
    friend void* RunGroupBy(void *);
    private:
        int runlen;
        Pipe* inPipe;
        Pipe* outPipe;
        OrderMaker *groupAtts;
        Function *computeMe;
	public:
	void Run (Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe);
	void WaitUntilDone ();
	void Use_n_Pages (int n);
};
class WriteOut : public RelationalOp 
{
    private:
        Pipe* inPipe;
        FILE* outFile;
        Schema * mySchema;
	public:
    friend void* runWriteOut(void *);
	void Run (Pipe &inPipe, FILE *outFile, Schema &mySchema);
	void WaitUntilDone ();
	void Use_n_Pages (int n){}
};
void * runSelectFile (void * );
void * runSelectPipe (void *);
void * runProject (void *);
void * runJoin(void *);
void addRecordToPipe(Record& record, Pipe* outPipe, CNF* cnf, Record* literal,ComparisonEngine&  compEngine);
void * runDuplicateRemoval(void *);
void * runWriteOut(void *);
void * RunSum(void *);
void * RunGroupBy(void *);
Record constructSumRecord(Type type, double val);
#endif
