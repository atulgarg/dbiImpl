#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include <queue>
#include <cmath>
#include <cstdlib>
#include <algorithm>
using namespace std;

class BigQ {
	public:
		pthread_t worker;
		OrderMaker sortOrder;
		Pipe &inputPipe;
		Pipe &outputPipe;
		int runLength;	

	public:

		BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen);
		~BigQ ();
};
/**
 *  * Wrapper class for comparison of records.
 *   */
class RecordWrapper
{
	public:
		RecordWrapper();
		RecordWrapper(Record* record, OrderMaker &orderMaker);
		RecordWrapper(Record* record, OrderMaker &orderMaker,int runNumber);
		Record* record;
		OrderMaker orderMaker;
		int runNumber;
		static int comparator(const void* r1,const void* r2);

};
void* workerFunc(void *bigQ);
void createRuns(int runlen,Pipe& in,Pipe& out,File* file,OrderMaker& sortOrder);
void writeRunToFile(File* file, vector<RecordWrapper*> &list);
void mergeRunsFromFile(File* file, int runLength,Pipe& out,OrderMaker& orderMaker);
void copyRecordsToFile(Page pages[],File* file,int runlen,OrderMaker& orderMaker);
#endif
