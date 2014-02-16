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
void* workerFunc(void *bigQ);
int comparator(Record r1,Record r2);
File* createRuns(int runlen,Pipe in,Pipe out);
void writeRunToFile(File* file, vector<Record> &list);
void mergeRunsFromFile(File* file, int runLength,Pipe out,OrderMaker *orderMaker);
static OrderMaker sortOrder;

#endif
