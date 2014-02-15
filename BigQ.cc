#include "BigQ.h"

BigQ :: BigQ (Pipe &in, Pipe &out, OrderMaker &sortorder, int runlen) {
	// read data from in pipe sort them into runlen pages
	inputPipe = in;
	outputPipe = out;
	this->sortOrder = sortorder;
	runLength = runlen;
	int rc = pthread_create(&worker,NULL,workerFunc,(void*)this);
	if(rc){
		cerr<<"Not able to create worker thread"<<endl;
		exit(1);
	}
    // construct priority queue over sorted runs and dump sorted data 
 	// into the out pipe

    // finally shut down the out pipe
	out.ShutDown ();
}

BigQ::~BigQ () {
}
/**
 *
 */
int comparator(Record* r1,Record* r2,OrderMaker* orderUs)
{
	ComparisonEngine* compEngine = new ComparisonEngine();
	return (compEngine->Compare(r1,r2,orderUs) <= 0);
}
/**
 *
 */
void workerFunc(void *bigQ)
{
	BigQ *bq  = (BigQ*) bigQ;
	File* file;
	Pipe in = bq->inputPipe;
	Pipe out = bq->outputPipe;
	int runlen = bq->runLength;
	//create file of sorted runs.
	file = createRuns(runlen,in,out);
	//once a file is created of sorted runs merge each of the run.
}
/**
 * @method createRuns to create a file of sorted runs and number of runs.
 * @returns total number of runs created.
 *
 */
File* createRuns(int runlen,Pipe in,Pipe out)
{
	Record *currentRecord;
	File* file  = new File();
	Page* page = new Page();
	vector<Record> list;
	int numPages = 0;
	
	while(in.Remove(currentRecord) == 0)
	{
		//push record to list and put the same to page to keep a check of number of pages to compare with run length.
		list.push_back(*currentRecord);
		int status = page->Append(currentRecord);
		//if page was full
		if(status == 0)
		{
			numPages++;
			if(numPages == runlen)
			{
			  //Sort the content of Vector and place it in File.
			  sort(list.begin(),list.end(),comparator);
  			  writeRunToFile(file,list);
			  list.clear();			  
			}
		currentRecord =&(list.back());
		status = page->Append(currentRecord);
		}
	}
	return file;
}

/**
 * @method writeRunToFile to write records read from input pipe to File as sorted runs.
 * @param File* pointer to File where records need to be written.
 * @param vector<Record> list of Records to be written to file.
 * 
 */
void writeRunToFile(File* file, vector<Record> list)
{
	Page* page = new Page();
	for(int i=0;i<list.size();i++)
	{
		Record record = list[i];
		int status = page->Append(&record);
		//if record was not added to page i.e. page was full.
		if(status == 0)
		{
			off_t offSet = file->GetLength();
			file->AddPage(page,offSet);
			page = new Page();
			//to check if page is cleaned.
		}
		else
			continue;
		//if record was not added add the record from the list to page.
		record = (list[i]);
		page->Append(&record);
	}
}
class ComparisonClass
{
	ComparisonEngine* compEngine;
	OrderMaker *orderMaker;
	public:
	ComparisonClass(OrderMaker *orderMaker)
	{
		compEngine = new ComparisonEngine();
		this->orderMaker = orderMaker;
	}
	bool operator()(const pair<Record*,int> &lhs, const pair<Record*,int> &rhs)
	{
		Record* r1 = lhs.first;
		Record* r2 = rhs.first;
		return (compEngine->Compare(r1,r2,orderMaker) <= 0);
	}
};
/**
 * 
 */
void mergeRunsFromFile(File* file, int runLength,Pipe out)
{
	int fileLength = file->GetLength();
	int numRuns = ceil(fileLength*1.0f/runLength);
	std::priority_queue<pair<Record*,int>,std::vector<pair<Record*,int> > > priorityQueue;
	//Array of Pages to keep hold of current Page from each of run.
	Page* pageBuffers = new Page[runLength];
	//initialise each page with corresponding page in File.
	vector<int> offset;
	//initialise offset array to keep track of next page for each run.
	//Initialise each of the Page Buffers with the first page of each run.
	for(int i=0;i<fileLength;i+=runLength)
	{
		//initialise offset
		offset.push_back(i);
		
	}
			

}
