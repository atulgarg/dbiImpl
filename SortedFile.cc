#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "SortedFile.h"
#include "DBFile.h"
#include "Defs.h"
#include <pthread.h>
#include<fstream>

 /* @method initialiseSortedFile to initialise a new file instance with file name specified as parameter.
 * @param fileLen parameter specifying mode in which file needs to be opened.
 * @param fpath complete name of the file.
 * @returns 1 for success and 0 for failure.
 *
 */
int SortedFile:: initialiseSortedFile(int fileLen,char* fpath)
{
    queryOrderChanged = -1;
	myfile = new File();
    this->fpath = fpath;
	myfile->Open(fileLen,fpath);
    if(myfile == NULL)
		return 0;
	else
		return 1;
}
/**
 * Method to initialise each pipe with adequate Buffer Size						//To Check
 */
SortedFile::SortedFile(): GenericDBFile()
{
	in = new Pipe(100);
	out = new Pipe(100);
    read_page_marker = 0;
    writePage = 0;
    readPage = 0;
}
 /**
 * @method Create to create a new file instance with specified file name. Method writes 
 * SortOrder for indexing in metadata file as specified in startup.
 * @param f_path complete file name of the file.
 * @param f_type type of file to be created heap/sorted.
 * @param startup pointer to Wrapper object of run length and OrderMaker.
 */
int SortedFile::Create (char *f_path, fType f_type, void *startup) 
{
    //Open a new meta data file as filename.meta and write sortOrder info to file.
    char * metadataFilename = DBFile::getMetaDataFileName(f_path);
    ofstream metafile(metadataFilename,std::ofstream::app);
    sortInfo = (SortInfo*)startup;

    //Write the SortInfo Object to Meta Data File
    metafile<<sortInfo->runLength<<endl;
    
    OrderMaker* sortOrder = sortInfo->myOrder;
    
    metafile<<sortOrder->numAtts<<endl;

    for(int i=0;i<sortOrder->numAtts;i++)
    {
        metafile<<sortOrder->whichAtts[i]<<endl;
        metafile<<sortOrder->whichTypes[i]<<endl;
    }

    //set the initial mode of file to read since BigQ element is empty this time.
    fmode = readMode;
   
    //close the metadata file since it is not required until next open.
    metafile.close();
    
    return initialiseSortedFile(0,f_path);
}
/**
 * @method Load to load all set of records from text file specified by loadpath and write it to a BigQ instance which will eventually write records
 * to a bin file in sorted order in binary format.
 * @param Schema Object of schema 
 * @param char* name of file, from where data needs to be loaded.
 */
void SortedFile::Load (Schema &f_schema, char *loadpath) 
{
    queryOrderChanged = -1;
    FILE *txtFile = fopen(loadpath,"r");
	if(txtFile != NULL)
	{
		//possible error chance because of rec not getting initialised.
		Record nextRecord;
		while(nextRecord.SuckNextRecord(&f_schema,txtFile) != 0)
		{
			Add(nextRecord);
		}
	}
	else
	{
		cerr<<"No file with specified name "<< loadpath <<" exists. "<<"\n";
		exit(1);
	}
}
/**
 * @method Open to open a existing file specified by f_path. Method initially looks for metadata file with name as filename.meta and constructs SortInfo
 * object from metafile.
 * @param f_path name of the file to open.
 * @returns 1 for success and 0 for failure.
 */
int SortedFile::Open (char *f_path) 
{
    //Get metadata file name
	char * metaDataFileName = DBFile::getMetaDataFileName(f_path);
	
    //read sortInfo struct and later extract SortOrder.
    ifstream infile(metaDataFileName,std::ifstream::in);

    int fileType;
    infile>>fileType;

    sortInfo = new SortInfo();
    OrderMaker * sortOrder = new OrderMaker();
    sortInfo->myOrder = sortOrder; 
    
    infile>>sortInfo->runLength;
    infile>>sortOrder->numAtts;
    for(int i=0;i<sortOrder->numAtts;i++)
    {
        infile>>sortOrder->whichAtts[i];
        int whichType;
        infile>>whichType;
        if(Int == whichType)
            sortOrder->whichTypes[i]= Int;
        else if(Double == whichType)
            sortOrder->whichTypes[i] = Double;
        else
            sortOrder->whichTypes[i] = String;
    }
    infile.close();
    
    //set the initial mode for file as read mode.
    fmode = readMode;
    
    return initialiseSortedFile(1,f_path);
}

void SortedFile::MoveFirst () 
{
    if(isModeChanged(readMode)) 
    {
        initialiseForRead();
    }
    
    read_page_marker = 0;
    readPage = NULL;
    queryOrderChanged = -1;
}

int SortedFile::Close () 
{
	//if there are records in BigQ instance.
	if(fmode == writeMode)
	{
		//merge records of BigQ and file before actually closing the file.
		mergeRecords();
	}
    queryOrderChanged = -1;
	myfile->Close();
}
/**
 * @method isModeChanged to check if mode is changed from previous mode.
 * @returns true if mode is changed and false if mode is same as earlier mode.
 */
bool SortedFile::isModeChanged(Mode m)
{
	return (m != fmode);
}

/**
 * @method Add to add new record. Method checks if mode for sorted file is changed instance of BigQ is initialised.
 * New record is inserted to input pipe.
 *
 */
void SortedFile::Add (Record &rec) 
{
	queryOrderChanged = -1;
    //add record to input pipe for BigQ instance.
	if(isModeChanged(writeMode))
	{
		initialiseForWrite();
	}
	in->Insert(&rec);
}
/**
 * @method initialise to initialize instance of BigQ instance and input and output pipe.
 */
void SortedFile::initialiseForWrite()
{
	OrderMaker *sortOrder = sortInfo->myOrder;
	int runLength = sortInfo->runLength;
	//initialise BigQ instance.
	in = new Pipe(100);
	out = new Pipe(100);
	bigQ = new BigQ(*in, *out, *sortOrder, runLength);
	fmode = writeMode;	
}
/**
 * @method to initialise for Read if mode is switched from write mode to read mode.
 */
void SortedFile::initialiseForRead()
{
	mergeRecords();
	cleanUp();
	//MoveFirst();
    read_page_marker = 0;
	readPage = NULL;
    fmode = readMode;
}

int SortedFile:: GetNextRecord(Record& fromFile)
{
    int status = 0;
    if(readPage == NULL)
    {
        readPage = new Page();
        if(read_page_marker < myfile->GetLength()-1)
        {
            myfile->GetPage(readPage,read_page_marker);
        }
    }
    if(readPage != NULL)
    {
        status = readPage->GetFirst(&fromFile);
        //if was unable to read the next record fetch next page from disk and read.
        if(status == 0)
        {
            //empty the read page reinitialise and read the next page.
            read_page_marker++;
            readPage->EmptyItOut();
            if(read_page_marker < myfile->GetLength()-1)
            {
                myfile->GetPage(readPage,read_page_marker);
                if(readPage != NULL)
                    status = readPage->GetFirst(&fromFile);
                else
                    return 0;
            }
        }
    }
    return status;
}
/**
 * method mergeRecords to merge records from existing file and BigQ instance when mode changes from write mode to read mode.
 */
void SortedFile:: mergeRecords()
{
	in->ShutDown();
	Record fromPipe;
	Record fromFile;
    off_t readMarker = 0;	
    //create temporary file to push new records
    File* newSortedFile = new File();
    newSortedFile->Open(0,"temp.dat");
	
    //initialise record from file.
    int fileStatus = GetNextRecord(fromFile);
    
    //initialise record from pip
    int pipeStatus = out->Remove(&fromPipe);
	
    off_t writeMarker = 0;
	Page* writePage = new Page();
	while(fileStatus != 0 && pipeStatus != 0)
	{
		RecordWrapper* fromPipeWrapper = new RecordWrapper(&fromPipe,sortInfo->myOrder);
		RecordWrapper* fromFileWrapper = new RecordWrapper(&fromFile,sortInfo->myOrder);
		if(RecordWrapper::compareRecords(fromPipeWrapper,fromFileWrapper) > 0) 
		{
			addToTempFile(fromPipe,newSortedFile,writeMarker,writePage);
			pipeStatus = out->Remove(&fromPipe);
		}else
		{
			addToTempFile(fromFile,newSortedFile,writeMarker,writePage);
			fileStatus = GetNextRecord(fromFile);
		}
	}
	if(fileStatus == 0)
	{
		//copy content from pipe
	    do	
		{
			addToTempFile(fromPipe,newSortedFile,writeMarker,writePage);
		}while(out->Remove(&fromPipe)!=0);
	}else if(pipeStatus == 0)
	{
        do
		{
			addToTempFile(fromFile,newSortedFile,writeMarker,writePage);
		}while(GetNextRecord(fromFile)!=0);

	}
    newSortedFile->AddPage(writePage,writeMarker);
    newSortedFile->Close();
    rename("temp.dat", fpath);

	myfile = newSortedFile;
}
/**
 * @method to add new record to file.
 */
void SortedFile::addToTempFile(Record  &record,File *file,off_t &writeMarker,Page* writePage)
{
        
        //First try adding the record....if no more records can be added to current page then get a new page and try adding there 
        //before that add that page to file.
        if(writePage->Append(&record) == 0)
        {
                //add the write page to file and and allocate new page here to add record.
                file->AddPage(writePage,writeMarker);
                writePage->EmptyItOut();
                writePage->Append(&record);
                writeMarker++;
        }
}
/**
 *
 */
void SortedFile:: cleanUp()
{
	in->ShutDown();
	out->ShutDown();
	read_page_marker = 0;
	delete bigQ;
}
/**
 * @method GetNext to get next record from file. If mode changes from write to read, all records in BigQ are written to file by merging
 * records from file and BigQ, initialises for read and moving to first record reads page by page.
 *
 */
int SortedFile::GetNext (Record &fetchme) 
{
    if(isModeChanged(readMode))
    {
        initialiseForRead();
    }
    queryOrderChanged = -1; 
    return GetNextRecord(fetchme);
   
}
/**
 * @method GetNext to get next record in file which matches the passed literal.
 * @param Record object in which next record is fetched.
 * @param CNF instance to match the condition
 * @param Record literal to match value for the condition.
 * @returns 1 for success else 0.
 */
int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) 
{
    bool searchRequired = false;
    if(queryOrderChanged == -1)
    {
        queryOrderChanged = cnf.QueryMaker(queryOrder,*(sortInfo->myOrder));
        searchRequired = true;
    }
    if(queryOrderChanged == 0)
    {
        while(GetNextRecord(fetchme)!=0)
        {
            ComparisonEngine compEngine;
            //check if record satisfies CNF expression.
            if(compEngine.Compare (&fetchme, &literal, &cnf) == 1)
                return 1;
        }
    }else
    {  //Binary Search
        return BinarySearch(fetchme, cnf, literal,searchRequired);
    }
    //cout<<"GetNext::End"<<endl;
    
}
/**
 * @method BinarySearch to search for first record in file which matches
 * @param fetchme fetch next matching record in this instance.
 * @param CNF cnf expression to match for record.
 * @param literal Record instance to use for comparison.
 * @returns 1 for success and 0 if fails to get next record.
 */
int SortedFile::BinarySearch(Record &fetchme,CNF &cnf,Record &literal,bool searchRequired)
{
    ComparisonEngine compEngine;
    off_t fpIndex = 0;//read_page_marker;
    off_t lpIndex = myfile->GetLength()-1;
    off_t mid = floor((fpIndex + lpIndex)/2.0);
    if(readPage == NULL)
        readPage = new Page();
    if(searchRequired)
    {
        while(lpIndex <= fpIndex)
        {
            myfile->GetPage(readPage,mid);
            read_page_marker = mid;
            if(GetNextRecord(fetchme)!=0)
            {
                int result = compEngine.Compare(&fetchme, &literal,&queryOrder); 
                if (result < 0) 
                    lpIndex = mid;
                else if (result > 0) 
                    fpIndex = mid-1;
                else 
                    fpIndex = mid; // even if they're equal, we need to find the first such record
                mid = floor((lpIndex+fpIndex)/2.0);

            }else
            {
                //No more records to search
                return 0;
            }
        }
        myfile->GetPage(readPage,mid);
        read_page_marker = mid;
    }
    while(GetNextRecord(fetchme)!=0)
    {
        if (compEngine.Compare(&fetchme,&literal,&queryOrder) <= 1 )
        {
            //check if record satisfies CNF expression.
            if(compEngine.Compare (&fetchme, &literal, &cnf) == 1)
                return 1;
        }
    }
    return 0;

}
