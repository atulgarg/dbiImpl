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
/**
 *  * @method writeSortedOrdertoMetadata to write sortOrder to meta data file.
 *   */
void SortedFile::writeSortedOrdertoMetadata(FILE* metaFile,SortInfo* sortInfo)
{
	//Write the SortInfo Object to Meta Data File
	int status = fseek(metaFile, sizeof(int), SEEK_SET);
	fwrite(sortInfo,sizeof(char), sizeof(*sortInfo), metaFile);
}

int SortedFile:: initialiseSortedFile(int fileLen,char* fpath)
{
	myfile = new File();
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
}
 /**
 *
 */
int SortedFile::Create (char *f_path, fType f_type, void *startup) 
{
	//Open a new meta data file as filename.meta and write sortOrder info to file.
	FILE* metaFile = fopen(DBFile::getMetaDataFileName(f_path),"wb");
	sortInfo = (SortInfo*)startup;
	writeSortedOrdertoMetadata(metaFile, sortInfo);
	//set the initial mode of file to read since BigQ element is empty this time.
	fmode = read;
	//close the metadata file since it is not required until next open.
	fclose(metaFile);

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
	SortInfo sortInfo;
	FILE* metaFile = fopen(metaDataFileName,"rb");
	if(metaFile == NULL)
	{
		cerr<<"Unable to find meta data file"<<endl;
		exit(0);
	}
	fseek(metaFile, sizeof(int),SEEK_SET);
	fread(&sortInfo, sizeof(char), sizeof(SortInfo),metaFile);
	fclose(metaFile);

	return initialiseSortedFile(1,f_path);
}

void SortedFile::MoveFirst () {
}

int SortedFile::Close () 
{
	//if there are records in BigQ instance.
	if(fmode == write)
	{
		//merge records of BigQ and file before actually closing the file.
		mergeRecords();
	}
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
	//add record to input pipe for BigQ instance.
	if(isModeChanged(write))
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
	fmode = write;	
}
/**
 * @method to initialise for Read if mode is switched from write mode to read mode.
 */
void SortedFile::initialiseForRead()
{
	mergeRecords();
	cleanUp();
	MoveFirst();
	fmode = read;
}
/**
 * method mergeRecords to merge records from existing file and BigQ instance when mode changes from write mode to read mode.
 */
void SortedFile:: mergeRecords()
{
	in->ShutDown();
	Record fromPipe;
	Record fromFile;
	File* newSortedFile = new File();
	int fileStatus = GetNext(fromFile);
	int pipeStatus = out->Remove(&fromPipe);
	off_t writeMarker =0;
	Page* writePage = new Page();
	while(fileStatus!=0 && pipeStatus !=0)
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
			fileStatus = GetNext(fromFile);
		}
	}
	if(pipeStatus == 0)
	{
		//copy content from pipe
		while(out->Remove(&fromPipe)!=0)
		{
			addToTempFile(fromPipe,newSortedFile,writeMarker,writePage);
		}
	}else if(fileStatus == 0)
	{
		while(GetNext(fromFile)!=0)
		{
			addToTempFile(fromFile,newSortedFile,writeMarker,writePage);
		}
	}
	//need to check here if this works.
	//need to delete old file instance as well.
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
	write_page_marker = 0;
	delete bigQ;
}
/**
 * @method GetNext to get next record from file. If mode changes from write to read, all records in BigQ are written to file by merging
 * records from file and BigQ, initialises for read and moving to first record reads page by page.
 *
 */
int SortedFile::GetNext (Record &fetchme) 
{
	if(isModeChanged(read))
	{
		initialiseForRead();
	}
	int status = 0;
	if(readPage == NULL)
        {
                readPage = new Page();
                if(read_page_marker < myfile->GetLength())
                {
                        myfile->GetPage(readPage,read_page_marker);
                }
        }
        if(readPage != NULL)
        {
                status = readPage->GetFirst(&fetchme);
                //if was unable to read the next record fetch next page from disk and read.
                if(status == 0)
                {
                        //empty the read page reinitialise and read the next page.
                        read_page_marker++;
                        readPage->EmptyItOut();
                        if(read_page_marker < myfile->GetLength()-1)
                        {
                                myfile->GetPage(readPage,read_page_marker);
                                if(readPage!=NULL)
                                        status = readPage->GetFirst(&fetchme);
                                else
                                        return 0;
                        }
                }
        }
        return status;	
}

int SortedFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) 
{
    OrderMaker queryOrder;
    if(cnf.QueryMaker(queryOrder,*(sortInfo->myOrder)) == 0)
       return GetNext(fetchme);
    else
    {
        //Binary Search
        return BinarySearch(fetchme, queryOrder, cnf, literal);
    }
}
int SortedFile::BinarySearch(Record &fetchme,OrderMaker& queryOrder,CNF &cnf,Record &literal)
{
    ComparisonEngine compEngine;
    off_t fpIndex = 0;
    off_t lpIndex = myfile->GetLength()-1;
    off_t mid = floor((fpIndex + lpIndex)/2.0);
    while(lpIndex<fpIndex)
    {
        myfile->GetPage(readPage,mid);
        if(GetNext(fetchme)!=0)
        {
            int result = compEngine.Compare(&fetchme, &literal,&queryOrder); 
            if (result < 0) 
                lpIndex = mid;
            else if (result > 0) 
                lpIndex = mid-1;
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
    while(GetNext(fetchme)!=0 && compEngine.Compare(&fetchme,&literal,&queryOrder))
    {
        //check if record satisfies CNF expression.
       if(compEngine.Compare (&fetchme, &literal, &cnf) == 1)
           return 1;
    }
    return 0;

    
}
