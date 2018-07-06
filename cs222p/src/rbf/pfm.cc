#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
}


RC PagedFileManager::createFile(const string &fileName)
{
	const char *pFileName = fileName.c_str();
    if(access( pFileName, F_OK ) == 0) {
    		return -1;
	}
	//there is no such a file, create it.
    //create a header page for saving counter and so on.
    FILE *pFile;
    pFile = fopen(pFileName, "wb+");	//use wb+ mode to create a new file
    void *headPage = malloc(PAGE_SIZE);
    memset(headPage, 0, PAGE_SIZE);

    unsigned counter[4] = {0,0,0,0};
    memcpy(headPage, counter, sizeof(unsigned)*4);

    fseek(pFile, 0, SEEK_SET);
    fwrite(headPage, sizeof(char), PAGE_SIZE, pFile);
    free(headPage);

    fclose(pFile);
    	return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
    return remove(fileName.c_str());
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	if (access( fileName.c_str(), F_OK ) == 0){	//use access() to check whether the file is exist
		if(access( fileHandle.bindFileName.c_str(), F_OK ) == 0)	//one fileHandle can not bind with two files
			return -1;

		fileHandle.bindFileName = fileName;	//bind
		fileHandle.filePointer = fopen(fileName.c_str(), "rb+");	//use rb+ mode to change the file. Do not use wb+ mode, which will wipe the existed pages!
		fseek(fileHandle.filePointer, 0, SEEK_SET);

	    unsigned counter[4];
	    fread(counter, sizeof(unsigned), 4, fileHandle.filePointer);
//	    cout << "OPEN_Counter: R W A N - " << counter[0] << " " << counter[1]<< " " << counter[2]<< " " << counter[3] <<endl;
	    fileHandle.readPageCounter = counter[0];
	    	fileHandle.writePageCounter = counter[1];
	    fileHandle.appendPageCounter = counter[2];
	    fileHandle.numberOfPages = counter[3];

		return 0;
	    }

	return -1;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	unsigned counter[4];
    counter[0] = fileHandle.readPageCounter;
    	counter[1] = fileHandle.writePageCounter;
    	counter[2] = fileHandle.appendPageCounter;
    	counter[3] = fileHandle.numberOfPages;
    	fseek(fileHandle.filePointer, 0, SEEK_SET);
    fwrite(counter, sizeof(unsigned), 4, fileHandle.filePointer);
//    cout << "CLOSE_Counter: R W A N - " << counter[0] << " " << counter[1]<< " " << counter[2]<< " " << counter[3] <<endl;

	return fclose(fileHandle.filePointer);
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    numberOfPages = 0;
    filePointer = NULL;
}


FileHandle::~FileHandle()
{
}


RC FileHandle::readPage(PageNum pageNum, void *data)
{
	if(pageNum >= numberOfPages) {
//		cout << "can not read a non-existing page: " << endl;
		return -1;
	}
	//move filePointer to the position of page#pageNum. prepare for reading
	fseek(filePointer , (pageNum+1) * PAGE_SIZE ,SEEK_SET);
	//if this number differs from the count parameter, a reading error occurred. Because the block of file is page with PAGE_SIZE bytes.
	if(int result = fread(data, sizeof(char), PAGE_SIZE, filePointer) != PAGE_SIZE) {	//(char *)data is necessary?
//		cout << "a reading error occurred! result: " << result << endl;
		return -1;
	}
	readPageCounter++;
	return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	if(pageNum >= numberOfPages)
		return -1;
	//move filePointer to the position of page#pageNum. prepare for writing
	fseek(filePointer , (pageNum+1) * PAGE_SIZE ,SEEK_SET);
	//if this number differs from the count parameter, a writing error prevented the function from completing.
	if(int result = fwrite(data, sizeof(char), PAGE_SIZE, filePointer) != PAGE_SIZE) {
//		cout << "a writing error occurred! result: " << result << endl;
		return -1;
	}
	writePageCounter++;
	return 0;
}


RC FileHandle::appendPage(const void *data)
{
	//move filePointer to the end of the file. prepare for appending page
	fseek(filePointer, 0, SEEK_END);
	if(int result = fwrite(data, sizeof(char), PAGE_SIZE, filePointer) != PAGE_SIZE) {
//		cout << "a appending error occurred! result: " << result << endl;
		return -1;
	}
    appendPageCounter++;
    numberOfPages++;
	return 0;
}


unsigned FileHandle::getNumberOfPages()
{
	return numberOfPages;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;
    return 0;
}
