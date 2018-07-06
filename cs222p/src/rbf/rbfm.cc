
#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
	pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return pfm->openFile(fileName,fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	void *pageData = malloc(PAGE_SIZE);
	void *formattedData = malloc(PAGE_SIZE);
	RecordOffset rOffset;
	PageInfo pageInfo;

	//find a page with enough space for record inserting
	short int dataSize = convertToFormattedData(recordDescriptor, data, formattedData);
	rid.pageNum = findAvailablePage(fileHandle, dataSize, pageData);

	//all exist pages have no enough space, append new one
	if( rid.pageNum == fileHandle.getNumberOfPages() ){
		rid.slotNum = 0;
		//set the first slot descriptor in the page
		rOffset.offset = 0;
		rOffset.length = dataSize;
		memcpy( pageData, formattedData, dataSize );
		memcpy( (char*)pageData+PAGE_SIZE-sizeof(PageInfo)-sizeof(RecordOffset), &rOffset, sizeof(RecordOffset));
		//set the page info descriptor
		pageInfo.numOfSlots = 1;
		pageInfo.recordSize = rOffset.offset + rOffset.length;
		memcpy( (char*)pageData+PAGE_SIZE-sizeof(PageInfo) , &pageInfo, sizeof(PageInfo));
		//write record data and corresponding descriptor to the page
		fileHandle.appendPage(pageData);

	//one of the exist pages has enough space.
	//since this page been read in findAvailablePage function, and the data of this page has been store in pageData pointer.
	//we don't need to read the page from file again for reducing disk IO.
	}else{
		memcpy( &pageInfo, (char*)pageData+PAGE_SIZE-sizeof(PageInfo), sizeof(PageInfo));
		//reuse the slot of deleted record
		bool slotOfDelRec = false;
		for(int i = 0; i < pageInfo.numOfSlots; i++) {
			memcpy( &rOffset, (char*)pageData+PAGE_SIZE-sizeof(PageInfo)-sizeof(RecordOffset) * (i+1), sizeof(RecordOffset) );
			if(rOffset.length == DeleteMark) {
				rid.slotNum = i;
				slotOfDelRec = true;
				break;
			}
		}
		if(!slotOfDelRec) {	//current page has no slot of deleted record, append a new slot
			rid.slotNum = pageInfo.numOfSlots;
			pageInfo.numOfSlots++;
		}
		rOffset.offset = pageInfo.recordSize;
		rOffset.length = dataSize;
		pageInfo.recordSize += rOffset.length;
		memcpy( (char*)pageData+PAGE_SIZE-sizeof(PageInfo)-sizeof(RecordOffset) * (rid.slotNum+1), &rOffset, sizeof(RecordOffset) );
		memcpy( (char*)pageData+PAGE_SIZE-sizeof(PageInfo) , &pageInfo, sizeof(PageInfo));
		memcpy( (char*)pageData+rOffset.offset , formattedData, dataSize);
		fileHandle.writePage(rid.pageNum, pageData);
	}

	free(pageData);
	free(formattedData);
	return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    void *pageData = malloc(PAGE_SIZE);

    if(fileHandle.readPage(rid.pageNum, pageData) == -1) {
    		return -1;
    }

    RC rc = readRecordFromPage(fileHandle, recordDescriptor, rid, pageData, data);
	free(pageData);
    if(rc == -1)
    		return -1;
	return 0;
}

RC RecordBasedFileManager::readRecordFromPage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const void *pageData, void *data) {
    RecordOffset recordOffset;
    memcpy(&recordOffset, (char *)pageData + PAGE_SIZE - sizeof(PageInfo) - sizeof(RecordOffset) * (rid.slotNum + 1), sizeof(RecordOffset));

    if(recordOffset.length == DeleteMark) {
//		cout << "Deleted record can't be read!" << endl;
		data = NULL;
		return -1;
	}

	RID newRid;
    if(recordOffset.length == TombstoneMark) {
    		memcpy(&newRid, (char *)pageData + recordOffset.offset, sizeof(RID));
    		return readRecord(fileHandle, recordDescriptor, newRid, data);
    }

    void *formattedData = malloc(PAGE_SIZE);
    memcpy(formattedData, (char *)pageData + recordOffset.offset, recordOffset.length);
    convertFromFormattedData(recordDescriptor, data, formattedData);
	free(formattedData);
	return 0;
}


RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	getDataSize(recordDescriptor, data, true);
	return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
	void *pageData = malloc(PAGE_SIZE);
	PageInfo pageInfo;
	RecordOffset recordOffset;
	if(fileHandle.readPage(rid.pageNum, pageData) == -1) {
	    	return -1;
	}

	memcpy(&recordOffset, (char *)pageData + PAGE_SIZE - sizeof(PageInfo) - sizeof(RecordOffset) * (rid.slotNum + 1), sizeof(RecordOffset));
	memcpy(&pageInfo, (char *)pageData+PAGE_SIZE-sizeof(PageInfo), sizeof(PageInfo));

//	cout << "RID[" << rid.pageNum << ", " << rid.slotNum << "] " << "deleting! PageInfo: NumbeOfRecord, RecordSize = " << pageInfo.numOfSlots << ", " << pageInfo.recordSize << endl;
//	cout << "RID[" << rid.pageNum << ", " << rid.slotNum << "] " << "deleting! RecordOffset: offset, length = " << recordOffset.offset << ", " << recordOffset.length << endl;

	//delete all address of tombstone
	if(recordOffset.length == TombstoneMark) {
		recordOffset.length = sizeof(RID);
		RID newRid;
		memcpy(&newRid, (char *)pageData+recordOffset.offset, sizeof(RID));
		deleteRecord(fileHandle, recordDescriptor, newRid);
	}

	shiftData(pageData, pageInfo, recordOffset, rid, recordOffset.length);
	recordOffset.length = DeleteMark;

//	cout << "RID[" << rid.pageNum << ", " << rid.slotNum << "] " << "deleted! PageInfo: NumbeOfRecord, RecordSize = " << pageInfo.numOfSlots << ", " << pageInfo.recordSize << endl;
//	cout << "RID[" << rid.pageNum << ", " << rid.slotNum << "] " << "deleted! RecordOffset: offset, length = " << recordOffset.offset << ", " << recordOffset.length << endl;

	memcpy((char *)pageData + PAGE_SIZE - sizeof(PageInfo) - sizeof(RecordOffset) * (rid.slotNum + 1), &recordOffset, sizeof(RecordOffset));
	memcpy((char *)pageData+PAGE_SIZE-sizeof(PageInfo), &pageInfo, sizeof(PageInfo));
	fileHandle.writePage(rid.pageNum, pageData);
	free(pageData);
	return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid) {
	void *pageData = malloc(PAGE_SIZE);
	void *formattedData = malloc(PAGE_SIZE);

	if(fileHandle.readPage(rid.pageNum, pageData) == -1) {
		return -1;
	}

	PageInfo pageInfo;
	RecordOffset recordOffset;
	RID currentRid = rid;
	memcpy(&recordOffset, (char *)pageData + PAGE_SIZE - sizeof(PageInfo) - sizeof(RecordOffset) * (rid.slotNum + 1), sizeof(RecordOffset));

	//get actual RID
	while(recordOffset.length == TombstoneMark) {
		memcpy(&currentRid, (char *)pageData + recordOffset.offset, sizeof(RID));
		fileHandle.readPage(currentRid.pageNum, pageData);
		memcpy(&recordOffset, (char *)pageData + PAGE_SIZE - sizeof(PageInfo) - sizeof(RecordOffset) * (currentRid.slotNum + 1), sizeof(RecordOffset));
	}
	memcpy(&pageInfo, (char *)pageData+PAGE_SIZE-sizeof(PageInfo), sizeof(PageInfo));
	short int dataSize = convertToFormattedData(recordDescriptor, data, formattedData);
	short int difference = recordOffset.length - dataSize;

//	cout << "Initial RID[" << rid.pageNum << ", " << rid.slotNum << "] " << "updating! recordOffset: offset-length " << recordOffset.offset << ", " << recordOffset.length << ". ";
//	cout << "difference: " << difference << endl;
//	cout << "Get Current RID[" << currentRid.pageNum << ", " << currentRid.slotNum << "] " << endl;

	short int freeSpace = PAGE_SIZE - pageInfo.recordSize - (pageInfo.numOfSlots+1)*sizeof(RecordOffset) - sizeof(PageInfo);
	if((difference + freeSpace) >= 0) {
		shiftData(pageData, pageInfo, recordOffset, (const RID)currentRid, difference);
		memcpy((char*)pageData+recordOffset.offset, formattedData, dataSize);
		recordOffset.length = dataSize;
	} else {
	//current page has no enough space, find a new address and store the updated record
		void *newPageData = malloc(PAGE_SIZE);	//use new void*. prevent previous pageInfo from being written into a new appended page
		RID newRid;
		PageInfo newPInfo;
		RecordOffset newROffset;
		newRid.pageNum = findAvailablePage(fileHandle, dataSize, newPageData);

//		cout << "No enough space, find a new RID: " << newRid.pageNum << ", " << newRid.slotNum << endl;
		if( newRid.pageNum == fileHandle.getNumberOfPages() ){	//new address points to a new page
			newRid.slotNum = 0;
			newROffset.offset = 0;
			newROffset.length = dataSize;
			memcpy( newPageData, formattedData, dataSize );
			memcpy( (char*)newPageData+PAGE_SIZE-sizeof(PageInfo)-sizeof(RecordOffset), &newROffset, sizeof(RecordOffset));
			newPInfo.numOfSlots = 1;
			newPInfo.recordSize = newROffset.offset + newROffset.length;
			memcpy( (char*)newPageData+PAGE_SIZE-sizeof(PageInfo) , &newPInfo, sizeof(PageInfo));
			fileHandle.appendPage(newPageData);
		} else {
			fileHandle.readPage(newRid.pageNum, newPageData);
			memcpy( &newPInfo, (char*)newPageData+PAGE_SIZE-sizeof(PageInfo), sizeof(PageInfo));

			bool slotOfDelRec = false;
			for(int i = 0; i < newPInfo.numOfSlots; i++) {
				memcpy( &newROffset, (char*)newPageData+PAGE_SIZE-sizeof(PageInfo)-sizeof(RecordOffset) * (i+1), sizeof(RecordOffset) );
				if(newROffset.length == 0) {
					newRid.slotNum = i;
					slotOfDelRec = true;
					break;
				}
			}
			if(!slotOfDelRec) {	//current page has no slot of deleted record, append a new slot
				newRid.slotNum = newPInfo.numOfSlots;
				newPInfo.numOfSlots++;
			}
			newROffset.offset = newPInfo.recordSize;
			newROffset.length = dataSize;
			newPInfo.recordSize += newROffset.length;

			memcpy( (char*)newPageData+PAGE_SIZE-sizeof(PageInfo)-sizeof(RecordOffset) * (newRid.slotNum+1), &newROffset, sizeof(RecordOffset) );
			memcpy( (char*)newPageData+PAGE_SIZE-sizeof(PageInfo) , &newPInfo, sizeof(PageInfo));
			memcpy( (char*)newPageData+newROffset.offset , formattedData, dataSize);
			fileHandle.writePage(newRid.pageNum, newPageData);
		}
		//modify the information of previous page, and shift data. because the slot of previous record has become tombstone
		shiftData(pageData, pageInfo, recordOffset, (const RID)currentRid, recordOffset.length-sizeof(RID));
		memcpy((char*)pageData+recordOffset.offset, &newRid, sizeof(RID));
		recordOffset.length = TombstoneMark;
		free(newPageData);
	}
	memcpy((char *)pageData + PAGE_SIZE - sizeof(PageInfo) - sizeof(RecordOffset) * (currentRid.slotNum + 1), &recordOffset, sizeof(RecordOffset));
	memcpy((char *)pageData+PAGE_SIZE-sizeof(PageInfo), &pageInfo, sizeof(PageInfo));
	fileHandle.writePage(currentRid.pageNum, pageData);

	free(formattedData);
	free(pageData);
	return 0;
}


// this function is used to calculate the orignal data format's size from given test data
// put the flag here for options of printing
size_t RecordBasedFileManager::getDataSize(const vector<Attribute> &recordDescriptor, const void *data, bool printFlag) {
	int offset = 0;
	//initialize nullFieldsIndicator
	int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    //read the value of nullFieldsIndicator from record (pointer)
    memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);
    memcpy(nullFieldsIndicator, data, nullFieldsIndicatorActualSize);

    //start to read a record, offset move to the first actual field of the record
    offset += nullFieldsIndicatorActualSize;
    //scan every bit of the nullFieldsIndicator
    for(int i=0; i<recordDescriptor.size(); i++){
		Attribute attribute = recordDescriptor[i];
		string name = attribute.name;
		AttrType type = attribute.type;

//		if(printFlag) printf("<%d> %s[%d]: ",i,name.c_str(),length);	//print description of one field
		if(printFlag) printf("%s: ",name.c_str());
		if( nullFieldsIndicator[i/8] & (1 << (7-(i%8)) ) ){	// the nth field is corresponding to the (7-(k%8))th bit of the [k/8] bype of the nullFieldsIndicator
			if(printFlag) printf("NULL\n");
			continue;
		}

		void *buffer;
		if( type == TypeVarChar ){
			//use buffer to read the length of varChar
			buffer = malloc(sizeof(int));
			memcpy( buffer , (char*)data+offset, sizeof(int));
			offset += sizeof(int);
			int varCharLength = *(int*)buffer;
//			if(printFlag) printf("%i ",varCharLength);
			free(buffer);
			//use buffer to read the content of varChar
			buffer = malloc(varCharLength+1);  // null terminator
			memcpy( buffer, (char*)data+offset, varCharLength);
			offset += varCharLength;
			((char *)buffer)[varCharLength]='\0';
			if(printFlag) printf("%s\n",buffer);
			free(buffer);
			continue;
		}

		size_t size;
		if( type == TypeReal ){
			size = sizeof(float);
			buffer = malloc(size);
			memcpy( buffer , (char*)data+offset, size);
			offset += size;
			if(printFlag) printf("%f \n",*(float*)buffer);
		}else{	//type = TypeInt
			size = sizeof(int);
			buffer = malloc(size);
			memcpy( buffer , (char*)data+offset, size);
			offset += size;
			if(printFlag) printf("%i \n",*(int*)buffer);
		}
		free(buffer);
    }

    free(nullFieldsIndicator);
    return offset;
}

unsigned RecordBasedFileManager::findAvailablePage(FileHandle &fileHandle, const short int dataSize, void *pageData) {
	//if no page exist
	if(fileHandle.getNumberOfPages() == 0) {
		return 0;
	}

	unsigned pageNum = fileHandle.getNumberOfPages() - 1;
	short int freeSpace;
	PageInfo pageInfo;
	fileHandle.readPage(pageNum, pageData);

	memcpy(&pageInfo, (char *)pageData+PAGE_SIZE-sizeof(PageInfo), sizeof(PageInfo));
	freeSpace = PAGE_SIZE - pageInfo.recordSize - (pageInfo.numOfSlots+1)*sizeof(RecordOffset) - sizeof(PageInfo);

	//if last page in the file has enough space, return the pageNum
	if(freeSpace >= dataSize) {
		return pageNum;
	}

	//find a page with enough space within 0th and (numberOfPages-1)th page
	for(pageNum = 0; pageNum < fileHandle.getNumberOfPages() - 1; ++pageNum) {
		fileHandle.readPage(pageNum, pageData);
		memcpy(&pageInfo, (char *)pageData+PAGE_SIZE-sizeof(PageInfo), sizeof(PageInfo));
		freeSpace = PAGE_SIZE - pageInfo.recordSize - (pageInfo.numOfSlots+1)*sizeof(RecordOffset) - sizeof(PageInfo);
		if(freeSpace >= dataSize) {
			return pageNum;
		}

	}
	//All of the existing pages have no enough space. then create a new page for the record
	pageNum = fileHandle.getNumberOfPages();

	return pageNum;
}

short int RecordBasedFileManager::convertToFormattedData(const vector<Attribute> &recordDescriptor, const void *data, void * formattedData) {
	short int offset = 0;	//offset for input data
	short int formattedOffset = 0;	//offset for formatted data
	int fieldNum = recordDescriptor.size();

	int nullFieldsIndicatorActualSize = ceil((double)fieldNum / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memcpy(nullFieldsIndicator, data, nullFieldsIndicatorActualSize);

    formattedOffset = formattedOffset + fieldNum * sizeof(short int);
    offset += nullFieldsIndicatorActualSize;

    //scan every bit of the nullFieldsIndicator
    for(int i=0; i<recordDescriptor.size(); i++){
		AttrType type = recordDescriptor[i].type;

		if(nullFieldsIndicator[i/8] & (1 << (7-(i%8)))) {
			memcpy((char*)formattedData+i*sizeof(short int), &formattedOffset, sizeof(short int));
			continue;
		}

		if(type == TypeVarChar){
			void *buffer;
			buffer = malloc(sizeof(int));
			memcpy(buffer , (char*)data+offset, sizeof(int));
			offset += sizeof(int);
			int varCharLength = *(int*)buffer;
			free(buffer);

			if(!varCharLength) {	//input an empty string but not NULL. manually insert a '\0' to indicate that it is not NULL.
				void *buffer = malloc(sizeof(char));
				((char *)buffer)[varCharLength] = '\0';
				memcpy((char*)formattedData+formattedOffset, buffer, sizeof(char));
				formattedOffset += sizeof(char);
				memcpy((char*)formattedData+i*sizeof(short int), &formattedOffset, sizeof(short int));
				free(buffer);
				continue;
			}

			memcpy((char*)formattedData+formattedOffset, (char*)data+offset, varCharLength);
			offset += varCharLength;
			formattedOffset += varCharLength;
			memcpy((char*)formattedData+i*sizeof(short int), &formattedOffset, sizeof(short int));
			continue;
		}

		size_t size;
		if(type == TypeReal)
			size = sizeof(float);
		else
			size = sizeof(int);
		memcpy((char*)formattedData+formattedOffset, (char*)data+offset, size);
		offset += size;
		formattedOffset += size;
		memcpy((char*)formattedData+i*sizeof(short int), &formattedOffset, sizeof(short int));
    }

    //keep the minimum length of each record
    if(formattedOffset < sizeof(RID)) {
    		for(int i = formattedOffset; i < sizeof(RID); i++) {
    			*((char *)formattedData+i) = '\0';
    		}
    		formattedOffset = sizeof(RID);
    }

    free(nullFieldsIndicator);
	return formattedOffset;
}

short int RecordBasedFileManager::convertFromFormattedData(const vector<Attribute> &recordDescriptor, void *data, const void *formattedData) {
	short int offset = 0;	//offset for input data
	short int preFormattedOffset = 0;	//offset for formatted data
	short int nextFormattedOffset = 0;
	int fieldNum = recordDescriptor.size();

	int nullFieldsIndicatorActualSize = ceil((double)fieldNum / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);


	preFormattedOffset = preFormattedOffset + fieldNum * sizeof(short int);
	offset += nullFieldsIndicatorActualSize;

	for(int i=0; i<fieldNum; i++){
		AttrType type = recordDescriptor[i].type;
		memcpy(&nextFormattedOffset, (char*)formattedData+i*sizeof(short int), sizeof(short int));

		if(nextFormattedOffset == preFormattedOffset) {
			nullFieldsIndicator[i/8] = nullFieldsIndicator[i/8] | (1 << (7-(i%8)));
			continue;
		}

		if(type == TypeVarChar){
			int varCharLength = nextFormattedOffset - preFormattedOffset;
			// check whether there is a '\0' char to indicate it is an empty string
			if(varCharLength == 1) {
				void *emptyBuffer = malloc(sizeof(char));
				memcpy(emptyBuffer, (char*)formattedData+preFormattedOffset, varCharLength);
				if(*((char*)emptyBuffer) == '\0') {
					varCharLength = 0;
					memcpy((char*)data+offset , &varCharLength, sizeof(int));
					offset += sizeof(int);
					offset += varCharLength;
					preFormattedOffset = nextFormattedOffset;
					free(emptyBuffer);
					continue;
				}
				free(emptyBuffer);
			}
			memcpy((char*)data+offset , &varCharLength, sizeof(int));
			offset += sizeof(int);
			memcpy((char*)data+offset, (char*)formattedData+preFormattedOffset, varCharLength);
			offset += varCharLength;
			preFormattedOffset = nextFormattedOffset;
			continue;
		}

		size_t size;
		if(type == TypeReal)
			size = sizeof(float);
		else
			size = sizeof(int);
		memcpy((char*)data+offset, (char*)formattedData+preFormattedOffset, size);
		offset += size;
		preFormattedOffset = nextFormattedOffset;
	}
	memcpy((char*)data, (char*)nullFieldsIndicator, nullFieldsIndicatorActualSize);
	free(nullFieldsIndicator);
	return offset;
}

RC RecordBasedFileManager::shiftData(void *pageData, PageInfo &pageInfo, RecordOffset &recordOffset, const RID rid, short int distance) {
	short int followROffset = recordOffset.length + recordOffset.offset;
	//if deleted record is the last one in the page, shifting is not needed
	if(followROffset != pageInfo.recordSize) {
		short int shiftDataSize = pageInfo.recordSize - followROffset;
		RecordOffset tempROffset;
		void *temp = malloc(shiftDataSize);
		memcpy(temp, (char*)pageData + followROffset, shiftDataSize);
		//if distance is positive, it means data will be moved forward (delete data or update a smaller record) and more free space is available
		memcpy((char*)pageData + followROffset - distance, temp, shiftDataSize);
		free(temp);

		for(int i = 0; i < pageInfo.numOfSlots; i++) {
			memcpy(&tempROffset, (char *)pageData + PAGE_SIZE - sizeof(PageInfo) - sizeof(RecordOffset) * (i + 1), sizeof(RecordOffset));
			if(tempROffset.offset > recordOffset.offset) {
				tempROffset.offset = tempROffset.offset - distance;
				memcpy((char *)pageData + PAGE_SIZE - sizeof(PageInfo) - sizeof(RecordOffset) * (i + 1), &tempROffset, sizeof(RecordOffset));
			}
		}
	}
//	cout << "RID[" << rid.pageNum << ", " << rid.slotNum << "] " << "shifting, PageInfo: RecordSize = " << pageInfo.recordSize << endl;
	pageInfo.recordSize = pageInfo.recordSize - distance;
//	cout << "RID[" << rid.pageNum << ", " << rid.slotNum << "] " << "shifted, PageInfo: RecordSize = " << pageInfo.recordSize << endl;
	return 0;
 }

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data)
{
	void *record = malloc(PAGE_SIZE);
	readRecord(fileHandle, recordDescriptor, rid, record);

	if(getAttributeFromRecord(recordDescriptor, attributeName, record, data) == -1){
		free(record);
		return -1;
	}

	free(record);
	return 0;

}

RC RecordBasedFileManager::getAttributeFromRecord(const vector<Attribute> &recordDescriptor, const string &attributeName, const void *record, void *data)
{
	short int offset = 0;
	int length = 0;
	Attribute attr;

	int nullFieldsIndicatorActualSize = ceil((double)recordDescriptor.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	memcpy(nullFieldsIndicator, record, nullFieldsIndicatorActualSize);
	offset += nullFieldsIndicatorActualSize;
	memset(data, 0, 1);

	for(int i = 0; i < recordDescriptor.size(); i++) {
		attr = recordDescriptor.at(i);

		if(attr.name == attributeName) {
			if(nullFieldsIndicator[i/8] & (1 << (7-(i%8)))){
			((char *)data)[0] = ((char *)data)[0] | (1 << 7);
//			cout<<attr.name<<"     "<<attributeName<<endl;
//			((char *)data)[0] = '\0';
			free(nullFieldsIndicator);
			return 0;
			}
			if(attr.type == TypeVarChar) {
				memcpy(&length, (char*)record+offset, sizeof(int));
				memcpy((char *)data + 1, (char*)record+offset, sizeof(int)+length);
				free(nullFieldsIndicator);
				return 0;
			} else {
				memcpy((char *)data + 1, (char*)record+offset, sizeof(int));
				free(nullFieldsIndicator);
				return 0;
			}
		}

		//current attribute is not desired attribute, check next one
		if(nullFieldsIndicator[i/8] & (1 << (7-(i%8)))) {
			//corresponding data is NULL. offset += 0.
			continue;
		}
		if(attr.type == TypeVarChar) {	//skip a TypeVarChar, length + sizeof(int) bytes.
			memcpy(&length, (char*)record+offset, sizeof(int));
			offset += (length + sizeof(int));
		} else {		//skip a TypeFloat or TypeInt, 4 bytes.
			offset += sizeof(int);
		}
	}
	//the desired attribute is not found in the attributeDescriptor.
	free(nullFieldsIndicator);
	return -1;
}


RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute,
		const CompOp compOp, const void *value, const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
	return rbfm_ScanIterator.prepareIterator(fileHandle,recordDescriptor,conditionAttribute,compOp,value,attributeNames);
}

RC RBFM_ScanIterator::prepareIterator(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute,
                       const CompOp compOp, const void *value, const vector<string> &attributeNames)
{
//    assert( value != NULL && "value pointer should not be null" );
    this->rbfm = RecordBasedFileManager::instance();
    rbfm -> openFile(fileHandle.bindFileName, this->fileHandle);
    this->recordDescriptor = recordDescriptor;
    this->attributeNames = attributeNames;
    this->compOp = compOp;
    this->value = (char *)value;

    this->compResult = 0;
    this->c_rid.pageNum = 0;
    this->c_rid.slotNum = 0;
    this->pageData = malloc(PAGE_SIZE);
    this->record = malloc(PAGE_SIZE);
    this->conditionValue = malloc(PAGE_SIZE);

    if(fileHandle.readPage(c_rid.pageNum, pageData) == -1) {
    		return -1;
    }

    memcpy(&(this->pageInfo), (char*)pageData+PAGE_SIZE-sizeof(PageInfo), sizeof(PageInfo));

    for(int i = 0; i < recordDescriptor.size(); i++) {
    		compAttr = recordDescriptor.at(i);
    		if (compAttr.name == conditionAttribute) {
//    			cout << "compAttr.name: " << compAttr.name << endl;
    			return 0;
    		}
    }
    return -1;
}


RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data)
{
	while(1) {
		if(c_rid.slotNum >= pageInfo.numOfSlots) {
			c_rid.pageNum++;
			c_rid.slotNum = 0;
			if(c_rid.pageNum >= fileHandle.getNumberOfPages())
				return EOF;
			if(fileHandle.readPage(c_rid.pageNum, pageData) != 0)
				return -1;
			memcpy(&pageInfo, (char*)pageData+PAGE_SIZE-sizeof(PageInfo), sizeof(PageInfo));
		}

		// current c_rid is the slot of tombstone, skip this slot and read next one
		RecordOffset rOffset;
		memcpy(&rOffset, (char*)pageData+PAGE_SIZE-sizeof(PageInfo)-(c_rid.slotNum+1)*sizeof(RecordOffset), sizeof(RecordOffset));
		if(rOffset.length == TombstoneMark) {
			c_rid.slotNum++;
			continue;
		}

		// current c_rid is the slot of deleted record, skip this slot and read next one
		if(rbfm -> readRecordFromPage(fileHandle, recordDescriptor, c_rid, pageData, record) == -1) {
			c_rid.slotNum++;
			continue;
		}

		if(rbfm -> getAttributeFromRecord(recordDescriptor, compAttr.name, record, conditionValue) == -1)
			return -1;

		if(((((char *)conditionValue)[0] & (1 << 7)) && value == NULL && compOp == EQ_OP) || (compOp == NO_OP)) {                    //Field is NULL
		     readVectorAttribute(record, data);
		     rid.pageNum = c_rid.pageNum;
		     rid.slotNum = c_rid.slotNum;
		     c_rid.slotNum++;
		     return 0;
		}
		if((((char *)conditionValue)[0] & (1 << 7)) | (value == NULL)){
			   c_rid.slotNum++;
			   continue;
		}

		if(compAttr.type == TypeVarChar) {
			int len1 = 0;
			int len2 = 0;
			memcpy(&len1, (char *)conditionValue + 1, sizeof(int));
			memcpy(&len2, value, sizeof(int));
			char *pc1 = (char *)malloc(len1+1);
			char *pc2 = (char *)malloc(len2+1);
			memcpy(pc1, (char*)conditionValue+sizeof(int)+1, len1);
			memcpy(pc2, (char*)value+sizeof(int), len2);
			//!!!Warning!!! the end of char* should be '\0'!!! Otherwise there will be some strange chars at the end.
			pc1[len1] = '\0';
			pc2[len2] = '\0';
			compResult = strcmp(pc1, pc2);
////			cout << len1 << endl;
//			cout << len2 << endl;
//			cout << pc1 << endl;
//			cout << pc2 << endl;
//			cout << compResult << endl;
			free(pc1);
			free(pc2);
		} else if(compAttr.type == TypeInt) {
//			cout << "comparing TypeInt!!!" << endl;
			int v1;
			int v2;
			memcpy(&v1, (char *)conditionValue+1, sizeof(int));
			memcpy(&v2, value, sizeof(int));
			compResult = v1 - v2;
//			cout << "Comparing TypeInt in rbfm.getNextRecord()." << endl;
//			cout << v1 << endl;
//			cout << v2 << endl;
//			cout << compResult << endl;
//			cout<<"---------------------"<<endl;
		} else if(compAttr.type == TypeReal) {
			float v1;
			float v2;
			memcpy(&v1, (char *)conditionValue+1, sizeof(float));
			memcpy(&v2, value, sizeof(float));
//			cout << "comparing TypeReal!!!" << endl;
//			cout << v1 << endl;
//			cout << v2 << endl;
			if(v1>v2 && fabs(v1-v2)>1E-6)
				compResult = 1;
			else if (v1<v2 && fabs(v1-v2)>1E-6)
				compResult = -1;
			else
				compResult = 0;
//			cout << compResult << endl;
		}

		switch(compOp){
			case EQ_OP:
			{
				if(compResult == 0){
					readVectorAttribute(record, data);
//					int t_id;
//					memcpy(&t_id, (char *)data + 1, sizeof(int));
//					cout<<"t_id: "<<t_id<<endl;
					rid.pageNum = c_rid.pageNum;
					rid.slotNum = c_rid.slotNum;
					c_rid.slotNum++;
					return 0;
				}
				break;
			}
			case LT_OP:
			{
				if(compResult < 0){
					readVectorAttribute(record, data);
					rid.pageNum = c_rid.pageNum;
					rid.slotNum = c_rid.slotNum;
					c_rid.slotNum++;
					return 0;
				}
				break;
			}
			case LE_OP:
			{
				if(compResult <= 0){
					readVectorAttribute(record, data);
					rid.pageNum = c_rid.pageNum;
					rid.slotNum = c_rid.slotNum;
					c_rid.slotNum++;
					return 0;
				}
				break;
			}
			case GT_OP:
			{
				if(compResult > 0){
					readVectorAttribute(record, data);
					rid.pageNum = c_rid.pageNum;
					rid.slotNum = c_rid.slotNum;
					c_rid.slotNum++;
					return 0;
				}
				break;
			}
			case GE_OP:
			{
				if(compResult >= 0){
					readVectorAttribute(record, data);
					rid.pageNum = c_rid.pageNum;
					rid.slotNum = c_rid.slotNum;
					c_rid.slotNum++;
					return 0;
				}
				break;
			}
			case NE_OP:
			{
				if(compResult != 0){
					readVectorAttribute(record, data);
					rid.pageNum = c_rid.pageNum;
					rid.slotNum = c_rid.slotNum;
					c_rid.slotNum++;
					return 0;
				}
				break;
			}
			case NO_OP:
			{
				readVectorAttribute(record, data);
				rid.pageNum = c_rid.pageNum;
				rid.slotNum = c_rid.slotNum;
				c_rid.slotNum++;
				return 0;
			}
		}
		c_rid.slotNum++;
	}

}



RC RBFM_ScanIterator::readVectorAttribute(void *record, void *data){
	Attribute attr;
	int null_bit = ceil(1.0*attributeNames.size()/8);
	int null_bit_r = ceil(1.0*recordDescriptor.size()/8);
	unsigned char* null_indicator = (unsigned char*)malloc(null_bit);
	unsigned char* null_indicator_r = (unsigned char*)malloc(null_bit_r);

	int offset = null_bit;
	int offset_r = null_bit_r;


	memset(null_indicator, 0, null_bit);
	memcpy(null_indicator_r, record, null_bit_r);

//    cout<<attributeNames[0]<<endl;
	for(int i = 0; i < attributeNames.size(); i++) {

		offset_r = null_bit_r;

		for(int j = 0; j < recordDescriptor.size(); j++) {
			attr = recordDescriptor[j];
			if(null_indicator_r[j/8] & (1 << (7 - j % 8))){
				if(attributeNames[i] == attr.name) {
					null_indicator[i/8] = (null_indicator[i/8] | (1 << (7-(i%8))));
					break;
				}
				continue;
			}

			if(attr.type == TypeVarChar){
				int len = 0;
				memcpy(&len, (char *)record + offset_r, sizeof(int));
				offset_r += sizeof(int);
				if(attributeNames[i] == attr.name) {
					memcpy((char *)data + offset, &len, sizeof(int));
					offset += sizeof(int);
					memcpy((char *)data + offset, (char *)record + offset_r, len);
					offset += len;
				}
				offset_r += len;
			}

			if(attr.type == TypeInt){
				if(attributeNames[i] == attr.name) {
					memcpy((char *)data + offset, (char *)record + offset_r, sizeof(int));
					offset += sizeof(int);
				}
				offset_r += sizeof(int);
			}

			if(attr.type == TypeReal){
				if(attributeNames[i] == attr.name) {
					memcpy((char *)data + offset, (char *)record + offset_r, sizeof(float));
					offset += sizeof(float);
				}
				offset_r += sizeof(float);
			}

		}

	}
	memcpy((char *)data, (char *)null_indicator, null_bit);
	free(null_indicator);
	free(null_indicator_r);
	return 0;
}




RC RBFM_ScanIterator::close() {
	c_rid.pageNum = 0;
    free(pageData);
    free(record);
    free(conditionValue);
    rbfm->closeFile(fileHandle);
    return 0;
};

