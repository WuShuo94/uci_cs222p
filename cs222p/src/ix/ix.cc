
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
	const char *pFileName = fileName.c_str();
    if(access( pFileName, F_OK ) == 0) {
    		return -1;
	}

    FILE *pFile;
    pFile = fopen(pFileName, "wb+");	//use wb+ mode to create a new file
    void *headPage = malloc(PAGE_SIZE);
    memset(headPage, 0, PAGE_SIZE);

    unsigned counter[5] = {0,0,0,0,0};
    memcpy(headPage, counter, sizeof(unsigned)*5);

    fseek(pFile, 0, SEEK_SET);
    fwrite(headPage, sizeof(char), PAGE_SIZE, pFile);
    free(headPage);

    fclose(pFile);
    	return 0;
}

RC IndexManager::destroyFile(const string &fileName)
{
	return remove(fileName.c_str());
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
	if (access( fileName.c_str(), F_OK ) == 0){	//use access() to check whether the file is exist
		if(access( ixfileHandle.bindFileName.c_str(), F_OK ) == 0)	//one fileHandle can not bind with two files
			return -1;

		ixfileHandle.bindFileName = fileName;	//bind
		ixfileHandle.filePointer = fopen(fileName.c_str(), "rb+");	//use rb+ mode to change the file. Do not use wb+ mode, which will wipe the existed pages!
		fseek(ixfileHandle.filePointer, 0, SEEK_SET);

	    unsigned counter[5];
	    fread(counter, sizeof(unsigned), 5, ixfileHandle.filePointer);
	    ixfileHandle.ixReadPageCounter = counter[0];
	    ixfileHandle.ixWritePageCounter = counter[1];
	    ixfileHandle.ixAppendPageCounter = counter[2];
	    ixfileHandle.ixNumberOfPages = counter[3];
	    ixfileHandle.rootNodePage = counter[4];
		return 0;
	    }

	return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	unsigned counter[5];
    counter[0] = ixfileHandle.ixReadPageCounter;
    	counter[1] = ixfileHandle.ixWritePageCounter;
    	counter[2] = ixfileHandle.ixAppendPageCounter;
    	counter[3] = ixfileHandle.ixNumberOfPages;
    	counter[4] = ixfileHandle.rootNodePage;
    	fseek(ixfileHandle.filePointer, 0, SEEK_SET);
    fwrite(counter, sizeof(unsigned), 5, ixfileHandle.filePointer);
	return fclose(ixfileHandle.filePointer);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	void *pageData = malloc(PAGE_SIZE);
	void *insertKey = malloc(PAGE_SIZE);
	PageInfo pageInfo;
	RecordOffset rOffset;
	PageNum nextNodePage, nextLeafNode;
	short int entrySlot;
	short int nodeType;
	short int insertKeyLength = composeKey(attribute, key, rid, insertKey);


	//empty index, create the first node (leaf node) as the root node
//	cout<<ixfileHandle.ixNumberOfPages<<endl;
	if(ixfileHandle.ixNumberOfPages == 0) {
		ixfileHandle.rootNodePage = 0;
		nextLeafNode = UINT_MAX;
		nodeType = 1;
		rOffset.length = insertKeyLength;
		rOffset.offset = sizeof(PageNum);
		pageInfo.numOfSlots = 1;
		pageInfo.recordSize = insertKeyLength+sizeof(PageNum);
		memcpy((char*)pageData+PAGE_SIZE-sizeof(short int), &nodeType, sizeof(short int));
		memcpy((char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), &pageInfo, sizeof(PageInfo));
		memcpy((char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-sizeof(RecordOffset), &rOffset, sizeof(RecordOffset));
		memcpy(pageData, &nextLeafNode, sizeof(PageNum));
		memcpy((char*)pageData+rOffset.offset, insertKey, insertKeyLength);
//		cout<<"create the first root node"<<endl;
//		cout<<endl;
		ixfileHandle.appendPage(pageData);
		free(pageData);
		free(insertKey);
//		cout<<"=----------"<<"ok"<<endl;
		return 0;
    }

	//traverse the b+ tree index
	stack<unsigned> traverseStack;
	nextNodePage = ixfileHandle.rootNodePage;
	ixfileHandle.readPage(nextNodePage, pageData);
	memcpy(&nodeType, (char*)pageData+PAGE_SIZE-sizeof(short int), sizeof(short int));
//	cout<<"insert---nodeType: "<<nodeType<<endl;
	while(nodeType != 1) {	//traverse the tree until reaching leaf node
		traverseStack.push(nextNodePage);
		entrySlot = binSearch(pageData, attribute, insertKey, insertKeyLength+sizeof(PageNum));
		if(entrySlot == -1) {
			memcpy(&nextNodePage, pageData, sizeof(PageNum));
		} else {
			memcpy(&rOffset, (char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(entrySlot+1)*sizeof(RecordOffset), sizeof(RecordOffset));
			memcpy(&nextNodePage, (char*)pageData+rOffset.offset+rOffset.length-sizeof(PageNum), sizeof(PageNum));
		}
		ixfileHandle.readPage(nextNodePage, pageData);
		memcpy(&nodeType, (char*)pageData+PAGE_SIZE-sizeof(short int), sizeof(short int));
//		cout << "traverse, entrySlot = " << entrySlot << endl;
//		cout << "traverse, nextpage = " << nextNodePage << endl;
//		cout << "traverse, nodeType = " << nodeType << endl;
	}
	memcpy(&pageInfo, (char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), sizeof(PageInfo));
	int requiredSpace = insertKeyLength+sizeof(RecordOffset);
	int freeSpace = PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(pageInfo.numOfSlots)*sizeof(RecordOffset)-pageInfo.recordSize;

	if(requiredSpace <= freeSpace){
		entrySlot = binSearch(pageData, attribute, insertKey, insertKeyLength);
//		cout << "insert in leaf, freeSpace = " << freeSpace << endl;
//		cout << "insert in leaf, entrySlot = " << entrySlot << endl;
		if(insertEntryInPage(pageData, entrySlot, insertKey, insertKeyLength) == -1) {
			free(pageData);
			free(insertKey);
			return -1;
		}
		//write page
		ixfileHandle.writePage(nextNodePage, pageData);

	} else {	//split leaf node and modify parent node
		bool modifyParent = true;
		int splitPoint = 0;
		unsigned newPageId = ixfileHandle.ixNumberOfPages;
		void *newPage = malloc(PAGE_SIZE);
		void *tempData = malloc(PAGE_SIZE);
		PageInfo newPageInfo;
		//scan slot and find the first slot of which the recordoffset.offset > 1/2*PAGE_SIZE
		for(int i = 0; i < pageInfo.numOfSlots; i++) {
			memcpy(&rOffset, (char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(i+1)*sizeof(RecordOffset), sizeof(RecordOffset));
			if((rOffset.offset+sizeof(short int)+sizeof(PageInfo)+i*sizeof(RecordOffset)) >= 0.5*PAGE_SIZE) {
				splitPoint = i;
				break;
			}
		}
		if(splitPoint == 0)
			return -1;
		//prepare to append a new page, write records + pageInfo + nextLeafNode
		nodeType = 1;
		newPageInfo.numOfSlots = pageInfo.numOfSlots - splitPoint;
		newPageInfo.recordSize = pageInfo.recordSize - rOffset.offset + sizeof(PageNum);

		memcpy((char*)newPage, (char*)pageData, sizeof(PageNum));
		memcpy((char*)newPage+sizeof(PageNum), (char*)pageData+rOffset.offset, pageInfo.recordSize-rOffset.offset);
		memcpy((char*)newPage+PAGE_SIZE-sizeof(short int), &nodeType, sizeof(short int));
		memcpy((char*)newPage+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), &newPageInfo, sizeof(PageInfo));
		//modify slots.offset and write slots to the new page
		memcpy(tempData, (char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-pageInfo.numOfSlots*sizeof(RecordOffset), newPageInfo.numOfSlots*sizeof(RecordOffset));
		updateSlotDirectory(tempData, -1, newPageInfo, rOffset.offset-sizeof(PageNum));
		memcpy((char*)newPage+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-newPageInfo.numOfSlots*sizeof(RecordOffset), tempData, newPageInfo.numOfSlots*sizeof(RecordOffset));
		//modify old page info + nextLeafNode
		pageInfo.recordSize = rOffset.offset;
		pageInfo.numOfSlots = splitPoint;
		memcpy((char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), &pageInfo, sizeof(PageInfo));
		memcpy(pageData, &newPageId, sizeof(PageNum));
		//insert the new data entry
		entrySlot = binSearch(newPage, attribute, insertKey, insertKeyLength);

		memcpy(&nodeType, (char*)newPage+PAGE_SIZE-sizeof(short int), sizeof(short int));
		if(entrySlot == -1) {
			entrySlot = binSearch(pageData, attribute, insertKey, insertKeyLength);
//			cout << "split, entrySlot of new entry in old page = " << entrySlot << endl;
			if(insertEntryInPage(pageData, entrySlot, insertKey, insertKeyLength) == -1) {
				free(pageData);
				free(insertKey);
				free(tempData);
				free(newPage);
				return -1;
			}
		} else {
//			cout << "split, entrySlot of new entry in new page = " << entrySlot << endl;
			if(insertEntryInPage(newPage, entrySlot, insertKey, insertKeyLength) == -1) {
				free(pageData);
				free(insertKey);
				free(tempData);
				free(newPage);
				return -1;
			}
		}


//		cout << "split; new page =  " << newPageId << ", old page =  " << nextNodePage << ", splitPoint = " << splitPoint << endl;
//		cout  << endl;
		memcpy(&pageInfo, (char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), sizeof(PageInfo));
		memcpy(&newPageInfo, (char*)newPage+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), sizeof(PageInfo));
//		cout << "split; pageInfo.recordSize = " << pageInfo.recordSize << ", pageInfo.numOfSlots = " << pageInfo.numOfSlots << endl;
//		cout << "split; newPageInfo.recordSize = " << newPageInfo.recordSize << ", newPageInfo.numOfSlots = " << newPageInfo.numOfSlots << endl;
//		cout << endl;
		//write & append page
		ixfileHandle.writePage(nextNodePage, pageData);
		ixfileHandle.appendPage(newPage);

		//generate index entry based on the first entry of the appended page, and prepare to copy up
		memcpy(insertKey, (char*)newPage+sizeof(PageNum), rOffset.length);
		memcpy((char*)insertKey+rOffset.length, &newPageId, sizeof(PageNum));
		insertKeyLength = rOffset.length + sizeof(PageNum);
		requiredSpace = insertKeyLength + sizeof(RecordOffset);
		//insert the index entry into the parent node
		while(modifyParent) {
			//get parent node data
			if(traverseStack.empty()) {
				//grow a new layer, the new node becomes root node
				ixfileHandle.rootNodePage = ixfileHandle.ixNumberOfPages;
				nodeType = 0;
//				cout << "split, grow new root: " << ixfileHandle.rootNodePage << endl << endl;
				rOffset.length = insertKeyLength;
				rOffset.offset = sizeof(PageNum);
				pageInfo.numOfSlots = 1;
				pageInfo.recordSize = insertKeyLength+sizeof(PageNum);
				memcpy((char*)pageData+PAGE_SIZE-sizeof(short int), &nodeType, sizeof(short int));
				memcpy((char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), &pageInfo, sizeof(PageInfo));
				memcpy((char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-sizeof(RecordOffset), &rOffset, sizeof(RecordOffset));
				memcpy(pageData, &nextNodePage, sizeof(PageNum));
				memcpy((char*)pageData+rOffset.offset, insertKey, insertKeyLength);
				ixfileHandle.appendPage(pageData);
				modifyParent = false;
				break;
			}
			nextNodePage = traverseStack.top();
			traverseStack.pop();

			ixfileHandle.readPage(nextNodePage, pageData);
			memcpy(&pageInfo, (char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), sizeof(PageInfo));
			freeSpace = PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(pageInfo.numOfSlots)*sizeof(RecordOffset)-pageInfo.recordSize;
			if(requiredSpace <= freeSpace) {
				entrySlot = binSearch(pageData, attribute, insertKey, insertKeyLength);
//				cout << "insert in parent node = " << nextNodePage <<", pageInfo.numOfSlots = " << pageInfo.numOfSlots << ", pageInfo.recordSize = " << pageInfo.recordSize << endl;
//				cout << "insert in parent node = " << nextNodePage <<", entrySlot = " << entrySlot << endl;
//				cout << endl;
				//entrySlot == -1, the following is still correct??? yes
				if (insertEntryInPage(pageData, entrySlot, insertKey, insertKeyLength) == -1) {
//					cout << "insert failed!" << endl;
					free(pageData);
					free(insertKey);
					free(tempData);
					free(newPage);
					return -1;
				}
				modifyParent = false;
				ixfileHandle.writePage(nextNodePage, pageData);
				break;
			} else { //split parent node
				splitPoint = 0;
				newPageId = ixfileHandle.ixNumberOfPages;
				for(int i = 0; i < pageInfo.numOfSlots; i++) {
					memcpy(&rOffset, (char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(i+1)*sizeof(RecordOffset), sizeof(RecordOffset));
					if((rOffset.offset+sizeof(short int)+sizeof(PageInfo)+i*sizeof(RecordOffset)) >= 0.5*PAGE_SIZE) {
						splitPoint = i;
						break;
					}
				}
				if(splitPoint == 0)
					return -1;
				//the page-id of this entry stored in tempData will be the 1'st (left most) page-id of new non-leaf node.
				//the key of this entry will be pushed up
				//the page id of this entry will be the first page id of new appended page
				memcpy(tempData, (char*)pageData+rOffset.offset, rOffset.length);
				//prepare to append a new non-leaf page, write records + slots + pageInfo + nextLeafNode
				nodeType = 0;
				newPageInfo.numOfSlots = pageInfo.numOfSlots - splitPoint - 1;
				newPageInfo.recordSize = pageInfo.recordSize-rOffset.offset - rOffset.length + sizeof(PageNum);
				memcpy(newPage, (char*)tempData+rOffset.length-sizeof(PageNum), sizeof(PageNum));
				memcpy((char*)newPage+sizeof(PageNum), (char*)pageData+rOffset.offset+rOffset.length, pageInfo.recordSize-rOffset.offset-rOffset.length);
				memcpy((char*)newPage+PAGE_SIZE-sizeof(short int), &nodeType, sizeof(short int));
				memcpy((char*)newPage+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), &newPageInfo, sizeof(PageInfo));
				//modify slots.offset and write slots to the new appended page
				void *newTempData = malloc(PAGE_SIZE);
				memcpy(newTempData, (char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-pageInfo.numOfSlots*sizeof(RecordOffset), newPageInfo.numOfSlots*sizeof(RecordOffset));
				updateSlotDirectory(newTempData, -1, newPageInfo, rOffset.offset+rOffset.length-sizeof(PageNum));
				memcpy((char*)newPage+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-newPageInfo.numOfSlots*sizeof(RecordOffset), newTempData, newPageInfo.numOfSlots*sizeof(RecordOffset));
				free(newTempData);
				//modify old page info
				pageInfo.recordSize = rOffset.offset;
				pageInfo.numOfSlots = splitPoint;
				memcpy((char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), &pageInfo, sizeof(PageInfo));
				//insert the new index entry
				if(keyCompare(attribute, insertKey, insertKeyLength-sizeof(PageNum), tempData, rOffset.length-sizeof(PageNum)) == -1) {
					entrySlot = binSearch(pageData, attribute, insertKey, insertKeyLength);
					if(insertEntryInPage(pageData, entrySlot, insertKey, insertKeyLength) == -1) {
						free(pageData);
						free(insertKey);
						free(tempData);
						free(newPage);
						return -1;
					}
				} else {
					entrySlot = binSearch(newPage, attribute, insertKey, insertKeyLength);
					if(insertEntryInPage(newPage, entrySlot, insertKey, insertKeyLength) == -1) {
						free(pageData);
						free(insertKey);
						free(tempData);
						free(newPage);
						return -1;
					}
				}
				//write & append page
				ixfileHandle.writePage(nextNodePage, pageData);
				ixfileHandle.appendPage(newPage);
				//generate the index entry needed to be pushed up to the parent node
				memcpy(insertKey, tempData, rOffset.length);
				memcpy((char*)insertKey+rOffset.length-sizeof(PageNum), &newPageId, sizeof(PageNum));
				insertKeyLength = rOffset.length;
				requiredSpace = insertKeyLength + sizeof(RecordOffset);
			}

		}
		free(tempData);
		free(newPage);
	}
	free(pageData);
	free(insertKey);
	return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	void *compKey = malloc(PAGE_SIZE);
	void *pageData = malloc(PAGE_SIZE);
	void *deleteKey = malloc(PAGE_SIZE);
	RecordOffset rOffset;
	PageInfo pageInfo;
	short int nodeType;
	short int entrySlot;
	unsigned nextNodePage = ixfileHandle.rootNodePage;
	short int deleteKeyLength = composeKey(attribute, key, rid, deleteKey);

	ixfileHandle.readPage(ixfileHandle.rootNodePage, pageData);
	memcpy(&nodeType, (char *)pageData+PAGE_SIZE-sizeof(short int), sizeof(short int));
	// get the leafNodePage
	while(nodeType != 1){
		entrySlot = binSearch(pageData, attribute, deleteKey, deleteKeyLength+sizeof(PageNum));
		if(entrySlot == -1){
			memcpy(&nextNodePage, pageData, sizeof(PageNum));
		} else {
			memcpy(&rOffset, (char *)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(entrySlot+1)*sizeof(RecordOffset), sizeof(RecordOffset));
			memcpy(&nextNodePage, (char *)pageData+rOffset.offset+rOffset.length-sizeof(PageNum), sizeof(PageNum));
		}
		ixfileHandle.readPage(nextNodePage, pageData);
		memcpy(&nodeType, (char *)pageData+PAGE_SIZE-sizeof(short int), sizeof(short int));
	}

	//modify entry + PageInfo + Slot
	void *tempData = malloc(PAGE_SIZE);
	int conditionKeyLength;
	entrySlot = binSearch(pageData, attribute, deleteKey, deleteKeyLength);

//	cout << "delete in page id = " << nextNodePage << ", entry slot = " << entrySlot << endl;

	//exactly equal? if not, can not find condition key
	if(entrySlot==-1){
		free(pageData);
		free(compKey);
		free(deleteKey);
		free(tempData);
		return -1;
	}
	else {
		memcpy(&rOffset, (char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(entrySlot+1)*sizeof(RecordOffset), sizeof(RecordOffset));
	}
	memcpy((char *)compKey, (char *)pageData+rOffset.offset, rOffset.length);
	conditionKeyLength = rOffset.length;
	if(keyCompare(attribute, compKey, conditionKeyLength, deleteKey, deleteKeyLength) != 0){
		free(pageData);
		free(compKey);
		free(deleteKey);
		free(tempData);
		return -1;
	}

	memcpy(&pageInfo, (char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), sizeof(PageInfo));
	rOffset.offset += rOffset.length;
	//1.shift slots
	memcpy(tempData, (char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-pageInfo.numOfSlots*sizeof(RecordOffset), (pageInfo.numOfSlots-entrySlot-1)*sizeof(RecordOffset));
	updateSlotDirectory(tempData, entrySlot, pageInfo, deleteKeyLength);
	memcpy((char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(pageInfo.numOfSlots-1)*sizeof(RecordOffset),tempData, (pageInfo.numOfSlots-entrySlot-1)*sizeof(RecordOffset));
	//2.shift entry
	memcpy(tempData, (char*)pageData+rOffset.offset, pageInfo.recordSize-rOffset.offset);
	memcpy((char*)pageData+rOffset.offset-deleteKeyLength, tempData, pageInfo.recordSize-rOffset.offset);
	//3.modify PageInfo
	pageInfo.numOfSlots--;
	pageInfo.recordSize -= deleteKeyLength;
	memcpy((char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), &pageInfo, sizeof(PageInfo));
	ixfileHandle.writePage(nextNodePage, pageData);

	free(pageData);
	free(compKey);
	free(deleteKey);
	free(tempData);
    return 0;
}


RC IndexManager::updateSlotDirectory(void *slotDirectory, const int entryslot, PageInfo pageInfo, const short int offset){
	for(int i=0;i<pageInfo.numOfSlots-entryslot-1;i++){
		RecordOffset rOffset;
		memcpy(&rOffset, (char *)slotDirectory+i*sizeof(RecordOffset), sizeof(RecordOffset));
		rOffset.offset -= offset;
		memcpy((char *)slotDirectory+i*sizeof(RecordOffset), &rOffset, sizeof(RecordOffset));
	}
	return 0;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
	return ix_ScanIterator.prepareIterator(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
}

RC IX_ScanIterator::prepareIterator(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive){
	this->ix = IndexManager::instance();
//	if (ixfileHandle.temp) {
//	ix -> openFile(ixfileHandle.bindFileName, *(this->ixfileHandle));
//		(this->ixfileHandle)->temp = true;
//	} else
	this->ixfileHandle = &ixfileHandle;
	this->a = 1000;
	this->attribute = attribute;
//	cout<<"attribute:        "<<attribute.name<<endl;
	this->lowKeyInclusive = lowKeyInclusive;
	this->highKeyInclusive = highKeyInclusive;
	this->scanFinished = false;
	this->lowKey = malloc(PAGE_SIZE);
	this->highKey = malloc(PAGE_SIZE);
	this->pageData = malloc(PAGE_SIZE);
	this->lowCompKey = malloc(PAGE_SIZE);
	this->highCompKey = malloc(PAGE_SIZE);
	this->highKeyNUll = false;

//	float posInfinity = INFINITY/2, negInfinity = -INFINITY/2;
	unsigned minValue = 0, maxValue = UINT_MAX;

//	if(ixfileHandle.filePointer == NULL) return -1;
	if((*(this->ixfileHandle)).filePointer==NULL) return -1;

//	ixfileHandle.readPage(ixfileHandle.rootNodePage, pageData);
	if((*(this->ixfileHandle)).readPage((*(this->ixfileHandle)).rootNodePage, pageData) == -1) {
//		cout << "initial read failed" << endl;
		return -1;
	}


	memcpy(&nodeType, (char *)pageData+PAGE_SIZE-sizeof(short int), sizeof(short int));

	if(lowKey == NULL){
//		cout<<"!!!!THIS IS NULL!!!!!"<<"    nodeType:"<<nodeType<<"   rootNP:"<<(*(this->ixfileHandle)).rootNodePage<<endl;
		c_rid.pageNum = (*(this->ixfileHandle)).rootNodePage;
		while(nodeType == 0){
			memcpy(&nextNodePage, (char *)pageData, sizeof(unsigned));
//			ixfileHandle.readPage(nextNodePage, pageData);
			(*(this->ixfileHandle)).readPage(nextNodePage, pageData);
//			cout<<"nextNodePage:"<<nextNodePage<<endl;
			memcpy(&nodeType, (char *)pageData+PAGE_SIZE-sizeof(short int), sizeof(short int));
			c_rid.pageNum = nextNodePage;
		}
//		c_rid.pageNum = nextNodePage;
		c_rid.slotNum = 0;
	}
	else{
//		cout<<"!!!!THIS IS NOT NULL"<<endl;
		if(this->lowKeyInclusive){
			this->i_rid.pageNum = minValue;
			this->i_rid.slotNum = minValue;
		}
		else{
			this->i_rid.pageNum = maxValue;
			this->i_rid.slotNum = maxValue;
		}
		if(attribute.type == TypeVarChar){
			int length;
			memcpy(&length, (char *)lowKey, sizeof(int));
			memcpy(this->lowKey, lowKey, sizeof(int)+length);
		}
		else{
			memcpy(this->lowKey, lowKey, 4);
		}

		lowKeyLength = ix->composeKey(attribute, this->lowKey, i_rid, this->lowCompKey);
		while(nodeType==0){
			short int slotIndex;
			RecordOffset rOffset;
			slotIndex = ix->binSearch(pageData, attribute, lowCompKey, lowKeyLength+sizeof(PageNum));
			if(slotIndex==-1){
				memcpy(&rOffset, (char *)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(slotIndex+2)*sizeof(RecordOffset), sizeof(RecordOffset));
			}
			else{
				memcpy(&rOffset, (char *)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(slotIndex+1)*sizeof(RecordOffset), sizeof(RecordOffset));
				rOffset.offset += rOffset.length;
			}
			memcpy(&nextNodePage, (char *)pageData+rOffset.offset-sizeof(unsigned), sizeof(unsigned));
//			cout<<"slotIndex:"<<slotIndex<<"  nextNodePage:"<<nextNodePage<<endl;
			(*(this->ixfileHandle)).readPage(nextNodePage, pageData);
			memcpy(&nodeType, (char *)pageData+PAGE_SIZE-sizeof(short int), sizeof(short int));
		}

		lowSlotID = ix->binSearch(pageData, attribute, lowCompKey, lowKeyLength);
		if(lowSlotID == -1){
			c_rid.slotNum = ++lowSlotID;
			c_rid.pageNum = nextNodePage;
		}
		else{
			void *key = malloc(PAGE_SIZE);
			RecordOffset rOffset;
			memcpy(&rOffset, (char *)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(lowSlotID+1)*sizeof(RecordOffset), sizeof(RecordOffset));
			memcpy(key, (char *)pageData+rOffset.offset, rOffset.length);
			if(ix->keyCompare(attribute, lowCompKey, lowKeyLength, key, rOffset.length)==0){
				if(this->lowKeyInclusive) c_rid.slotNum = lowSlotID;
				else c_rid.slotNum = ++lowSlotID;
				c_rid.pageNum = nextNodePage;
			}
			else{
				c_rid.slotNum = ++lowSlotID;
				c_rid.pageNum = nextNodePage;
			}
			free(key);
		}

	}

	if(highKey == NULL){
//		free(this->highKey);
//		*(char*)(this->highKey) = NULL;
//		this->highKey = NULL;
//		cout<<"the highKey is NULL"<<endl;
		this->highKeyNUll = true;
	}
	else{
		if(this->highKeyInclusive){
			this->i_rid.pageNum = maxValue;
			this->i_rid.slotNum = maxValue;
		}
		else{
			this->i_rid.pageNum = minValue;
			this->i_rid.slotNum = minValue;
		}
		if(attribute.type == TypeVarChar){
			int length;
			memcpy(&length, (char *)highKey, sizeof(int));
			memcpy(this->highKey, highKey, sizeof(int)+length);
		}
		else{
			memcpy(this->highKey, highKey, 4);
		}

		highKeyLength = ix->composeKey(attribute, this->highKey, i_rid, this->highCompKey);
	}

	memcpy(&pageInfo, (char *)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), sizeof(PageInfo));
//	cout<<"highKeyNull after prepare:     "<<this->highKeyNUll<<endl;
	return 0;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
	if(scanFinished) return -1;
	while(1){

//		cout<<"c_rid.slotNum   :"<<c_rid.slotNum<<"     pageInfo.numOfSlots    :"<<pageInfo.numOfSlots<<"    highKeyNUll:    "<<highKeyNUll<<endl;
		//judge whether the scanning of current page has been finished
		if(c_rid.slotNum >= pageInfo.numOfSlots){
			memcpy(&rightNodePage, (char *)pageData, sizeof(unsigned));
//			cout<<"rightNodePage:    "<<rightNodePage<<endl;
			if(rightNodePage==-1){
				scanFinished = true;
//				cout<<"read the last page"<<endl;
				return -1;
			}
//			cout<<"STEP_TWO"<<endl;
			(*(this->ixfileHandle)).readPage(rightNodePage, pageData);
			c_rid.pageNum = rightNodePage;
			c_rid.slotNum = 0;
			memcpy(&pageInfo, (char *)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), sizeof(PageInfo));
		}

		//
//		cout<<"highKeyNull:    "<<highKeyNUll<<endl;
		if(highKeyNUll){
//			cout<<"STEP_THREE"<<endl;
			highSlotID = pageInfo.numOfSlots-1;
//			cout<<"highSlotID when highkey null:  "<<highSlotID<<endl;
		}
		else{
//			cout<<"STEP_FOUR"<<endl;
//			cout<<"----highkeylength----:"<<highKeyLength<<endl;
			highSlotID = ix->binSearch(pageData, attribute, highCompKey, highKeyLength);
			if(highSlotID != -1){
				void *key1 = malloc(PAGE_SIZE);
				RecordOffset rOffset;
				memcpy(&rOffset, (char *)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(highSlotID+1)*sizeof(RecordOffset), sizeof(RecordOffset));
				memcpy(key1, (char *)pageData+rOffset.offset, rOffset.length);
				if(ix->keyCompare(attribute, highCompKey, highKeyLength, key1, rOffset.length)==0){
					if(!highKeyInclusive) highSlotID--;
				}
				free(key1);
			}
		}

//		cout<<"highSlotID:    "<<highSlotID<<endl;
		//3 situation of highSlotID:
		if(highSlotID==-1){
			if(pageInfo.numOfSlots == 0){  //when the leaf node is empty
//				cout<<"STEP pagenum is ZERO"<<endl;
				c_rid.slotNum++;
				continue;
			}
			else{
//				cout<<"STEP pagenum is not ZERO"<<endl;
				scanFinished = true;
				return -1;
			}
		}
		else if(highSlotID == pageInfo.numOfSlots-1) {
			RecordOffset rOffset;
			memcpy(&rOffset, (char *)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(c_rid.slotNum+1)*sizeof(RecordOffset), sizeof(RecordOffset));
			memcpy(key, (char *)pageData+rOffset.offset, rOffset.length-sizeof(RID));
			memcpy(&rid, (char *)pageData+rOffset.offset+rOffset.length-sizeof(RID), sizeof(RID));
			c_rid.slotNum++;
			return 0;
		}
		else if(highSlotID < pageInfo.numOfSlots-1){
			if(c_rid.slotNum > highSlotID) {
				scanFinished = true;
				return -1;
			}
			else{
				RecordOffset rOffset;
				memcpy(&rOffset, (char *)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(c_rid.slotNum+1)*sizeof(RecordOffset), sizeof(RecordOffset));
				memcpy(key, (char *)pageData+rOffset.offset, rOffset.length-sizeof(RID));
				memcpy(&rid, (char *)pageData+rOffset.offset+rOffset.length-sizeof(RID), sizeof(RID));
				c_rid.slotNum++;
				return 0;
			}
		}
	}
}

RC IX_ScanIterator::close()
{
	free(lowCompKey);
	free(highCompKey);
	free(pageData);
	free(lowKey);
	free(highKey);
	// free(ixfileHandle);
//	if ((*ixfileHandle).temp)
//	ix -> closeFile((*ixfileHandle));
	return 0;
}



void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) {
	void *pageData = malloc(PAGE_SIZE);
	ixfileHandle.readPage(ixfileHandle.rootNodePage, pageData);
	printBtree(ixfileHandle, attribute, pageData, 0, ixfileHandle.rootNodePage);
//	short int nodeType;
//	PageInfo pageInfo;
//	memcpy(&nodeType, (char *)pageData+PAGE_SIZE-sizeof(short int), sizeof(short int));
//	memcpy(&pageInfo, (char *)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), sizeof(PageInfo));
//	cout<<"pageInfo.numOfSlots:"<<pageInfo.numOfSlots<<"   nodeType:"<<nodeType<<endl;
	free(pageData);
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute, void *pageData, int depth, PageNum nextNodePage){


	short int nodeType;
	RecordOffset rOffset;
	PageInfo pageInfo;
	PageNum theFirstLeftPage, theRightPage;
	ixfileHandle.readPage(nextNodePage,pageData);
	memcpy(&nodeType, (char *)pageData+PAGE_SIZE-sizeof(short int), sizeof(short int));
	memcpy(&pageInfo, (char *)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), sizeof(PageInfo));
//	cout<<"pageInfo.numOfSlots:"<<pageInfo.numOfSlots<<"   nodeType"<<nodeType<<endl;

	for(int i=0;i<depth;i++){cout<<"\t";}

	if(nodeType == 0){
		vector<PageNum> path;
		RID rid;

//		cout<<"{\n\"keys\":[";
		memcpy(&theFirstLeftPage, (char *)pageData, sizeof(PageNum));
		path.push_back(theFirstLeftPage);

		for(int i=0;i<pageInfo.numOfSlots;i++){
//			void *beforeKey = malloc(PAGE_SIZE);
//			if(i!=0) cout<<",";
			memcpy(&rOffset,(char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(i+1)*sizeof(RecordOffset), sizeof(RecordOffset));
			char *beforeKey = new char[rOffset.length-sizeof(unsigned)-sizeof(RID)+1];
			beforeKey[rOffset.length-sizeof(unsigned)-sizeof(RID)] = '\0';
			if(attribute.type == TypeVarChar){
				memcpy(beforeKey,(char *)pageData+rOffset.offset, rOffset.length-sizeof(unsigned)-sizeof(RID));
				memcpy(&rid, (char *)pageData+rOffset.offset+rOffset.length-sizeof(unsigned)-sizeof(RID), sizeof(RID));
//				cout<<"\""<<(char *)beforeKey<<"("<<rid.pageNum<<","<<rid.slotNum<<")\"";
//				cout<<"\""<<(char *)beforeKey<<"\"";

			}
			else if(attribute.type == TypeInt){
				memcpy(beforeKey,(char *)pageData+rOffset.offset, sizeof(int));
				memcpy(&rid, (char *)pageData+rOffset.offset+sizeof(int), sizeof(RID));
//				cout<<"\""<<*(int *)beforeKey<<"("<<rid.pageNum<<","<<rid.slotNum<<")\"";
			}
			else if(attribute.type == TypeReal){
				memcpy(beforeKey,(char *)pageData+rOffset.offset, sizeof(float));
				memcpy(&rid, (char *)pageData+rOffset.offset+sizeof(float), sizeof(RID));
//				cout<<"\""<<*(float *)beforeKey<<"("<<rid.pageNum<<","<<rid.slotNum<<")\"";
			}
			delete []beforeKey;

			memcpy(&theRightPage, (char *)pageData+rOffset.offset+rOffset.length-sizeof(PageNum), sizeof(PageNum));
			path.push_back(theRightPage);
		}
//		cout<<"],\n";


//		for(int i=0;i<depth;i++){cout<<"\t";}
//		cout<<"\"children\":[\n";
		for(int i=0;i<path.size();i++){
			printBtree(ixfileHandle, attribute, pageData, depth+1, path[i]);
//			if(i!=path.size()-1){cout<<",\n";}
		}
//		cout<<"]\n}\n";
		return;

	}

	if(nodeType == 1){
//		cout<<"-----------"<<endl;

		RID rid;
//		cout<<"{\"key\":[";
		for(int i=0;i<pageInfo.numOfSlots;i++){
//			void *beforeKey=malloc(PAGE_SIZE);
//			if(i!=0) cout<<",";
			memcpy(&rOffset,(char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(i+1)*sizeof(RecordOffset), sizeof(RecordOffset));
			char *beforeKey = new char[rOffset.length-sizeof(RID)+1];
			char *tmpKey = new char[rOffset.length-sizeof(RID)+1];
			beforeKey[rOffset.length-sizeof(RID)] = '\0';
			tmpKey[rOffset.length-sizeof(RID)] = '\0';

			if(attribute.type == TypeVarChar){
				memcpy(beforeKey,(char *)pageData+rOffset.offset, rOffset.length-sizeof(RID));
				memcpy(&rid, (char *)pageData+rOffset.offset+rOffset.length-sizeof(RID), sizeof(RID));
//				cout<<"\""<<(char *)beforeKey<<"("<<rid.pageNum<<","<<rid.slotNum<<")\"";
			}
			else if(attribute.type == TypeInt){
				memcpy(beforeKey,(char *)pageData+rOffset.offset, rOffset.length-sizeof(RID));
				memcpy(&rid, (char *)pageData+rOffset.offset+rOffset.length-sizeof(RID), sizeof(RID));
//				cout<<"\""<<*(int *)beforeKey<<"("<<rid.pageNum<<","<<rid.slotNum<<")\"";
			}
			else if(attribute.type == TypeReal){
				memcpy(beforeKey,(char *)pageData+rOffset.offset, rOffset.length-sizeof(RID));
				memcpy(&rid, (char *)pageData+rOffset.offset+rOffset.length-sizeof(RID), sizeof(RID));
//				cout<<"\""<<*(float *)beforeKey<<"("<<rid.pageNum<<","<<rid.slotNum<<")\"";
			}
//			cout<<"]\"";
//			free(beforeKey);
			delete []beforeKey;
			delete []tmpKey;
		}
//		cout<<"]},\n";
		return;
	}

}



RC IndexManager::insertEntryInPage(void *pageData, const short int entrySlot, const void *entry, const short int entryLength)
{
	PageInfo pageInfo;
	RecordOffset rOffset;
	void *tempData = malloc(PAGE_SIZE);
	memcpy(&rOffset, (char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(entrySlot+1)*sizeof(RecordOffset), sizeof(RecordOffset));
	if(entrySlot == -1){
		rOffset.length = 0;
		rOffset.offset = sizeof(PageNum);
	} else if(entrySlot > -1) {
		memcpy(&rOffset, (char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(entrySlot+1)*sizeof(RecordOffset), sizeof(RecordOffset));
		memcpy(tempData, (char*)pageData+rOffset.offset, rOffset.length);
		if(memcmp(tempData, entry, entryLength) == 0) {
			free(tempData);
			return -1;
		}
	}
	//read page info & record offset
	memcpy(&pageInfo, (char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), sizeof(PageInfo));
	//modify & shift slots
	rOffset.offset += rOffset.length;
	memcpy(tempData, (char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-pageInfo.numOfSlots*sizeof(RecordOffset), (pageInfo.numOfSlots-entrySlot-1)*sizeof(RecordOffset));
	updateSlotDirectory(tempData, entrySlot, pageInfo, -entryLength);
	memcpy((char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(pageInfo.numOfSlots+1)*sizeof(RecordOffset),tempData, (pageInfo.numOfSlots-entrySlot-1)*sizeof(RecordOffset));
	//shift records
	memcpy(tempData, (char*)pageData+rOffset.offset, pageInfo.recordSize-rOffset.offset);
	memcpy((char*)pageData+rOffset.offset+entryLength, tempData, pageInfo.recordSize-rOffset.offset);
	//insert new entry&slot
	rOffset.length = entryLength;
	memcpy((char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(entrySlot+2)*sizeof(RecordOffset), &rOffset, sizeof(RecordOffset));
	memcpy((char*)pageData+rOffset.offset, entry, entryLength);
	//modify PageInfo
	pageInfo.numOfSlots++;
	pageInfo.recordSize += entryLength;
	memcpy((char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), &pageInfo, sizeof(PageInfo));

	free(tempData);
	return 0;
}

short int IndexManager::composeKey(const Attribute &attribute, const void *key, const RID &rid, void *cKey)
{
	int length = 0;
	if(attribute.type == TypeVarChar) {
		memcpy(&length, key, sizeof(int));
		memcpy(cKey, (char*)key+sizeof(int), length);
		memcpy((char*)cKey+length, &rid, sizeof(RID));
	} else {
		length = 4;
		memcpy(cKey, key, length);
		memcpy((char*)cKey+length, &rid, sizeof(RID));
	}
	return length+sizeof(RID);
}

short int IndexManager::binSearch(const void *pageData, const Attribute &attribute, const void *conditionKey, short int cKeyLength)
{
	void *key = malloc(PAGE_SIZE);
	PageInfo pageInfo;
	RecordOffset rOffset;
	short int nodeType = 0;
	short int keyLength = 0;
	short int lo = 0, mi = 0, hi = 0;
	memcpy(&pageInfo, (char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo), sizeof(PageInfo));
	memcpy(&nodeType, (char*)pageData+PAGE_SIZE-sizeof(short int), sizeof(short int));
	hi = pageInfo.numOfSlots; // search range [lo, hi)

	while(lo < hi) {
//		cout<<"----lo----"<<lo<<"----hi----"<<hi<<"----mi----"<<endl;
		mi = (lo + hi) >> 1;
		memcpy(&rOffset, (char*)pageData+PAGE_SIZE-sizeof(short int)-sizeof(PageInfo)-(mi+1)*sizeof(RecordOffset), sizeof(RecordOffset));
		if(nodeType == 0) {
			keyLength = rOffset.length-sizeof(PageNum);
			memcpy(key, (char*)pageData+rOffset.offset, keyLength);
			(keyCompare(attribute, conditionKey, cKeyLength-sizeof(PageNum), key, keyLength) == -1)? hi = mi : lo = mi + 1;
		} else {
			keyLength = rOffset.length;
//			cout<<"offset:    "<<rOffset.offset<<"    length:    "<<rOffset.length<<endl;
			memcpy(key, (char*)pageData+rOffset.offset, keyLength);
			void *aa = malloc(10);
			RID rr;
			memcpy(&rr, (char *)key+4, 8);
//			cout<<"the key num(which should be less than 100):    "<<rr.slotNum<<endl;
			free(aa);
			(keyCompare(attribute, conditionKey, cKeyLength, key, keyLength) == -1)? hi = mi : lo = mi + 1;
		}
	}
	free(key);
	return --lo;
}

short int IndexManager::keyCompare(const Attribute &attribute, const void *conditionKey, const short int cKeyLength, const void *key, const short int keyLength)
{
	int compResult = 0;
	if(attribute.type == TypeVarChar) {
		int len1 = cKeyLength - sizeof(RID);
		int len2 = keyLength - sizeof(RID);
		char *pc1 = (char *)malloc(len1+1);
		char *pc2 = (char *)malloc(len2+1);
		memcpy(pc1, conditionKey, len1);
		memcpy(pc2, key, len2);
		//!!!Warning!!! the end of char* should be '\0'!!! Otherwise there will be some strange chars at the end.
		pc1[len1] = '\0';
		pc2[len2] = '\0';
		compResult = strcmp(pc1, pc2);
		free(pc1);
		free(pc2);
	} else if(attribute.type == TypeInt) {
		int v1;
		int v2;
		memcpy(&v1, conditionKey, sizeof(int));
		memcpy(&v2, key, sizeof(int));
		compResult = v1 - v2;
	} else if(attribute.type == TypeReal) {
		float v1;
		float v2;
		memcpy(&v1, conditionKey, sizeof(float));
		memcpy(&v2, key, sizeof(float));
		if(v1>v2 && fabs(v1-v2)>1E-6)
			compResult = 1;
		else if (v1<v2 && fabs(v1-v2)>1E-6)
			compResult = -1;
		else
			compResult = 0;
	}
	if(compResult < 0) {
		return -1;
	} else if(compResult > 0) {
		return 1;
	}else if(compResult == 0) {	//then compare rid
		RID rid1;
		RID rid2;
		memcpy(&rid1, (char*)conditionKey+cKeyLength-sizeof(RID), sizeof(RID));
		memcpy(&rid2, (char*)key+keyLength-sizeof(RID), sizeof(RID));
		if(rid1.pageNum==rid2.pageNum && rid1.slotNum==rid2.slotNum) {
			return 0;
		}else if(rid1.pageNum<rid2.pageNum || (rid1.pageNum==rid2.pageNum && rid1.slotNum<rid2.slotNum)) {
			return -1;
		}else if(rid1.pageNum>rid2.pageNum || (rid1.pageNum==rid2.pageNum && rid1.slotNum>rid2.slotNum)) {
			return 1;
		}
	}
	return -1;
}


IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
    ixNumberOfPages = 0;
    rootNodePage = 0;
    filePointer = NULL;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = ixReadPageCounter;
	writePageCount =  ixWritePageCounter;
	appendPageCount = ixAppendPageCounter;
	return 0;
}

RC IXFileHandle::readPage(PageNum pageNum, void *data)
{
	if(pageNum >= ixNumberOfPages)
		return -1;
	fseek(filePointer , (pageNum+1) * PAGE_SIZE ,SEEK_SET);
	if(int result = fread(data, sizeof(char), PAGE_SIZE, filePointer) != PAGE_SIZE)
		return -1;
	ixReadPageCounter++;
	return 0;
}


RC IXFileHandle::writePage(PageNum pageNum, const void *data)
{
	if(pageNum >= ixNumberOfPages)
		return -1;
	fseek(filePointer , (pageNum+1) * PAGE_SIZE ,SEEK_SET);
	if(int result = fwrite(data, sizeof(char), PAGE_SIZE, filePointer) != PAGE_SIZE)
		return -1;
	ixWritePageCounter++;
	return 0;
}


RC IXFileHandle::appendPage(const void *data)
{
	fseek(filePointer, 0, SEEK_END);
	if(int result = fwrite(data, sizeof(char), PAGE_SIZE, filePointer) != PAGE_SIZE)
		return -1;
    ixAppendPageCounter++;
    ixNumberOfPages++;
	return 0;
}
