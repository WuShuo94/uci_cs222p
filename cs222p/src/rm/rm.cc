
#include "rm.h"

RelationManager* RelationManager::instance()
{
    static RelationManager _rm;
    return &_rm;
}

RelationManager::RelationManager()
{
	rbfm = RecordBasedFileManager::instance();
	getTablesDescriptor(tablesDescriptor);
	getColumnsDescriptor(columnsDescriptor);
	getIndexesDescriptor(indexesDescriptor);
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
	const string tableName1 = "Tables";
	const string tableName2 = "Columns";
	const string tableName3 = "Indexes";


	if((rbfm -> createFile(tableName1)) == 0) {
		insertTablesRecord(1, tableName1);
		insertTablesRecord(2, tableName2);
		insertTablesRecord(3, tableName3);

		if(rbfm -> createFile(tableName2) == 0) {
			insertColumnsRecords(1, tableName1, tablesDescriptor);
			insertColumnsRecords(2, tableName2, columnsDescriptor);
			insertColumnsRecords(3, tableName3, indexesDescriptor);
//			return 0;
		}
		if(rbfm -> createFile(tableName3) == 0) {
			return 0;
		}
	}
	return -1;
}

RC RelationManager::deleteCatalog()
{
	if(rbfm->destroyFile("Tables")==0 && rbfm->destroyFile("Columns")==0 && rbfm->destroyFile("Indexes")==0) {
		return 0;
	}
	return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	int tid;
	if(rbfm -> createFile(tableName) == 0) {
		tid = getAvailableTableId();
//		cout << "creating " << tableName << ", tid = " << tid << endl;
		insertTablesRecord(tid, tableName);
		insertColumnsRecords(tid, tableName, attrs);
		return 0;
	}
	return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    if(IsSystemTable(tableName))
    		return -1;

	RID rid;
	rid.pageNum = 0;
	rid.slotNum = 0;
    int tid = getTableId(tableName, rid);
    	if(tid == -1)
    		return -1;

//    	cout << "deleting table, rid: " << rid.pageNum << ", " << rid.slotNum << endl;
    	FileHandle fileHandle_t;
    	rbfm -> openFile("Tables", fileHandle_t);
    	rbfm -> deleteRecord(fileHandle_t, tablesDescriptor, rid);
    	rbfm -> closeFile(fileHandle_t);

    	rid.pageNum = 0;
    	rid.slotNum = 0;
    	vector<string> attributeNames;
    	void *data = malloc(PAGE_SIZE);
    	FileHandle fileHandle;
    	RBFM_ScanIterator rbfm_ScanIterator;
    	rbfm-> openFile("Columns", fileHandle);

    	if(rbfm-> scan(fileHandle, columnsDescriptor, "table-id", EQ_OP, &tid, attributeNames, rbfm_ScanIterator) == 0) {
    		while(rbfm_ScanIterator.getNextRecord(rid,data) != EOF) {
    			rbfm -> deleteRecord(fileHandle, columnsDescriptor, rid);
    		}
    	}

    	//delete all index files
    	vector<vector<string> > indexFiles;
    	getAllIndexFiles(tableName, indexFiles);
    	for(auto indexFile : indexFiles){
    		ix->destroyFile(indexFile[2]);
    	}

    	free(data);
    	rbfm_ScanIterator.close();
    	rbfm-> closeFile(fileHandle);
    	rbfm-> destroyFile(tableName);
    	return 0;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	RID rid;
	rid.pageNum = 0;
	rid.slotNum = 0;
	int tid = getTableId(tableName, rid);
//	cout << "have found tid of " << tableName << ", tid = " << tid << endl;
	if(tid == -1)
		return -1;
	int offset = 0;
	string tmpstr;
	Attribute attr;
	rid.pageNum = 0;
	rid.slotNum = 0;
	vector<string> attributeNames;
	attributeNames.push_back("table-id");
	attributeNames.push_back("column-name");
	attributeNames.push_back("column-type");
	attributeNames.push_back("column-length");
	void *data = malloc(PAGE_SIZE);
	FileHandle fileHandle;
	RBFM_ScanIterator rbfm_ScanIterator;

	rbfm-> openFile("Columns", fileHandle);
	//get attrs from "Columns" by scanning tid

	if(rbfm-> scan(fileHandle, columnsDescriptor, "table-id", EQ_OP, &tid, attributeNames, rbfm_ScanIterator) == 0) {
		while(rbfm_ScanIterator.getNextRecord(rid,data) != EOF) {
			offset = 0;
			//skip null indicator of columnsDescriptor(5 fields, 1 byte)
			offset++;
			//skip tid
			offset += sizeof(int);
			//convert original varChar data to string
			dataToString((char *)data+offset, tmpstr);
			//get attrName
			attr.name = tmpstr;
//			cout << "getAttr-name " << attr.name << endl;
			offset += (sizeof(int) + tmpstr.size());
			//get attrType
			memcpy(&attr.type, (char*)data+offset, sizeof(AttrType));
			offset += sizeof(AttrType);
			//get attrLength
			memcpy(&attr.length, (char*)data+offset, sizeof(AttrLength));
			offset += sizeof(AttrLength);
			attrs.push_back(attr);
		}
	}
	free(data);
	rbfm_ScanIterator.close();
	rbfm-> closeFile(fileHandle);
	return 0;
}

int RelationManager::getTableId(const string &tableName, RID &rid)
{
	FileHandle fileHandle;
	RBFM_ScanIterator rbfm_ScanIterator;
	void *data = malloc(PAGE_SIZE);
	vector<string> attributeNames;
	attributeNames.push_back("table-id");
	int tid = -1;
	int counter = 0;

	//get tid from "Tables" by scanning tableName
	int len = tableName.size();
	void *value = malloc(PAGE_SIZE);
	memcpy(value, &len, sizeof(int));
	memcpy((char *)value + sizeof(int), tableName.c_str(), len);

	rbfm-> openFile("Tables", fileHandle);

	if(rbfm->scan(fileHandle, tablesDescriptor, "table-name", EQ_OP, value, attributeNames, rbfm_ScanIterator) == 0) {         // formatted TypeVarChar Record!!!!!!!!!
		while(rbfm_ScanIterator.getNextRecord(rid, data) != EOF) {
			//skip null indicator (1bit in the first byte of the data)
			memcpy(&tid, (char*)data+1, sizeof(int));
			counter ++;
		}
		if (counter != 1) {
			free(value);
			free(data);
			rbfm_ScanIterator.close();
			rbfm-> closeFile(fileHandle);
			return -1;
		}
	}
	free(value);
	free(data);
	rbfm_ScanIterator.close();
	rbfm-> closeFile(fileHandle);
	return tid;
}

RC RelationManager::matchKey(const vector<Attribute> &descriptor, const Attribute &attribute, const void *data, void *key, bool &isNull){

	int offset = 0;
	int nullIndicatorLength = ceil((double)descriptor.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullIndicatorLength);
	memcpy(nullFieldsIndicator, data, nullIndicatorLength);
	offset = nullIndicatorLength;

	for(int i = 0; i < descriptor.size(); i++) {
		if(descriptor[i].name == attribute.name) {
			if(nullFieldsIndicator[i/8] & (1 << (7-(i%8)))) {
				isNull = true;
				break;
			}
			if(attribute.type == TypeVarChar) {
				int len;
				memcpy(&len, (char*)data+offset, sizeof(int));
				memcpy(key, (char*)data+offset, sizeof(int)+len);
				isNull = false;
				break;
			} else {
				memcpy(key, (char*)data+offset, sizeof(int));
				isNull = false;
				break;
			}
		} else {
			if(nullFieldsIndicator[i/8] & (1 << (7-(i%8)) )) {
				continue;
			}
			if(descriptor[i].type == TypeVarChar) {
				int len = 0;
				memcpy(&len, (char*)data+offset, sizeof(int));
				offset += (sizeof(int) + len);
			} else {
				offset += sizeof(int);
			}
		}
	}
	free(nullFieldsIndicator);
	return 0;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	FileHandle filehandle;
	vector<Attribute> descriptor;

	vector<vector<string> > indexFiles;
	getAllIndexFiles(tableName, indexFiles);

	if(!IsSystemTable(tableName)){
		if(getAttributes(tableName,descriptor) == 0) {
			if(rbfm->openFile(tableName,filehandle)==0){
				if(rbfm->insertRecord(filehandle,descriptor,data,rid)==0){
					//update all index files of this table

					for(auto indexFile: indexFiles){
//						cout<<indexFile[2]<<"   this is file"<<endl;
						IXFileHandle ixFileHandle;
						Attribute attribute;
						void *key = malloc(PAGE_SIZE);

						if(ix->openFile(indexFile[2], ixFileHandle) == -1) {
//							cout<<"open index failed";
							free(key);
							return -1;
						}
						matchAttribute(tableName, indexFile[1], descriptor, attribute);
//						cout<<"-------attribute in INSERT---------"<<attribute.type<<endl;
						bool isNull = false;
						matchKey(descriptor, attribute, data, key, isNull);
//						void *bb = malloc(10);
//						memcpy(bb, (char *)data+5, 4);
//						cout<<"-------key in INSERT--------"<<*((int*)bb)<<endl;
//						free(bb);
//						void *aa = malloc(10);
//						memcpy(aa, (char *)key, 4);
//						cout<<"-------key in INSERT--------"<<*((int*)aa)<<endl;
//						free(aa);
						if(!isNull) {
							if(ix->insertEntry(ixFileHandle, attribute, key, rid) == -1){
	//							cout<<"insert tuple failed";
								free(key);
								return -1;
							}
						}
						ix->closeFile(ixFileHandle);
						free(key);
					}
					rbfm->closeFile(filehandle);
					return 0;
				}
				rbfm->closeFile(filehandle);
			}
		}
	}
	return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	FileHandle filehandle;
	vector<Attribute> descriptor;

	vector<vector<string> >indexFiles;
	getAllIndexFiles(tableName, indexFiles);
	if(!IsSystemTable(tableName)){
		if(getAttributes(tableName,descriptor) == 0) {
			if(rbfm->openFile(tableName,filehandle) == 0){
				void *data = malloc(PAGE_SIZE);
				if(readTuple(tableName, rid, data) == -1) {
					free(data);
					rbfm->closeFile(filehandle);
					return -1;
				}
				if(rbfm->deleteRecord(filehandle,descriptor,rid) == 0){
					//update all index files of this table
					for(auto indexFile: indexFiles){
						IXFileHandle ixFileHandle;
						Attribute attribute;
						void *key = malloc(PAGE_SIZE);

						ix->openFile(indexFile[2], ixFileHandle);
						matchAttribute(tableName, indexFile[1], descriptor, attribute);
						bool isNull = false;
						matchKey(descriptor, attribute, data, key, isNull);
						if(!isNull)
							ix->deleteEntry(ixFileHandle, attribute, key, rid);
						ix->closeFile(ixFileHandle);
						free(key);
					}
					free(data);
					rbfm->closeFile(filehandle);
					return 0;
				}
				free(data);
				rbfm->closeFile(filehandle);
			}
		}
	}
	return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	FileHandle filehandle;
	vector<Attribute> descriptor;

	vector<vector<string> > indexFiles;
	getAllIndexFiles(tableName, indexFiles);

	if(!IsSystemTable(tableName)){
		if(getAttributes(tableName,descriptor) == 0) {
			if(rbfm->openFile(tableName,filehandle)==0){
				void *dataNeedToDelete = malloc(PAGE_SIZE);
				if(readTuple(tableName, rid, dataNeedToDelete) == -1){
					free(dataNeedToDelete);
					rbfm->closeFile(filehandle);
					return -1;
				}
				if(rbfm->updateRecord(filehandle,descriptor,data,rid)==0){
					//update all index files of this table
					for(auto indexFile:indexFiles){
						IXFileHandle ixFileHandle;
						Attribute attribute;
						void *key = malloc(PAGE_SIZE);

                        ix->openFile(indexFile[2], ixFileHandle);
                        matchAttribute(tableName, indexFile[1], descriptor, attribute);
                        bool isNull = false;
                        //delete original data
                        matchKey(descriptor, attribute, dataNeedToDelete, key, isNull);
                        if(!isNull)
                        		ix->deleteEntry(ixFileHandle, attribute, key, rid);
                        //insert new data
                        matchKey(descriptor, attribute, data, key, isNull);
                        if(!isNull)
                        		ix->insertEntry(ixFileHandle, attribute, key, rid);
                        ix->closeFile(ixFileHandle);
                        free(key);
					}
					free(dataNeedToDelete);
					rbfm->closeFile(filehandle);
					return 0;
				}
				rbfm->closeFile(filehandle);
			}
		}
	}
	return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	FileHandle filehandle;
	vector<Attribute> descriptor;
	if(getAttributes(tableName, descriptor) == 0) {
		if(rbfm->openFile(tableName,filehandle)==0){
			if(rbfm->readRecord(filehandle, descriptor, rid, data)==0){
				rbfm->closeFile(filehandle);
				return 0;
			}
			rbfm->closeFile(filehandle);
		}
	}
	return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{

	if(rbfm->printRecord(attrs,data)==0)
		return 0;
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    FileHandle fileHandle;
    rbfm->openFile(tableName, fileHandle);
    void* record = malloc(PAGE_SIZE);
    rbfm->readRecord(fileHandle, recordDescriptor, rid, record);
    rbfm->printRecord(recordDescriptor, record);
    free(record);
    rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
    rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    FileHandle tmpFileHandle;
	rbfm->openFile(tableName, tmpFileHandle);
    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    rm_ScanIterator.prepareIterator(tmpFileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
    rbfm->closeFile(tmpFileHandle);
    return 0;
}

RC RM_ScanIterator::prepareIterator(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const string &conditionAttribute, const CompOp compOp, const void *value, const vector<string> &attributeNames){
//    this -> fileHandle = fileHandle;
	rbfm_Scaniterator.prepareIterator(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames);
    return 0;
}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data){
    return rbfm_Scaniterator.getNextRecord(rid, data);
}

RC RM_ScanIterator::close(){
//	rbfm->closeFile(fileHandle);
    return rbfm_Scaniterator.close();
}

// Extra credit work
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
    return -1;
}

RC RelationManager::getTablesDescriptor(vector<Attribute> &tablesDescriptor)
{
    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    tablesDescriptor.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tablesDescriptor.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    tablesDescriptor.push_back(attr);

    return 0;
}

RC RelationManager::getColumnsDescriptor(vector<Attribute> &columnsDescriptor)
{
    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnsDescriptor.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    columnsDescriptor.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    columnsDescriptor.push_back(attr);

    attr.name = "column-length";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	columnsDescriptor.push_back(attr);

	attr.name = "column-position";
	attr.type = TypeInt;
	attr.length = (AttrLength)4;
	columnsDescriptor.push_back(attr);

    return 0;
}

RC RelationManager::getIndexesDescriptor(vector<Attribute> &indexesDescriptor){
    Attribute attr;

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    indexesDescriptor.push_back(attr);

    attr.name = "attribute-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    indexesDescriptor.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    indexesDescriptor.push_back(attr);

    return 0;
}

bool RelationManager::IsSystemTable(const string &tableName)
{
	return ((tableName == "Tables") || (tableName == "Columns") || (tableName == "Indexes"));
}

int RelationManager::getAvailableTableId()
{
	RM_ScanIterator rm_ScanIterator;
	int tid = -1;
	int tmpTid = 0;
	RID rid;
	void *data = malloc(PAGE_SIZE);
	int *pValue = new int;
	*pValue = 2;
	vector<string> attrName;
	attrName.push_back("table-id");
	rid.pageNum = 0;
	rid.slotNum = 0;

//	cout << "starting get available tid!!!" << endl;
	if(scan("Tables", "table-id", GE_OP, pValue, attrName, rm_ScanIterator) == 0) {
		while(rm_ScanIterator.getNextTuple(rid,data) != EOF) {
			//skip null indicator (1bit in the first byte of the data)
			memcpy(&tmpTid, (char*)data+1, sizeof(int));
//			cout << "In getAvailableTID, tmpTid = " << tmpTid << ", tid = " << tid << endl;
			tid = (tmpTid > tid) ? tmpTid : tid;
		}
	}
	tid++;
//	cout << "In getAvailableTID, final tid = " << tid << endl;

	delete pValue;
	free(data);
	rm_ScanIterator.close();
	return tid;
}

RC RelationManager::insertTablesRecord(const int &tid, const string &tableName)
{
	int offset = 0;
	int length;
	void *data = malloc(PAGE_SIZE);
	FileHandle fileHandle;
	RID rid;

	// generate null indicator of the record of "Tables"
	int nullFieldsIndicatorActualSize = ceil((double)tablesDescriptor.size() / CHAR_BIT);
    unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);
    memcpy(data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    // generate tid of the record of "Tables"
    memcpy((char*)data+offset, &tid, sizeof(int));
    offset += sizeof(int);

    // generate tableName of the record of "Tables"
    length = tableName.size();
    memcpy((char*)data+offset, &length, sizeof(int));
    offset += sizeof(int);
    // !!! The first byte of string in memory is '\n'. !!!
    // !!! So it should be convert to char*. Or use memcpy from (char *)(&s) + 1 !!!
    memcpy((char*)data+offset, tableName.c_str(), length);
    offset += length;

    // generate fileName of the record of "Tables"
    memcpy((char*)data+offset, &length, sizeof(int));
	offset += sizeof(int);
	memcpy((char*)data+offset, tableName.c_str(), length);
	offset += length;

    rbfm -> openFile("Tables", fileHandle);
    rbfm -> insertRecord(fileHandle, tablesDescriptor, data, rid);
    PageInfo pageInfo;
    fileHandle.readPage(rid.pageNum, data);
    memcpy(&pageInfo, (char*)data+PAGE_SIZE-sizeof(PageInfo), sizeof(PageInfo));
//    cout << "Inserting data to Tables, PageInfo.numOfSlots = " << pageInfo.numOfSlots << endl;
    rbfm -> closeFile(fileHandle);
//  printTuple(tablesDescriptor, data);
//	cout << "insert Tables RID" << rid.pageNum << ", " << rid.slotNum << endl;
    free(nullFieldsIndicator);
    free(data);
    return 0;
}

RC RelationManager::insertColumnsRecords(const int &tid, const string &tableName, const vector<Attribute> &attrs)
{
	int offset;
	int length;
	Attribute attribute;
	string attrName;
	AttrType attrType;
	AttrLength attrLength;
	void *data = malloc(PAGE_SIZE);
	FileHandle fileHandle;
	RID rid;

	rbfm -> openFile("Columns", fileHandle);

	for(int i = 0; i < attrs.size(); i++) {
		offset = 0;
		// generate null indicator of the record
		int nullFieldsIndicatorActualSize = ceil((double)columnsDescriptor.size() / CHAR_BIT);
	    unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	    memset(nullFieldsIndicator, 0, nullFieldsIndicatorActualSize);
	    memcpy(data, nullFieldsIndicator, nullFieldsIndicatorActualSize);
	    offset += nullFieldsIndicatorActualSize;

	    //generate tid of the record
	    memcpy((char*)data+offset, &tid, sizeof(int));
	    offset += sizeof(int);

	    //generate attrName of the record
		attribute = attrs[i];
		attrName = attribute.name;
		length = attrName.size();
		memcpy((char*)data+offset, &length, sizeof(int));
		offset += sizeof(int);
		memcpy((char*)data+offset, attrName.c_str(), length);
		offset += length;

	    //generate attrType of the record
		attrType = attribute.type;
		length = sizeof(AttrType);
		memcpy((char*)data+offset, &attrType, length);
		offset += length;

		//generate attrLength of the record
		attrLength = attribute.length;
		length = sizeof(AttrLength);
		memcpy((char*)data+offset, &attrLength, length);
		offset += length;

		//generate attrPosition of the record
		int num = i+1;
		memcpy((char*)data+offset, &num, sizeof(int));
		offset += sizeof(int);

		rbfm -> insertRecord(fileHandle, columnsDescriptor, data, rid);
//		rbfm -> readRecord(fileHandle, columnsDescriptor, rid, data);
//		printTuple(columnsDescriptor, data);
//		cout << endl;
	    free(nullFieldsIndicator);
	}

    rbfm -> closeFile(fileHandle);
    free(data);
    return 0;
}

RC RelationManager::insertIndexesRecords(const string &tableName, const string &attributeName){
    int offset = 0;
    int length;
    void *data = malloc(PAGE_SIZE);
    FileHandle fileHandle;
    RID rid;

    //generate null_indicator
    unsigned char *nullFieldsIndicator = (unsigned char *) malloc(1);
    memset(nullFieldsIndicator, 0, 1);
    memcpy(data, nullFieldsIndicator, 1);
    offset += 1;

    //generate table-name
    length = tableName.size();
    memcpy((char *)data+offset, &length, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)data+offset, tableName.c_str(), length);
    offset += length;

    //generate attribute
    length = attributeName.size();
    memcpy((char *)data+offset, &length, sizeof(int));
    offset += sizeof(int);
    memcpy((char *)data+offset, attributeName.c_str(), length);
    offset += length;

    //generate file-name (table-name.attribute)
    length = tableName.size() + 1 + attributeName.size();
    memcpy((char *)data+offset, &length, sizeof(int));
    offset += sizeof(int);
    string filename = tableName+"."+attributeName;
    memcpy((char *)data+offset, filename.c_str(), length);
    offset += length;


    rbfm->openFile("Indexes", fileHandle);
    rbfm->insertRecord(fileHandle, indexesDescriptor, data, rid);
    rbfm->closeFile(fileHandle);
    free(data);
    free(nullFieldsIndicator);
    return 0;
}

RC RelationManager::dataToString(void *data, string &str)
{
	int size = 0;
	char *varChar = (char *)malloc(PAGE_SIZE);

	memcpy(&size, data, sizeof(int));
	memcpy(varChar, (char*)data+sizeof(int), size);

	varChar[size] = '\0';
	string tmpstr(varChar);
	str = tmpstr;

	free(varChar);
	return 0;
}

RC RelationManager::getAllIndexFiles(const string &tableName, vector<vector<string> > &indexFiles){
	FileHandle fileHandle;
	RID rid;

	void *data = malloc(PAGE_SIZE);
	int offset = 0;
	string tmpstr;

	RBFM_ScanIterator rbfm_ScanIterator;
	rbfm->openFile("Indexes", fileHandle);


	vector<string> attributeNames;
	attributeNames.push_back("table-name");
	attributeNames.push_back("attribute-name");
	attributeNames.push_back("file-name");

	//generate formartted input record
	int length = tableName.length();
	void *tableName_input = malloc(PAGE_SIZE);
	memcpy((char *)tableName_input, &length, sizeof(int));
	memcpy((char *)tableName_input+sizeof(int), tableName.c_str(), length);

	if(rbfm->scan(fileHandle, indexesDescriptor, "table-name", EQ_OP, tableName_input, attributeNames, rbfm_ScanIterator) == 0){

		while(rbfm_ScanIterator.getNextRecord(rid, data) != EOF){
			vector<string> indexFile;

			offset = 0;
			//skip the nullindicator
		    int nullFieldsIndicatorActualSize = ceil((double)attributeNames.size() / CHAR_BIT);
		    offset += nullFieldsIndicatorActualSize;

			//convert original varChar data to string
			dataToString((char *)data+offset, tmpstr);
			//append table-name in indexFile
			indexFile.push_back(tmpstr);
			offset += (sizeof(int) + tmpstr.size());
//			cout<<tmpstr<<endl;
			//append attribute-name in indexFile
			dataToString((char *)data+offset, tmpstr);
			indexFile.push_back(tmpstr);
			offset += (sizeof(int) + tmpstr.size());

			//append file-name in indexFile
			dataToString((char *)data+offset, tmpstr);
			indexFile.push_back(tmpstr);
			offset += (sizeof(int) + tmpstr.size());

			indexFiles.push_back(indexFile);
		}
	}

	free(data);
	free(tableName_input);
	rbfm_ScanIterator.close();
	rbfm->closeFile(fileHandle);
	return 0;
}

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	FileHandle fileHandle;
	IXFileHandle ixfileHandld;
	RBFM_ScanIterator rbfm_ScanIterator;
	vector<Attribute> attributeDescriptor;
	vector<string> attributeNames;
	void *data = malloc(PAGE_SIZE);
	RID rid;
	Attribute attribute;

	if(IsSystemTable(tableName)) return -1;

	//generate indexfile
	string filename = tableName + "." + attributeName;
	if(ix->createFile(filename) == 0){
		insertIndexesRecords(tableName, attributeName);
	}

	//init parameters
	getAttributes(tableName, attributeDescriptor);
	for(auto attri: attributeDescriptor){
//		cout<<"Descriptor:   "<<attri.name<<endl;
	}
	matchAttribute(tableName, attributeName, attributeDescriptor, attribute);
//	cout<<"attribute:        !!!!     "<<attribute.name<<endl;
	attributeNames.push_back(attributeName);
//	getAttributes(tableName, attributeDescriptor);
//	for(auto item : attributeDescriptor){
//		if(item.name == attributeName) {
//			attribute = item;
//			break;
//		}
//	}

	//insert data in index file
	rbfm->openFile(tableName, fileHandle);
	ix->openFile(filename, ixfileHandld);
	if(rbfm->scan(fileHandle, attributeDescriptor, attributeName, NO_OP, "", attributeNames, rbfm_ScanIterator) == 0){
//		cout<<"this is scan loop"<<endl;
		while(rbfm_ScanIterator.getNextRecord(rid, data) != EOF){
			//"+1" to skip the nullindicator //null_indicator + data;(GET)  //TypeINT or TypeREAL or length+TypeVARCHAR
//			cout<<"KEY========"<<*((float *)dd)<<endl;
			if(!( ((char*)data)[0] & (1<<7) ))
				ix->insertEntry(ixfileHandld, attribute, (char*)data+1, rid);
		}
	}

	free(data);
	rbfm_ScanIterator.close();
	rbfm->closeFile(fileHandle);
	ix->closeFile(ixfileHandld);

	return 0;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	FileHandle fileHandle;
	vector<string> attributeNames;
	RBFM_ScanIterator rbfm_ScanIterator;
	RID rid;
	void *data = malloc(PAGE_SIZE);

	if(IsSystemTable(tableName)) return -1;

	string fileName = tableName + "." + attributeName;
	void *fileName_input = malloc(PAGE_SIZE);
	int length = fileName.length();
	memcpy((char *)fileName_input, &length, sizeof(int));
	memcpy((char *)fileName_input+sizeof(int), fileName.c_str(), length);
	if(ix->destroyFile(fileName) != 0) return -1;

	rbfm->openFile("Indexes", fileHandle);
	if(rbfm->scan(fileHandle, indexesDescriptor, "file-name", EQ_OP, fileName_input, attributeNames, rbfm_ScanIterator) == 0){
		while(rbfm_ScanIterator.getNextRecord(rid, data) != EOF){
			rbfm->deleteRecord(fileHandle, indexesDescriptor, rid);
		}
	}

	free(data);
	free(fileName_input);
	rbfm_ScanIterator.close();
	rbfm->closeFile(fileHandle);

	return 0;
}

RC RelationManager::indexScan(const string &tableName,
                      const string &attributeName,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      RM_IndexScanIterator &rm_IndexScanIterator)
{
	IXFileHandle ixfileHandle;
	vector<Attribute> attributeDescriptor;
	Attribute attribute;

	//init ixfileHandle and attribute
	string fileName = tableName + "." + attributeName;
//	if(ix->openFile(fileName, ixfileHandle) == -1) {
////		cout<<"open failed"<<endl;
//		return -1;
//	}
	getAttributes(tableName, attributeDescriptor);
	matchAttribute(tableName, attributeName, attributeDescriptor, attribute);
//	getAttributes(tableName, attributeDescriptor);
//	for(auto item : attributeDescriptor){
//		if(item.name == attributeName) {
//			attribute = item;
//			break;
//		}
//	}
//	cout << 	"In RelationManager::indexScan(), ixfileHandle.ixNumberOfPages = " <<ixfileHandle.ixNumberOfPages << endl;
//	cout << "In RelationManager::indexScan(), ixfileHandle.bindFileName = " << ixfileHandle.bindFileName << endl;
//	ixfileHandle.temp = true;
//	cout<<"this is in RM::indexScan"<<endl;
	return rm_IndexScanIterator.prepareIterator(fileName, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);

}

RC RM_IndexScanIterator::prepareIterator(string &fileName, const Attribute &attribute, const void *lowKey, const void *highKey, bool lowKeyInclusive, bool highKeyInclusive){
//	cout << fileName << endl;
	if(ix->openFile(fileName, ixfileHandle) == -1)
		return -1;
//	cout << fileName << endl;
//	return ix_ScanIterator.prepareIterator(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
	if(ix_ScanIterator.prepareIterator(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive) == 0){
//		cout<<"this is in the prepare layer:   "<<ix_ScanIterator.highKeyNUll<<endl;
		return 0;
	}
	return -1;
}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
//	cout<<"STEP HERE"<<endl;
	return ix_ScanIterator.getNextEntry(rid, key);
}

RC RM_IndexScanIterator::close() {
	ix_ScanIterator.close();
	ix ->closeFile(ixfileHandle);
	return 0;
}

RC RelationManager::matchAttribute(const string &tableName, const string &attributeName, const vector<Attribute> &attributeDescriptor, Attribute &attribute){
	for(auto item : attributeDescriptor){
		if(item.name == attributeName) {
			attribute = item;
			return 0;
		}
	}
	return -1;
}
