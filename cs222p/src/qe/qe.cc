#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) {
	this -> condition = condition;
	this -> pIterator = input;
	input -> getAttributes(this -> attrs);
//	cout << "initial filter" << endl;
}

RC Filter::getNextTuple(void *data) {
	if(condition.op == NO_OP)
		return (pIterator -> getNextTuple(data));
	//check whether selected attribute is exist
	bool existLeftAttr = false;
	bool existRightAttr = false;
	Attribute leftAttr;
	Attribute rightAttr;
	for(int i = 0; i < attrs.size(); i++) {
		if(attrs[i].name == condition.lhsAttr) {
			existLeftAttr = true;
			leftAttr = attrs[i];
		}
		if(condition.bRhsIsAttr) {
			if(attrs[i].name == condition.lhsAttr) {
				existRightAttr = true;
				rightAttr = attrs[i];
			}
		} else {
			rightAttr = leftAttr;
			existRightAttr = true;
		}
	}
	if((!existLeftAttr) && (!existRightAttr))
		return QE_EOF;


	//fetch field and compare
	int offset = 0;
	int compResult;
	int nullIndicatorLength = ceil((double)attrs.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullIndicatorLength);
	void *rightField = malloc(PAGE_SIZE);
//	cout << "get next in filter" << endl;

	while(1) {
		//get one tuple
		if(pIterator -> getNextTuple(data) == QE_EOF) {
			free(nullFieldsIndicator);
			free(rightField);
			return QE_EOF;
		}
		//fetch right field
		if(condition.bRhsIsAttr) {
			for(int i = 0; i < attrs.size(); i++) {
				if(attrs[i].name == condition.rhsAttr) {
					bool isNull = false;
					fetchField(data, attrs, rightField, attrs[i], isNull);
					if(isNull) {
						free(nullFieldsIndicator);
						free(rightField);
						return QE_EOF;
					}
					break;
				}
			}
		} else {
			if(condition.rhsValue.data == NULL) {
				free(nullFieldsIndicator);
				free(rightField);
//				cout << "condition.rhsValue.data == NULL" << endl;
				return QE_EOF;
			}
			int len;
			if(rightAttr.type == TypeVarChar) {
				memcpy(&len, condition.rhsValue.data, sizeof(int));
				len += sizeof(int);
			}else {
				len = sizeof(int);
			}
			memcpy(rightField, condition.rhsValue.data, len);
//			cout << *(int*)rightField << endl;
		}

		offset = 0;
		memcpy(nullFieldsIndicator, data, nullIndicatorLength);
		offset += nullIndicatorLength;

		for(int i = 0; i < attrs.size(); i++) {
			if( nullFieldsIndicator[i/8] & (1 << (7-(i%8)) ) ){
				continue;
			}
			//found and fetch left field
			if(attrs[i].name == leftAttr.name) {
				if(leftAttr.type == TypeVarChar) {
					int len1 = 0;
					memcpy(&len1, (char*)data+offset, sizeof(int));
					offset += sizeof(int);
					int len2 = 0;
					memcpy(&len2, rightField, sizeof(int));
					char *pc1 = (char*)malloc(len1+1);
					char *pc2 = (char*)malloc(len2+1);
					memcpy(pc1, (char*)data+offset, len1);
					memcpy(pc2, (char*)rightField+sizeof(int), len2);
					pc1[len1] = '\0';
					pc2[len2] = '\0';
					compResult = strcmp(pc1, pc2);
					offset += len1;
					free(pc1);
					free(pc2);
				} else if(leftAttr.type == TypeInt) {
					int i1 = 0;
					int i2 = 0;
					memcpy(&i1, (char*)data+offset, sizeof(int));
					memcpy(&i2, rightField, sizeof(int));
					compResult = i1 - i2;
					offset += sizeof(int);
//					cout << "have found matched left attr, i1 = " << i1 << ", i2 = " << i2 << ", coumResult = " << compResult << endl;
				}else if(leftAttr.type == TypeReal) {
					float f1;
					float f2;
					memcpy(&f1, (char*)data+offset, sizeof(float));
					memcpy(&f2, rightField, sizeof(float));
					if(f1>f2 && fabs(f1-f2)>1E-6)
						compResult = 1;
					else if (f1<f2 && fabs(f1-f2)>1E-6)
						compResult = -1;
					else
						compResult = 0;
					offset += sizeof(float);
//					cout << "have found matched float left attr, compResult = " << compResult << endl;

				} else{
					free(nullFieldsIndicator);
					return QE_EOF;
				}

				//whether this tuple satisfies the condition
				switch(condition.op){
					case EQ_OP:
					{
						if(compResult == 0){
							free(nullFieldsIndicator);
							free(rightField);
							return 0;
						}
						break;
					}
					case LT_OP:
					{
						if(compResult < 0){
							free(nullFieldsIndicator);
							free(rightField);
							return 0;
						}
						break;
					}
					case LE_OP:
					{
						if(compResult <= 0){
							free(nullFieldsIndicator);
							free(rightField);
							return 0;
						}
						break;
					}
					case GT_OP:
					{
						if(compResult > 0){
							free(nullFieldsIndicator);
							free(rightField);
							return 0;
						}
						break;
					}
					case GE_OP:
					{
						if(compResult >= 0){
							free(nullFieldsIndicator);
							free(rightField);
							return 0;
						}
						break;
					}
					case NE_OP:
					{
						if(compResult != 0){
							free(nullFieldsIndicator);
							free(rightField);
							return 0;
						}
						break;
					}
				}
			}
			//current attribute is not left attribute, skip
			if(attrs[i].type == TypeVarChar) {
				int len = 0;
				memcpy(&len, (char*)data+offset, sizeof(int));
				offset += sizeof(int);
				offset += len;
			} else {
				offset += sizeof(int);
			}
		}
	}
	free(nullFieldsIndicator);
	free(rightField);
	return 0;
}


void Filter::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	pIterator -> getAttributes(attrs);
}

//Filter::~Filter() {
//	pIterator -> close();
//}


Project::Project(Iterator *input, const vector<string> &attrNames) {
	this -> pIterator = input;
	input -> getAttributes(this -> childAttrs);
	this -> attrNames = attrNames ;
	this -> getAttributes(this -> attrs);
}

void Project::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	for(int i = 0; i < attrNames.size(); i++) {
    		for(int j = 0; j < childAttrs.size(); j++) {
    			if(attrNames[i] == childAttrs[j].name) {
    				attrs.push_back(childAttrs[j]);
    				break;
    			}
    		}
    }
}

RC Project::getNextTuple(void *data) {
//	getAttributes(attrs);
	if(attrs.size() != attrNames.size()) {
		return QE_EOF;
	}

	void *record = malloc(PAGE_SIZE);

	if(pIterator -> getNextTuple(record) == -1) {
		free(record);
		return QE_EOF;
	}

	int childOffset = 0;
	int childNullIndicatorLength = ceil((double)childAttrs.size() / CHAR_BIT);
	unsigned char *childNullFieldsIndicator = (unsigned char *) malloc(childNullIndicatorLength);
	childOffset = childNullIndicatorLength;
	memcpy(childNullFieldsIndicator, record, childNullIndicatorLength);
	int offset = 0;
	int nullIndicatorLength = ceil((double)attrs.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullIndicatorLength);
	memset(nullFieldsIndicator, 0, nullIndicatorLength);
	offset += nullIndicatorLength;

	for(int i = 0; i < attrs.size(); i++) {
		childOffset = childNullIndicatorLength;
		for(int j = 0; j < childAttrs.size(); j++) {
			if(attrs[i].name == childAttrs[j].name) {
				if(childNullFieldsIndicator[j/8] & (1 << (7-(j%8)) )) {
					nullFieldsIndicator[i/8] = nullFieldsIndicator[i/8] | (1 << (7-(i%8)));
					break;
				}
				if(attrs[i].type == TypeVarChar) {
					int len = 0;
					memcpy(&len, (char*)record+childOffset, sizeof(int));
					memcpy((char*)data+offset, (char*)record+childOffset, len+sizeof(int));
					childOffset += (sizeof(int) + len);
					offset += (sizeof(int) + len);
				} else {
					memcpy((char*)data+offset, (char*)record+childOffset, sizeof(int));
					childOffset += sizeof(int);
					offset += sizeof(int);
				}
				break;
			} else {
				if(childNullFieldsIndicator[j/8] & (1 << (7-(j%8)) )) {
					continue;
				}
				if(childAttrs[j].type == TypeVarChar) {
					int len = 0;
					memcpy(&len, (char*)record+offset, sizeof(int));
					childOffset += sizeof(int);
					childOffset += len;
				} else {
					childOffset += sizeof(int);
				}
			}
		}
	}
	memcpy(data, nullFieldsIndicator, nullIndicatorLength);
	free(record);
	free(childNullFieldsIndicator);
	free(nullFieldsIndicator);
	return 0;
}



BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages) {
	this -> condition = condition;
	this -> pLeftIterator = leftIn;
	this -> pRightIterator = rightIn;
	pLeftIterator -> getAttributes(this -> leftAttrs);
	pRightIterator -> getAttributes(this -> rightAttrs);
	this -> bufSize = 200;
	this -> tuplesNumOfBlock = numPages*(PAGE_SIZE/bufSize);
	this -> counter = 0;
	this -> rightTuple = malloc(bufSize);
	for(int i = 0; i < leftAttrs.size(); i++) {
		if(leftAttrs[i].name == condition.lhsAttr) {
			leftAttr = leftAttrs[i];
			break;
		}
	}
	for(int i = 0; i < leftAttrs.size(); i++) {
		if(rightAttrs[i].name == condition.rhsAttr) {
			rightAttr = rightAttrs[i];
			break;
		}
	}
//	cout << leftAttr.name << " join " << rightAttr.name << endl;
	if(leftAttr.type == TypeVarChar)
		loadNextBlock(varcharBlockHashTable);
	else if(leftAttr.type == TypeInt)
		loadNextBlock(intBlockHashTable);
	else if(leftAttr.type == TypeReal)
		loadNextBlock(floatBlockHashTable);
}

BNLJoin::~BNLJoin() {
	free(rightTuple);
}

RC BNLJoin::getNextTuple(void *data){
	if(condition.op != EQ_OP || !condition.bRhsIsAttr || (leftAttr.type != rightAttr.type))
		return QE_EOF;

	void *rightField = malloc(bufSize);
	void *leftTuple;


	while(1) {
		// check whether needs to fetch right tuple
		if(counter == 0) {
			if(pRightIterator->getNextTuple(rightTuple) == QE_EOF) {
				//finished scanning right table, then load next block and re-scan right table
				if(leftAttr.type == TypeVarChar) {
					if(loadNextBlock(varcharBlockHashTable) == QE_EOF) {
						free(rightField);
						return QE_EOF;
					}
				} else if(leftAttr.type == TypeInt) {
					if(loadNextBlock(intBlockHashTable) == QE_EOF) {
						free(rightField);
						return QE_EOF;
				}
				} else if(leftAttr.type == TypeReal) {
					if(loadNextBlock(floatBlockHashTable) == QE_EOF) {
						free(rightField);
						return QE_EOF;
					}
				}
				pRightIterator -> setIterator();
				pRightIterator->getNextTuple(rightTuple);
//				aa
			}
		}
		bool isNull = false;
		fetchField(rightTuple, rightAttrs, rightField, rightAttr, isNull);
		if(isNull)
			continue;

		//find matched left tuple
		vector<void *> leftTuples;
		if(rightAttr.type  == TypeVarChar) {
			int len = 0;
			memcpy(&len, rightField, sizeof(int));
			char *pc = (char*)malloc(len+1);
			memcpy(pc, (char*)rightField+sizeof(int), len);
			pc[len] = '\0';
			unordered_map< string, vector<void *> >::const_iterator mapValues = varcharBlockHashTable.find(pc);
			free(pc);
			if(mapValues == varcharBlockHashTable.end())
				continue;
			leftTuples = mapValues -> second;

		} else if(rightAttr.type  == TypeInt) {
			int v;
			memcpy(&v, rightField, sizeof(int));
			unordered_map< int, vector<void *> >::const_iterator mapValues = intBlockHashTable.find(v);
			if(mapValues == intBlockHashTable.end())
				continue;
			leftTuples = mapValues -> second;
//			cout << "mapped int, leftTuples size = " << leftTuples.size() << ". key = " << v << endl;

		} else if(rightAttr.type  == TypeReal) {
			float v;
			memcpy(&v, rightField, sizeof(float));
			unordered_map< float, vector<void *> >::const_iterator mapValues = floatBlockHashTable.find(v);
			if(mapValues == floatBlockHashTable.end())
				continue;
			leftTuples = mapValues -> second;
		}

		if(counter == 0)
			counter = leftTuples.size();
		leftTuple = leftTuples[leftTuples.size() - counter];
		counter --;
		break;
	}

	//combine to a whole tuple
	joinTuples(data, leftTuple, rightTuple, leftAttrs, rightAttrs);

	free(rightField);
	return 0;
}

void BNLJoin::getAttributes(vector<Attribute> &attrs) const{
	for(int i = 0; i < leftAttrs.size(); i++){
		attrs.push_back(leftAttrs[i]);
	}
	for(int i = 0; i < rightAttrs.size(); i++){
		attrs.push_back(rightAttrs[i]);
	}
}

RC BNLJoin::loadNextBlock(unordered_map< int, vector<void *> > &intBlockHashTable) {
	int actualNum = 0;
	void *leftTuple;
	unordered_map< int, vector<void *> >::const_iterator mapValues;

	//erase the block & hash table
	mapValues = intBlockHashTable.begin();
	for(int i = 0; i < intBlockHashTable.size(); i++) {
		vector<void *> leftTuples;
		leftTuples = mapValues -> second;
		mapValues++;
		for(int j = 0; j < leftTuples.size(); j++) {
			free(leftTuples[j]);
		}
	}
	intBlockHashTable.clear();

	//load next block
	for(int i = 0; i < tuplesNumOfBlock; i++ ) {
		vector<void *> leftTuples;
		leftTuple = malloc(bufSize);
		if(pLeftIterator->getNextTuple(leftTuple) == QE_EOF) {
			free(leftTuple);
			break;
		}
		int leftField;
		bool isNull = false;
		fetchField(leftTuple, leftAttrs, &leftField, leftAttr, isNull);
		if(isNull) {
			free(leftTuple);
			actualNum++;
			continue;
		}
		mapValues = intBlockHashTable.find(leftField);
		if(mapValues != intBlockHashTable.end())
			leftTuples = mapValues -> second;
		leftTuples.push_back(leftTuple);
		intBlockHashTable[leftField] = leftTuples;
		actualNum++;
	}
	if(actualNum == 0)
		return QE_EOF;
	return 0;
};

RC BNLJoin::loadNextBlock(unordered_map< float, vector<void *> > &floatBlockHashTable) {
	int actualNum = 0;
	void *leftTuple;
	unordered_map< float, vector<void *> >::const_iterator mapValues;

	//erase the block & hash table
	mapValues = floatBlockHashTable.begin();
	for(int i = 0; i < floatBlockHashTable.size(); i++) {
		vector<void *> leftTuples;
		leftTuples = mapValues -> second;
		mapValues++;
		for(int j = 0; j < leftTuples.size(); j++) {
			free(leftTuples[j]);
		}
	}
	floatBlockHashTable.clear();

	for(int i = 0; i < tuplesNumOfBlock; i++ ) {
		vector<void *> leftTuples;
		leftTuple = malloc(bufSize);
		if(pLeftIterator->getNextTuple(leftTuple) == QE_EOF) {
			free(leftTuple);
			break;
		}
		float leftField;
		bool isNull = false;
		fetchField(leftTuple, leftAttrs, &leftField, leftAttr, isNull);
		if(isNull) {
			free(leftTuple);
			actualNum++;
			continue;
		}
		mapValues = floatBlockHashTable.find(leftField);
		if(mapValues != floatBlockHashTable.end())
			leftTuples = mapValues -> second;
		leftTuples.push_back(leftTuple);
		floatBlockHashTable[leftField] = leftTuples;
		actualNum++;
	}
	if(actualNum == 0)
		return QE_EOF;
	return 0;
}

RC BNLJoin::loadNextBlock(unordered_map< string, vector<void *> > &varcharBlockHashTable) {
	int actualNum = 0;
	void *leftTuple;
	unordered_map< string, vector<void *> >::const_iterator mapValues;

	//erase the block & hash table
	mapValues = varcharBlockHashTable.begin();
	for(int i = 0; i < varcharBlockHashTable.size(); i++) {
		vector<void *> leftTuples;
		leftTuples = mapValues -> second;
		mapValues++;
		for(int j = 0; j < leftTuples.size(); j++) {
			free(leftTuples[j]);
		}
	}
	varcharBlockHashTable.clear();

	for(int i = 0; i < tuplesNumOfBlock; i++ ) {
		vector<void *> leftTuples;
		leftTuple = malloc(bufSize);
		if(pLeftIterator->getNextTuple(leftTuple) == QE_EOF) {
			free(leftTuple);
			break;
		}
		void *leftField = malloc(bufSize);
		bool isNull = false;
		fetchField(leftTuple, leftAttrs, leftField, leftAttr, isNull);
		if(isNull) {
			free(leftTuple);
			free(leftField);
			actualNum++;
			continue;
		}
		int len;
		memcpy(&len, leftField, sizeof(int));
		char *pc = (char*)malloc(len+1);
		memcpy(pc, (char*)leftField+sizeof(int), len);
		pc[len] = '\0';
		mapValues = varcharBlockHashTable.find(pc);
		if(mapValues != varcharBlockHashTable.end())
			leftTuples = mapValues -> second;
		leftTuples.push_back(leftTuple);
		varcharBlockHashTable[pc] = leftTuples;
		actualNum++;
		free(leftField);
		free(pc);
	}
	if(actualNum == 0)
		return QE_EOF;
	return 0;
}




INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
	this -> condition = condition;
	this -> pLeftIterator = leftIn;
	this -> pRightIterator = rightIn;
	this -> leftTuple = malloc(PAGE_SIZE);
	pLeftIterator -> getAttributes(this -> leftAttrs);
	pRightIterator -> getAttributes(this -> rightAttrs);
	for(int i = 0; i < leftAttrs.size(); i++) {
		if(leftAttrs[i].name == condition.lhsAttr) {
			leftAttr = leftAttrs[i];
			break;
		}
	}
	for(int i = 0; i < rightAttrs.size(); i++) {
		if(rightAttrs[i].name == condition.rhsAttr) {
			rightAttr = rightAttrs[i];
			break;
		}
	}

	bool initial = true;
	void *key = malloc(PAGE_SIZE);
	while(initial) {
		bool isNull = false;
		pLeftIterator -> getNextTuple(leftTuple);
		fetchField(leftTuple, leftAttrs, key, leftAttr, isNull);
		if(isNull) {
			//skip, read next left tuple
			continue;
		}
		setRightIndexIterator(key);
		initial = false;
	}
	free(key);

//	cout << "initial INLJ, join " << leftAttr.name << " & " << rightAttr.name << endl;
}

INLJoin::~INLJoin() {
	free(leftTuple);
}

RC INLJoin::getNextTuple(void *data){
	if((!condition.bRhsIsAttr) || (leftAttr.type != rightAttr.type))
		return QE_EOF;

	void *rightTuple = malloc(PAGE_SIZE);
	void *key = malloc(PAGE_SIZE);

	while(1) {
		if((pRightIterator -> getNextTuple(rightTuple)) == QE_EOF) {
			while(1) {
				if((pLeftIterator -> getNextTuple(leftTuple)) != QE_EOF) {
					bool isNull = false;
					fetchField(leftTuple, leftAttrs, key, leftAttr, isNull);
					if(isNull)
						continue;
					setRightIndexIterator(key);
					break;
				} else {
					free(key);
					free(rightTuple);
					return QE_EOF;
				}
			}
		} else {
			break;
		}
	}

	joinTuples(data, leftTuple, rightTuple, leftAttrs, rightAttrs);

	free(key);
	free(rightTuple);
	return 0;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	for(int i = 0; i < leftAttrs.size(); i++){
		attrs.push_back(leftAttrs[i]);
	}
	for(int i = 0; i < rightAttrs.size(); i++){
		attrs.push_back(rightAttrs[i]);
	}
}

void  INLJoin::setRightIndexIterator(void *key) {
	if(condition.op == NO_OP){
		pRightIterator -> setIterator(NULL, NULL, true, true);
	} else if(condition.op == LT_OP) {
		pRightIterator -> setIterator(key, NULL, false, true);
	} else if(condition.op == LE_OP) {
		pRightIterator -> setIterator(key, NULL, true, true);
	} else if(condition.op == GT_OP) {
		pRightIterator -> setIterator(NULL, key, true, false);
	} else if(condition.op == GE_OP) {
		pRightIterator -> setIterator(NULL, key, true, true);
	} else if(condition.op == EQ_OP) {
		pRightIterator -> setIterator(key, key, true, true);
//		cout << "setRightIndexIterator, EQ = " << *(float*)key << endl;
	}
}



Aggregate::Aggregate(Iterator *input, Attribute aggAttr, AggregateOp op) {
	this -> pIterator = input;
	this -> attr = aggAttr;
	pIterator ->getAttributes(this -> attrs);
	this -> op = op;
	this -> flag = false;
}

RC Aggregate::getNextTuple(void *data) {
	//we do the some kinds of aggregation (MIN, MAX, SUM, AVG) on a numeric attribute (INT or REAL).
	//we do COUNT aggregation on any kinds of attributes.
	if((attr.type == TypeVarChar && op != COUNT) || flag) {
//		cout << "invalid parameter" << endl;
		flag = true;
		return QE_EOF;
	}

	int intField = 0;
	float floatField = 0;
	int tempInt = 0;
	float tempFloat = 0;
	int count = 0;
	void *tuple = malloc(PAGE_SIZE);

	while((pIterator -> getNextTuple(tuple)) != QE_EOF) {
		if(op == COUNT) {
			count++;
			continue;
		} else if(attr.type == TypeInt) {
			bool isNull = false;
			fetchField(tuple, attrs, &tempInt, attr, isNull);
			if(isNull) continue;
			if(count == 0) {
				intField = tempInt;
				count ++;
				continue;
			}
			switch(op) {
				case MIN: {
					(tempInt < intField)? intField=tempInt : intField;
					break;
				}
				case MAX: {
					(tempInt > intField)? intField=tempInt : intField;
					break;
				}
				case SUM: {
					intField += tempInt;
					break;
				}
				case AVG: {
					intField += tempInt;
					break;
				}
			}
		} else if(attr.type == TypeReal) {
			bool isNull = false;
			fetchField(tuple, attrs, &tempFloat, attr, isNull);
			if(isNull) continue;
			if(count == 0) {
				floatField = tempFloat;
				count++;
				continue;
			}
			switch(op) {
				case MIN: {
					if(floatField>tempFloat && fabs(floatField-tempFloat)>1E-6)
						floatField = tempFloat;
					break;
				}
				case MAX: {
					if(floatField<tempFloat && fabs(floatField-tempFloat)>1E-6)
						floatField = tempFloat;
					break;
				}
				case SUM: {
					floatField += tempFloat;
					break;
				}
				case AVG: {
					floatField += tempFloat;
					break;
				}
			}

		}
		count ++;
	}
	if(count == 0 && op != COUNT) {
		free(tuple);
		flag = true;
		return QE_EOF;
	}

	unsigned char *nullFieldsIndicator = (unsigned char *) malloc(1);
	memset(nullFieldsIndicator, 0, 1);

	if(op == COUNT) {
		floatField = count;
	} else {
		if(attr.type == TypeInt)
			floatField = intField;
		if(op == AVG)
			floatField /= count;
	}
	memcpy((char*)data+1, &floatField, sizeof(float));

	memcpy(data, nullFieldsIndicator, 1);
	free(nullFieldsIndicator);
	free(tuple);
	flag = true;
	return 0;
}

void Aggregate::getAttributes(vector<Attribute> &attrs) const {
	attrs.clear();
	Attribute temp;
	temp = attr;
	switch(op) {
		case MIN: {
			temp.name = "MIN(" + attr.name + ")";
			break;
		}
		case MAX: {
			temp.name = "MAX(" + attr.name + ")";
			break;
		}
		case COUNT: {
			temp.name = "COUNT(" + attr.name + ")";
			break;
		}
		case SUM: {
			temp.name = "SUM(" + attr.name + ")";
			break;
		}
		case AVG: {
			temp.name = "AVG(" + attr.name + ")";
			break;
		}
	}
	attrs.push_back(temp);
}

RC Iterator::fetchField(void *record, vector<Attribute> &attrs, void *data, Attribute &attr, bool &isNull) {
	int count = 0;
	int offset = 0;
	int nullIndicatorLength = ceil((double)attrs.size() / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullIndicatorLength);
	memcpy(nullFieldsIndicator, record, nullIndicatorLength);
	offset = nullIndicatorLength;
	for(int i = 0; i < attrs.size(); i++) {
		if(attrs[i].name == attr.name) {
			if(nullFieldsIndicator[i/8] & (1 << (7-(i%8)))) {
				isNull = true;
				break;
			}
			if(attr.type == TypeVarChar) {
				int len;
				memcpy(&len, (char*)record+offset, sizeof(int));
				memcpy(data, (char*)record+offset, sizeof(int)+len);
				isNull = false;
				break;
			} else {
				memcpy(data, (char*)record+offset, sizeof(int));
//				cout << *(int *)data << endl;
				isNull = false;
				break;
			}
		} else {
			count++;
			if(nullFieldsIndicator[i/8] & (1 << (7-(i%8)) )) {
				continue;
			}
			if(attrs[i].type == TypeVarChar) {
				int len = 0;
				memcpy(&len, (char*)record+offset, sizeof(int));
				offset += (sizeof(int) + len);
			} else {
				offset += sizeof(int);
			}
		}
	}
	free(nullFieldsIndicator);
	if(count == attrs.size())
		return -1;
	return 0;
}

void Iterator::joinTuples(void *data, void *leftTuple, void *rightTuple, vector<Attribute> &leftAttrs, vector<Attribute> &rightAttrs) {
	int nullIndicatorLength = ceil((double)(leftAttrs.size()+rightAttrs.size()) / CHAR_BIT);
	unsigned char *nullFieldsIndicator = (unsigned char *) malloc(nullIndicatorLength);
	memset(nullFieldsIndicator, 0, nullIndicatorLength);
	int lNullIndicatorLength = ceil((double)leftAttrs.size() / CHAR_BIT);
	unsigned char *lNullFieldsIndicator = (unsigned char *) malloc(lNullIndicatorLength);
	memcpy(lNullFieldsIndicator, leftTuple, lNullIndicatorLength);
	int rNullIndicatorLength = ceil((double)rightAttrs.size() / CHAR_BIT);
	unsigned char *rNullFieldsIndicator = (unsigned char *) malloc(rNullIndicatorLength);
	memcpy(rNullFieldsIndicator, rightTuple, rNullIndicatorLength);

	int offset = nullIndicatorLength;
	int lOffset = lNullIndicatorLength;
	int rOffset = rNullIndicatorLength;

	for(int i = 0; i < leftAttrs.size(); i++){
		if(lNullFieldsIndicator[i/8] & (1 << (7-(i%8)) )) {
			nullFieldsIndicator[i/8] = nullFieldsIndicator[i/8] | (1 << (7-(i%8)));
			continue;
		}
		int len = 0;
		if(leftAttrs[i].type == TypeVarChar) {
			memcpy(&len, (char*)leftTuple+lOffset, sizeof(int));
			len += sizeof(int);
		} else {
			len = sizeof(int);
		}
		memcpy((char*)data+offset, (char*)leftTuple+lOffset, len);
		offset += len;
		lOffset += len;
	}
	for(int i = 0; i < rightAttrs.size(); i++){
		if(rNullFieldsIndicator[i/8] & (1 << (7-(i%8)) )) {
			nullFieldsIndicator[(i+leftAttrs.size())/8] = nullFieldsIndicator[(i+leftAttrs.size())/8] | (1 << (7-((i+leftAttrs.size())%8)));
			continue;
		}
		int len = 0;
		if(rightAttrs[i].type == TypeVarChar) {
			memcpy(&len, (char*)rightTuple+rOffset, sizeof(int));
			len += sizeof(int);
		} else {
			len = sizeof(int);
		}
		memcpy((char*)data+offset, (char*)rightTuple+rOffset, len);
		offset += len;
		rOffset += len;
	}
	memcpy(data, nullFieldsIndicator, nullIndicatorLength);
	free(nullFieldsIndicator);
	free(lNullFieldsIndicator);
	free(rNullFieldsIndicator);
}
