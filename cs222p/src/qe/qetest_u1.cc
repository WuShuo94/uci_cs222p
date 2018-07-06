#include <fstream>
#include <iostream>
#include <vector>
#include <cstdlib>
#include <cstdio>
#include <cstring>

#include "qe_test_util.h"

int testCase_u1() {
	// Mandatory for all
	// Create an Index
	// Load Data
	// Create an Index

	RC rc = success;
	cerr << endl << "***** In QE Test Case u1 *****" << endl;

	// Create an index before inserting tuples.
	rc = createIndexforLeftB();
	if (rc != success) {
		cerr << "***** createIndexforLeftB() failed.  *****" << endl;
		return rc;
	}
	// Insert tuples.
	rc = populateLeftTable();
	if (rc != success) {
		cerr << "***** populateLeftTable() failed.  *****" << endl;
		return rc;
	}

	// Create an index after inserting tuples - should reflect the currently existing tuples.
	rc = createIndexforLeftC();
	if (rc != success) {
		cerr << "***** createIndexforLeftC() failed.  *****" << endl;
		return rc;
	}
	return rc;
}


int main() {
	int i;
	int *pi;
	pi = &i;
	pi = NULL;
	if(i == NULL)
		cout << "i = NULL" << endl;

	int len = 3;
	char *pc = (char*)malloc(len+1);
	pc[0] = 'a';
	pc[1] = 'b';
	pc[2] = 'c';
	pc[len] = '\0';
	unordered_map< string, void *> myMap;
	myMap[pc] = pc;
	unordered_map< string, void *>::const_iterator mapValues = myMap.find(pc);
	if(mapValues != myMap.end()) {
		void *data = malloc(PAGE_SIZE);
		data = mapValues -> second;
		cout << "map.value = " << (char*)data << endl;
	}
}
