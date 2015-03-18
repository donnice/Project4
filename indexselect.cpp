#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string.h>
#include <stdlib.h>

Status Operators::IndexSelect(const string& result,       // Name of the output relation
                              const int projCnt,          // Number of attributes in the projection
                              const AttrDesc projNames[], // Projection list (as AttrDesc)
                              const AttrDesc* attrDesc,   // Attribute in the selection predicate
                              const Operator op,          // Predicate operator
                              const void* attrValue,      // Pointer to the literal value in the predicate
                              const int reclen)           // Length of a tuple in the output relation
{
	cout << "Algorithm: Index Select" << endl;

	Status status;
	Status status2;
	
	status2 = OK;
	if(status2 != OK)
		return status2;

	HeapFile heapRes = HeapFile(result, status);
	if(status != OK) 
		return status;

	HeapFileScan Scanner = HeapFileScan(attrDesc->relName, status);
	if(status != OK) 
		return status;

	// Find indexed index

	Index index = Index(attrDesc->relName, attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, 1, status);
    	if (status != OK) 
		return status;

	// Do the scan
	status = index.startScan(attrValue);
	if(status != OK) 
		return status;	

	AttrDesc *rel;
	int len = 0, newLength = 0;

	status = attrCat->getRelInfo(result, len, rel);
	if(status != OK) return status;

	for(int i = 0; i < len; i++)
	{
		newLength += projNames[i].attrLen;
	}

	// store the pages
	RID rid;
	Record record, newRecord;
	while(index.scanNext(rid) != NOMORERECS)
	{
		// Return rid
		status = Scanner.getRandomRecord(rid, record);
		if(status != OK) 
			return status;

		// Copy memory into new Record
		newRecord.data = malloc(newLength);

		int k = 0;	
		for(int i = 0; i < len; i++)
		{
			k = rel[i].attrLen;
			memcpy((char *) newRecord.data + rel[i].attrOffset , (char *) record.data + projNames[i].attrOffset , k);
		}

		newRecord.length = newLength;

		// Store the new Record in the heap page
		status = heapRes.insertRecord(newRecord, rid); 
		if(status != OK) 
			return status;
	}
	status = index.endScan();
	if(status != OK) 
		return status;

  	return OK;
}

