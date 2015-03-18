#include "catalog.h"
#include "query.h"
#include "index.h"
#include <string.h>
#include <stdlib.h>
#include "utility.h"

/* 
 * A simple scan select using a heap file scan
 */

Status Operators::ScanSelect(const string& result,       // Name of the output relation
                             const int projCnt,          // Number of attributes in the projection
                             const AttrDesc projNames[], // Projection list (as AttrDesc)
                             const AttrDesc* attrDesc,   // Attribute in the selection predicate
                             const Operator op,          // Predicate operator
                             const void* attrValue,      // Pointer to the literal value in the predicate
                             const int reclen)           // Length of a tuple in the result relation
{
  	cout << "Algorithm: File Scan" << endl;
  
	Status status;

	Status status2;
	status2 = OK;
	if(status2 != OK)
		return status2;

	// Declare Heap to read From
	HeapFileScan heapIn = HeapFileScan(projNames[0].relName, status);
	if(status != OK) 
		return status;

	if(attrDesc) // If there is a predicate
	{
		// Declare Heap to read from
		heapIn = HeapFileScan(attrDesc->relName, attrDesc->attrOffset, attrDesc->attrLen, 
					(Datatype)attrDesc->attrType, (const char*) attrValue, op, status);
		if(status != OK) 
			return status;
	
		// Start the scan of the heapIn
		status = heapIn.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, (const char*) attrValue, op);
		if(status != OK) 
			return status;
	}

	// Declare heap to write to
	HeapFile heapOut = HeapFile(result, status);
	if(status != OK) 
		return status;


	// Reset size for the new relation and update the length
	AttrDesc *r;
	int len = 0, updatedLength = 0;

	status = attrCat->getRelInfo(result, len, r);
	if(status != OK) return status;

	for(int i = 0; i < len; i++)
	{
		updatedLength += projNames[i].attrLen;
	}

	// Scan through all of the heap until scanNext returns 	
	RID rid;
	Record record, newRecord;

	while(heapIn.scanNext(rid, record) != FILEEOF)
	{
		// Copy memory into new Record
		newRecord.data = malloc(updatedLength);

		for(int i = 0; i < len; i++)
		{
			memcpy((char *) newRecord.data + r[i].attrOffset , (char *) record.data + projNames[i].attrOffset , r[i].attrLen);
		}

		newRecord.length = updatedLength;

		// Store the new Record in the heap page
		status = heapOut.insertRecord(newRecord, rid); 
		if(status != OK) 
			return status;
		if(status2 != OK)
			return status2;

	}

	// End the scan of heapIn
	status = heapIn.endScan();
	if(status != OK) 
		return status;



	return OK;
}
