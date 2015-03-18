#include "catalog.h"
#include "query.h"
#include "sort.h"
#include "index.h"
#include <vector>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <cstring>
#include <string>
#include <queue>
#include "utility.h"

Status Operators::SNL(const string& result,           // Output relation name
                      const int projCnt,              // Number of attributes in the projection
                      const AttrDesc attrDescArray[], // Projection list (as AttrDesc)
                      const AttrDesc& attrDesc1,      // The left attribute in the join predicate
                      const Operator op,              // Predicate operator
                      const AttrDesc& attrDesc2,      // The left attribute in the join predicate
                      const int reclen)               // The length of a tuple in the result relation
{
  	cout << "Algorithm: Simple NL Join" << endl;

  	/* Your solution goes here */
	
	Status status;	
	string rel1(attrDesc1.relName);
	string rel2(attrDesc2.relName);
	RelDesc record1;
	RelDesc record2;

	//get descriptions for attributes of both relations
	AttrDesc* r1;
	AttrDesc* r2;
	status = attrCat->getRelInfo(attrDesc1.relName, record1.attrCnt, r1);
	if (status != OK) return status;
	status = attrCat->getRelInfo(attrDesc2.relName, record2.attrCnt, r2);
	if (status != OK) return status;
	
	HeapFileScan* heap2scan = new HeapFileScan(attrDesc2.relName, status);
	if (status != OK) 
		return status;
	

	//update relCat and attrCat
	//AttrDesc newattr[projCnt];
	int sets[projCnt];
	int size = 0;
	for (int i = 0; i < projCnt; i++)
	{
		sets[i] = size;
		size += attrDescArray[i].attrLen;
	
	}
	
	
	//new heapfile
	HeapFile* new_heap = new HeapFile(result, status);
	if (status != OK) return status;
	
	//scan and join
	Record rec1, rec2;
	RID rid1, rid2;
  
	status = heap2scan->startScan(attrDesc2.attrOffset, attrDesc2.attrLen, (Datatype)attrDesc2.attrType, NULL, op);
	if (status != OK) return status;
		
	while (heap2scan->scanNext(rid2, rec2) == OK)
	{	
		HeapFileScan* heap1scan = new HeapFileScan(rel1, status);
		if (status != OK) return status;
		status = heap1scan->startScan(attrDesc1.attrOffset, attrDesc1.attrLen, (Datatype)attrDesc1.attrType, NULL, op);
		if (status != OK) return status;
		while (heap1scan->scanNext(rid1, rec1) == OK)
		{
			if ( (op == LT && (matchRec(rec1, rec2, attrDesc1, attrDesc2) < 0)) || (op == LTE && (matchRec(rec1, rec2, attrDesc1, attrDesc2) <= 0)) || (op == GTE && (matchRec(rec1, rec2, attrDesc1, attrDesc2) >= 0)) || (op == GT && (matchRec(rec1, rec2, attrDesc1, attrDesc2) > 0)) || (op == NE && (matchRec(rec1, rec2, attrDesc1, attrDesc2) != 0)))
			{
				Record newrec;
				newrec.length = size;
				newrec.data = malloc(size);
				for (int i = 0; i < projCnt; i++)
				{
					if (strcmp(attrDescArray[i].relName, attrDesc1.relName) == 0) 
						memcpy((char*)newrec.data + sets[i], rec1.data + attrDescArray[i].attrOffset , attrDescArray[i].attrLen);	
					else 
						memcpy((char*)newrec.data + sets[i], rec2.data + attrDescArray[i].attrOffset , attrDescArray[i].attrLen);		
				}
				RID rid;
				status = new_heap->insertRecord(newrec, rid);
				free(newrec.data);
				if (status != OK) return status;
			}
			}
			
		status = heap1scan->endScan();
		if (status != OK) return status;
		delete heap1scan;
	}
	status = heap2scan->endScan();
	if (status != OK) return status;
	
	delete heap2scan;
	delete new_heap;
	
	

  return OK;
}
