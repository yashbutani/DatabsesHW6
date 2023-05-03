#include "catalog.h"
#include "query.h"
#include <string.h>


// forward declaration
const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen);

/*
 * Selects records from the specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Select(const string & result, 
		       const int projCnt, 
		       const attrInfo projNames[],
		       const attrInfo *attr, 
		       const Operator op, 
		       const char *attrValue)
{
   // Qu_Select sets up things and then calls ScanSelect to do the actual work
    cout << "Doing QU_Select " << endl;

	Status status;
	AttrDesc descripts;
	AttrDesc* descs;
	// char * filter;
	int length = 0;

	descs = new AttrDesc[projCnt];

	for(int i = 0; i < projCnt; i++) {
		status = attrCat->getInfo(projNames[i].relName, projNames[i].attrName, descs[i]);
		length += descs[i].attrLen;
		if (status != OK) return status;
	}

	Operator inputop;
	const char *filterVal;

	 if (attr != NULL) {
		status = attrCat->getInfo(attr->relName, attr->attrName, descripts);
		float ffil;
		int intfil;

		if (attr->attrType == FLOAT) {
			ffil = atof(attrValue);
			filterVal = (char*)&ffil;
		}
		else if (attr->attrType == INTEGER) {
			intfil = atoi(attrValue);
			filterVal = (char*)&intfil;
		}
		else {
			filterVal = attrValue;
		}
		if (status != OK) return status;
		inputop = op;
	 }
	 else {
        status = attrCat->getInfo(projNames[0].relName, projNames[0].attrName, descripts);
        if (status != OK) return status;

		strcpy(descripts.relName, projNames[0].relName);
        strcpy(descripts.attrName, projNames[0].attrName);
        
		inputop = EQ;
		filterVal = NULL;

        descripts.attrLen = 0;
        descripts.attrType = STRING;
        descripts.attrOffset = 0;
    }

status = ScanSelect(result, projCnt, descs, &descripts, inputop, filterVal, length);
if (status != OK) return status;
return OK;

}


const Status ScanSelect(const string & result, 
			const int projCnt, 
			const AttrDesc projNames[],
			const AttrDesc *attrDesc, 
			const Operator op, 
			const char *filter,
			const int reclen) {
cout << "Doing HeapFileScan Selection using ScanSelect()" << endl;

    Status status; 
    RID rid; 
    char out[reclen];
    Record new_record; 
    new_record.data = out; 
    new_record.length = reclen; 
    Record generic; 

    InsertFileScan relation(result, status);
    HeapFileScan scan(attrDesc->relName, status);

    status = scan.startScan(attrDesc->attrOffset, attrDesc->attrLen, (Datatype)attrDesc->attrType, filter, op);
    if (status != OK) return status;
    
    while (true)
    {
		RID insertion; 
        int len = 0; 
        status = scan.scanNext(rid); 
        if (status != OK) return OK; 

        status = scan.getRecord(generic); 
        if (status != OK) return status;

        for (int i = 0; i < projCnt; i++) {
            memcpy(len + out, (char *) generic.data + projNames[i].attrOffset, projNames[i].attrLen); 
			len = len + projNames[i].attrLen; 
        }
        relation.insertRecord(new_record,insertion); 
    }
    return OK;
}
