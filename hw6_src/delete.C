#include "catalog.h"
#include "query.h"

/*
 * Deletes records from a specified relation.
 *
 * Returns:
 * 	OK on success
 * 	an error code otherwise
 */

const Status QU_Delete(const string & relation, 
		       const string & attrName, 
		       const Operator op,
		       const Datatype type, 
		       const char *attrValue)
{
	
	Status status; 
	RID outrid; 

	HeapFileScan* hfs = new HeapFileScan(relation,status);

	if(status != OK)
	{
		return status; 
	}

	AttrDesc record; 
	attrCat->getInfo(relation,attrName,record); 
	int length = record.attrLen;
	int offset = record.attrOffset; 

	// if(attrName == '\0')
	// {
	// 	status = hfs->startScan(0,0,STRING,NULL,EQ);
	// 	if(status != OK)
	// 	{
	// 		delete hfs; 
	// 		return status; 
	// 	}
	// }
	// else
	// {
	int val;
	float fval;
		if(type == STRING)
		{
			status = hfs->startScan(offset,length,type,attrValue,op); 
			if(status != OK)
			{
				delete hfs; 
				return status; 
			}
		}
		else if (type == INTEGER)
		{
			val = atoi(attrValue); 
			status = hfs->startScan(offset,length,type,(char *)&val,op); 
			if(status != OK)
			{
				delete hfs; 
				return status; 
			}
		}
		else //float 
		{
			fval = atof(attrValue); 
			status = hfs->startScan(offset,length,type,(char *)&val,op); 
			if(status != OK)
			{
				delete hfs; 
				return status; 
			}
		}


	while(hfs->scanNext(outrid) == OK)
	{
		hfs->deleteRecord();
	}

	hfs->endScan(); 
	delete hfs; 

return OK;
}


