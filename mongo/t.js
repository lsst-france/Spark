
print('find') 

result = db.y.find({ '_id': ObjectId("5908e63cd15fa104356eaf64") }, {'_id':1})

while ( result.hasNext() ) {printjson( result.next() );}

print('aggregate match direct')

result = db.y.aggregate( [ {'$match': {'_id': ObjectId("5908e63cd15fa104356eaf64") } }, {'$project': {'_id': 1}} ] )

while ( result.hasNext() ) {
   printjson( result.next() );
}

print('aggregate match with $eq')

while ( result.hasNext() ) {
   printjson( result.next() );
}

result = db.y.aggregate( [ {'$match': {'_id': {'$eq': ObjectId("5908e63cd15fa104356eaf64") } } }, {'$project': {'_id': 1}} ] )

while ( result.hasNext() ) {
   printjson( result.next() );
}

print('aggregate match with $ne')

result = db.y.aggregate( [ {'$match': {'_id': {'$ne': ObjectId("5908e63cd15fa104356eaf64") } } }, {'$project': {'_id': 1}}, {'$limit': 5} ] )

while ( result.hasNext() ) {
   printjson( result.next() );
}


