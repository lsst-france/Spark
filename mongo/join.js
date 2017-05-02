min_ra   = db.y.find( {}, {'_id':0, 'loc':1}).sort( {'loc.0':  1} ).limit(1)[0]['loc'][0]
max_ra   = db.y.find( {}, {'_id':0, 'loc':1}).sort( {'loc.0': -1} ).limit(1)[0]['loc'][0]
min_decl = db.y.find( {}, {'_id':0, 'loc':1}).sort( {'loc.1':  1} ).limit(1)[0]['loc'][1]
max_decl = db.y.find( {}, {'_id':0, 'loc':1}).sort( {'loc.1': -1} ).limit(1)[0]['loc'][1]

print(min_ra, max_ra, min_decl, max_decl)

dra =    { '$abs': {'$subtract': [ {'$arrayElemAt': ['$ns.loc', 0]}, {'$arrayElemAt': ['$loc', 0]}] } }
dra2 =   { '$multiply': [dra, dra] }

ddecl =  { '$abs': {'$subtract': [ {'$arrayElemAt': ['$ns.loc', 1]}, {'$arrayElemAt': ['$loc', 1]}] } }
ddecl2 = { '$multiply': [ddecl, ddecl] }

dist =   { '$sqrt':  { '$add': [ dra2, ddecl2] } }

ra = 0.
decl = 0.
ext = 4
bottomleft = [ ra - ext, decl - ext ]
topright = [ ra + ext, decl + ext ]

db.z.drop()

pipeline = [
  {$geoNear:
    {'near': [0, 0],
     'query': { 'loc': { '$geoWithin': {'$box': [bottomleft, topright] }  } },
     distanceField: 'dist' }
  },
  {$out: 'z'},
]

//printjson(db.y.aggregate(pipeline, { explain: true }))
result = db.y.aggregate(pipeline, { cursor: { batchSize: 0 } })

while ( result.hasNext() ) {
   printjson( result.next()['dist'] );
}

print(db.z.count())

pipeline2 = [
  {'$lookup': {'from':'z', 'localField':'y.loc', 'foreignField':'z.loc', 'as':'ns'} },
  {'$unwind': '$ns'},
  {'$addFields': {'dist': dist} },
  {'$match': { '$and': [ { 'dist': { '$gt': 0 } }, { 'dist': { '$lt': 0.1 } } ] } },
  {'$project': {'_id': 0, 'loc':1, 'ns.loc':1, 'dist': 1}},
]

result = db.y.aggregate(pipeline2, { cursor: { batchSize: 0 } })

while ( result.hasNext() ) {
   printjson( result.next()['dist'] );
}


