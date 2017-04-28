
dra =    { '$abs': {'$subtract': [ {'$arrayElemAt': ['$ns.loc', 0]}, {'$arrayElemAt': ['$loc', 0]}] } }
dra2 =   { '$multiply': [dra, dra] }

ddecl =  { '$abs': {'$subtract': [ {'$arrayElemAt': ['$ns.loc', 1]}, {'$arrayElemAt': ['$loc', 1]}] } }
ddecl2 = { '$multiply': [ddecl, ddecl] }

dist =   { '$sqrt':  { '$add': [ dra2, ddecl2] } }

ra = 0.
decl = 0.
ext = 10.
bottomleft = [ ra - ext, decl - ext ]
topright = [ ra + ext, decl + ext ]

pipeline = [
  {$geoNear:
    {'near': [0, 0],
     'query': { 'loc': { '$geoWithin': {'$box': [bottomleft, topright] }  } },
     distanceField: 'dist' }
  },
  {'$lookup': {'from':'y', 'localField':'y.loc', 'foreignField':'y.loc', 'as':'ns'} },
  {'$unwind': '$ns'},
  {'$addFields': {'dist': dist} },
  {'$match': { '$and': [ { 'dist': { '$gt': 0 } }, { 'dist': { '$lt': 1 } } ] } },
  {'$project': {'_id': 0, 'loc':1, 'ns.loc':1, 'dist': 1}},
]

db.y.aggregate(pipeline)

