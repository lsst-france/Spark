
outname = 'u_psfFlux';

step = 500000
count = db.Object.count()

// step = 10;
// count = 5 * step;

out = db[outname];
out.drop();
out.insert({u_psfFlux:[]});

op1 = {$concatArrays: ['$u_psfFlux', '$x.u_psfFlux']};
op2 = {$arrayElemAt: [ '$x.u_psfFlux', 0 ]};
op3 = {$concatArrays: ['$u_psfFlux', op2]};

start = 0

print(count)

for(i = 0; start < count; i++)
{
  tmp = 'tmp';
  db[tmp].drop();

  skip = start;

  print(i, skip, start);

  db.Object.aggregate( 
    [ 
      {$project: {'u_psfFlux': 1} }, 
      {$skip:    skip},
      {$limit:   step},
      {$group:   {'_id': '', 'u_psfFlux': {$push: '$u_psfFlux' } } }, 
      {$out:     tmp} 
    ], 
    {allowDiskUse: true} 
  );

  op = {$lookup: {'from': tmp, 'localField': outname + '._id', 'foreignField': tmp + '._id', 'as': 'x'} };
  out.aggregate( [ op, {'$project': {'u_psfFlux': op3} }, {'$out': outname} ], {allowDiskUse: true});

  start += step;
}

out.aggregate( [ {$project: {a: {$slice: ['$u_psfFlux', 10]}}} ] );


