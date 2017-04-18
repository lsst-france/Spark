import re

i = 0
with open('objects.log', 'rb') as f:
    for line in f:
        line = line.strip().decode('utf-8')
        m1 = re.match(".*[']ra['][:].([-]*[0-9]+[.][0-9]+)", line)
        m2 = re.match(".*[']decl['][:].([-]*[0-9]+[.][0-9]+)", line)
        m3 = re.match(".*[']loc['][:].[[]([-]*[0-9]+[.][0-9]+)[,].([-]*[0-9]+[.][0-9]+)", line)

        if m1 is None or m2 is None:
            continue

        i += 1
        ra = float(m1.group(1))
        decl = float(m2.group(1))
        loc_ra = float(m3.group(1))
        loc_decl = float(m3.group(2))

        if loc_ra == ra:
            print(i, 'ra=', ra, '\tdecl=', decl, '\tloc.ra', loc_ra,  '\tloc.decl', loc_decl)

    print(i)


