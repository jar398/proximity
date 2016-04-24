# TBD:
#   - count lost (deprecated) taxa & annotate point of departure
#   - put stats in tabular form


import sys, os, csv
import recap.registry

version_count = 3

recapreg = recap.registry.Registry('var/recap/initial_resources.json', 'var/recap/initial_captures.json')

versions = ['1.0', '2.0','2.1', '2.2', '2.3', '2.4',
            '2.5', '2.6', '2.7', '2.8', '2.9']

# versions2 = map(lambda v: v['relative_name'], artifact.load_artifacts(artifact.directory_path))

def process_registry(start, count):
    cmetas = recapreg.all_captures('ott')
    print len(cmetas), 'OTT versions'
    registry = {}               # maps OTT id to TNU i.e. to (qid, capturename)
    equivalences = {}           # maps qualified id to OTT id
    merges = {}                 # maps OTT id to OTT id
    i = 0
    ott = {}
    for ott_version in cmetas:
        next_ott = register_one_version(ott_version, ott, registry, equivalences)
        i += 1
        if i >= count: break
    return registry

def register_one_version(ott_version, previous_ott, registry, equivalences):
    ott = {}
    ott_sources = ott_version['sources']  # maps idspace -> capture
    # What would smasher do?
    tpath = get_taxonomy_path(ott_version)   # path to taxonomy.tsv
    if tpath == None:
        # diagnostic has already been printed
        return
    size_before = len(registry)
    print 'reading', tpath
    with open(tpath, 'r') as infile:
        sep = '\t|\t'
        header = infile.next().split(sep)
        def get_col(name):
            if name in header:
                return header.index(name)
            else:
                return None
        uid_col = get_col('uid')
        source_col = get_col('source')
        sourceid_col = get_col('sourceid')
        sourceinfo_col = get_col('sourceinfo')
        name_col = get_col('name')
        # Returns list of qid.  A qid is an (idspace, id) pair.
        def get_source_list(row):
            if source_col != None:
                return [(row[source_col], row[sourceid_col])]
            else:
                sourceinfo = row[sourceinfo_col]
                source_list = []
                for qid in sourceinfo.split(','):
                    if qid.startswith('http'):
                        source_list.append((qid, row[name_col]))
                    else:
                        s = qid.split(':', 1)
                        idspace = s[0]
                        if len(s) > 1:
                            source_list.append((idspace, s[1]))
                        else:
                            print row
                            source_list.append((idspace, row[name_col]))
                return source_list
        novel = {}    # New ids in this OTT version
        changes = []
        for line in infile:
            row = line.split(sep)
            # 0=uid	|	1=parent_uid	|	2=name	|	3=rank	|	4=source	|	5=sourceid	|	sourcepid	|	uniqname	|	preottol_id	|	
            id = int(row[uid_col])
            source_list = get_source_list(row)
            registration = registry.get(id)
            if registration == None:
                # New registration
                registered_qid = source_list[0]
                (idspace, _) = registered_qid
                capture_name = ott_sources.get(idspace)
                if capture_name == None:
                    # Probably comes from one of the patch files!
                    capture_name = idspace
                tnu = (registered_qid, capture_name)
                novel[id] = tnu
                registry[id] = tnu
            change = check_source_list(source_list, id, equivalences, changes)

    print ' added', len(novel), 'ids'
    write_registry(novel, 'var/ottid_registry/%s-ids.csv' % ott_version['name'])
    write_changes(changes, 'var/ottid_registry/%s-changes.csv' % ott_version['name'])

def check_source_list(source_list, id, equivalences, changes):
    # Update equivalences for all source qids
    ids = {}
    found_id = False
    for qid in source_list:
        # See where all the qids map.
        # If unmapped, map it to this id.
        # If mapped, check for conflict.
        other_id = equivalences.get(qid)
        if other_id != None:
            if other_id == id:
                found_id = True
            else:
                ids[other_id] = qid
        equivalences[qid] = id
    n = len(ids)
    if n == 0: return
    qidstuff = ';'.join(map((lambda qid: '%s:%s' % qid), ids.values()))
    idstuff = ';'.join(map(str, ids.keys()))
    if found_id:
        mode = 'merge_in'
    else:
        if n == 1:
            mode = 'change_id'
        else:
            mode = 'form_new'
    changes.append((id, qidstuff, idstuff, mode))
    return None

def write_changes(changes, outpath):
    with open(outpath, 'w') as outfile:
        print 'writing', outpath
        writer = csv.writer(outfile)
        writer.writerow(('id', 'qids', 'from_ids', 'mode'))
        for row in changes:
            writer.writerow(row)

# the tarball filename is used as a key in the version registry (for getting id)

def get_taxonomy_path(ott_version):
    resource_name = 'ott'
    rmeta = recapreg.get_resource(resource_name)
    path = os.path.join('../files.opentreeoflife.org', recap.registry.get_capture_path(ott_version, rmeta))

    if not os.path.exists(path):
        print '** cannot find', path
        return None

    cdir = os.path.dirname(path)

    # Places to look for unpacked version
    unpack1 = os.path.join(cdir, ott_version['name'])
    unpack2 = os.path.join(cdir, resource_name)

    if os.path.isdir(unpack1):
        unpack = unpack1
    elif os.path.isdir(unpack2):
        unpack = unpack2
    else:
        print '** need to unpack', path
        print '(cd %s; tar xzvf %s)' % (vdir, tarball)
        return None

    tpath = os.path.join(unpack, 'taxonomy')
    if os.path.exists(tpath):
        return tpath
    else:
        tpath = os.path.join(unpack, 'taxonomy.tsv')
        if os.path.exists(tpath):
            return tpath
        else:
            print '** cannot find', tpath
            return None

    return tpath

def write_registry(registry, outpath):
    with open(outpath, 'w') as outfile:
        print 'writing', outpath
        writer = csv.writer(outfile)
        writer.writerow(('id', 'idspace', 'id_in_idspace', 'capture'))
        items = registry.items()
        items.sort(key=lambda(id,record):id)
        for (id, ((idspace, qu_id), capture)) in items:
            writer.writerow((id, idspace, qu_id, capture))

process_registry(int(sys.argv[1]), int(sys.argv[2]))
