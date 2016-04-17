# Register one resource or version

import os, json, registry

default_registry_dir = 'the_registry'

#  The registry

class Registry:
    def __init__(self, registry_dir=default_registry_dir):
        if not os.path.isdir(registry_dir):
            os.mkdir(registry_dir)
        self.registry_dir = registry_dir
        self.resources = Table(os.path.join(registry_dir, 'resources.json'))
        self.captures = Table(os.path.join(registry_dir, 'captures.json'))
        self.unused_ids_path = os.path.join(registry_dir, 'unused_ids.json')

    def get_resource(self, name):
        return self.resources.get(name)

    def register_resource(self, rmeta):
        rmeta = validate_resource(rmeta)
        self.resources.put(rmeta['name'], rmeta)
        self.resources.flush()

    def register_capture(self, cmeta):
        cmeta = validate_capture(cmeta, self)
        self.captures.put(cmeta['name'], cmeta)
        if not 'id' in cmeta:
            cmeta['id'] = self.next_special_id()
        self.captures.flush()

    def next_special_id(self):
        if not os.path.exists(self.unused_ids_path):
            self.wipe_unused()
        with open(self.unused_ids_path) as infile:
            ids = json.load(infile)
        id = ids[0]
        ids = ids[1:]
        with open(self.unused_ids_path, 'w') as outfile:
            json.dump(ids, outfile, indent=1)
        return id

    def all_resources(self):
        return sorted(self.resources.values(), key=lambda rmeta:rmeta['date'])

    def all_captures(self, capture_of=None):
        captures = self.captures.values()
        if capture_of != None:
            all = []
            for cmeta in captures:
                if cmeta['capture_of'] == capture_of:
                    all.append(cmeta)
        return sorted(captures, key=lambda cmeta:cmeta['date'])

    def wipe(self):
        self.resources.wipe()
        self.captures.wipe()
        self.wipe_unused()

    def wipe_unused(self):
        with open(self.unused_ids_path, 'w') as outfile:    # overwrite
            json.dump(initial_unused_ids, outfile, indent=1)

# Unused ids (handy gaps in OTT 1.0 sequence)

initial_unused_ids = [7, 9, 11, 13, 15, 17, 18, 19, 21, 23,
    26, 27, 28, 29, 30, 31, 32, 33, 34, 36, 37, 39, 40, 43, 45, 46,
    51, 52, 55, 57, 61, 62, 64, 66, 68, 70, 72, 74, 75, 76, 77, 78,
    79, 80, 81, 83, 84, 97, 99, 101, 106, 108, 111, 113, 115, 116,
    117, 120, 144, 145, 147, 151, 153, 154, 156, 157, 158, 160, 161,
    162, 163, 165, 166, 168, 170, 172, 174, 202, 203, 204, 205, 228,
    229, 232, 233, 238, 240, 241, 242, 243, 244, 245, 247, 249, 253,
    255, 256, 257, 259, 261]

cleanp = False

def validate_resource(rmeta):
    if cleanp:
        # Extract a subset of the original blob for registration.  E.g. flush 
        # issue_prefix (replaced by _prefix).
        return {'name': rmeta["name"],
                'description': rmeta["description"],
                'date': rmeta['start_date']}
    else:
        return rmeta

def validate_capture(cmeta, registry):
    capture_of = cmeta["capture_of"]
    # Validate the resource name
    if registry.get_resource(capture_of) == None:
        print '** Unrecognized resource!', capture_of, cmeta['name']
        return
    if cleanp:
        return {'name': cmeta['name'],
                'capture_of': capture_of,
                'date': cmeta['date'],
                'legal': cmeta['legal']}
    else:
        return cmeta




# Tables
        
class Table:
    def __init__(self, path):
        self.path = path
        self.index_by_name = None
        self.index_by_id = None
    def refresh(self):
        if os.path.exists(self.path):
            with open(self.path) as infile:
                print 'reading', self.path
                bloblist = json.load(infile)
                self.index_by_name = index_by_field(bloblist, 'name')
                if bloblist[0].get('id') != None:
                    self.index_by_id = index_by_field(bloblist, 'id')
        else:
            self.index_by_name = {}
            self.index_by_id = {}
    def swapin(self):
        if self.index_by_name == None:
            self.refresh()
    def flush(self):
        bloblist = sorted(self.index_by_name.values(), key=lambda dict:dict["date"])
        with open(self.path, 'w') as outfile:
            # print 'writing', self.path
            json.dump(bloblist, outfile, indent=2)

    def values(self):
        self.swapin()
        return self.index_by_name.values()
    def get(self, name):
        self.swapin()
        if self.index_by_name == None:
            self.refresh()
        return self.index_by_name.get(name)
    def get_by_id(self, id):
        self.swapin()
        return self.index_by_name.get(name)
    def put(self, name, blob):
        self.swapin()
        # See if blob is already in table
        name = blob['name']
        oldblob = self.index_by_name.get(name)
        if oldblob != None:
            for key in blob:
                if key in oldblob:
                    if blob[key] != oldblob[key]:
                        print '** Cannot change registered info!', name, key
                        return False
                else:
                    print '** Cannot add info to registration!', name, key
                    return False
            return True
        else:
            self.index_by_name[name] = blob
            if 'id' in blob: self.index_by_id[blob['id']] = blob
            # Not very safe!  This is just a prototype, fix later
            return True
    def wipe(self):
        if os.path.exists(self.path):
            os.remove(self.path)
        self.index_by_name = None
        self.index_by_id = None

def index_by_field(dict_list, key):
    index = {}
    for d in dict_list:
        index[d[key]] = d
    return index
