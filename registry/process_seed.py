# TBD: Assign ids.

# Artifact = registry operation = a resource or a capture.

# Loads and validates the capture list organized by resource and issue.
#
# When invoked as shell command, writes out normalized form.

import json, os, sys, csv, codecs

default_seed_meta_path = 'seed.json'

# Convert 'seed' form (resource / issue / capture from seed.json file)
# to list of registry operations (interleaved resource registration
# and capture registration).

def load_seed_events(seed_meta_path=default_seed_meta_path):
    with open(seed_meta_path) as infile:
        seed_meta = json.load(infile)
        for rmeta in seed_meta["resources"]: # Resource dictionary / blob
            # Performs lots of side effects on rmeta
            process_resource_meta(rmeta)
        return seed_meta_to_registry_commands(seed_meta)

# Validate the metadata and clobber all the dictionaries with new fields.
# Copy some fields down to issues and captures (inheritance).
# No return value.

def process_resource_meta(rmeta):
    for key in rmeta:
        if not key in resource_allowed_keys:
            print 'echo @@ invalid resource field', key
    if not "name" in rmeta:
        print 'echo @@ resource has no name', rmeta
        return
    earliest_date = None
    latest_prefix = None
    latest_suffix = None
    for issue in rmeta["issues"]:
        (cmeta, prefix, suffix) = process_capture_meta(issue, rmeta)
        if cmeta != None and earliest_date == None:
            earliest_date = cmeta['date']
            rmeta['date'] = earliest_date
        if prefix != None: latest_prefix= prefix
        if suffix != None: latest_suffix= suffix
    rmeta['_prefix'] = latest_prefix
    rmeta['_suffix'] = latest_suffix

# Moves issue metadata down into capture metadata.
# Returns (capture metadata blob or None, prefix, suffix).

def process_capture_meta(issue, rmeta):

    stem = issue["name"] # Globally unique.

    # Validate issue fields
    for key in issue:
        if not key in issue_allowed_keys:
            print 'echo @@ unrecognized key in issue', key, stem

    fail = (None, None, None)

    # Get capture metadata for the (now) unique capture for this issue
    (cmeta, suffix) = normalize_issue_capture(issue, rmeta)
    if cmeta == None:
        print 'echo @@ no archivable capture', stem
        return fail
    cmeta['_type'] = 'capture'

    # Globally unique name for indexing, file system, etc.
    cmeta['name'] = stem

    # Resource that this is a part of
    rname = rmeta["name"]
    cmeta['capture_of'] = rname

    # Legal is a bit complicated
    legal = cmeta["legal"]
    if not legal in ["pd", "cc0", "public"]:
        # print 'echo @@ not known to be archivable', stem, suffix
        cmeta['archivable'] = False

    # Inheritance from resource
    if "ott_idspace" in rmeta:
        cmeta['_ott_idspace'] = rmeta["ott_idspace"]

    # Inheritance from issue
    if "===" in issue:
        if "===" in cmeta:
            cmeta["==="] = cmeta["==="] + u' - ' + issue["==="]
        else:
            cmeta["==="] = issue["==="]

    # Set the date field for the capture - needed for sorting
    cmeta['date'] = generic_date(cmeta)

    # Filename = (prefix + relative_name) + suffix = stem + suffix
    # (Analyzr this so that we can create more capture filenames in the future.)

    # Default capture file location is files.opentreeoflife.org/resource/iname/
    #  which contains capture files (zips, tarballs)
    if not 'name' in issue:
        print 'echo @@ issue has no name', issue
        return fail
    prefix = rmeta.get('issue_prefix')
    if prefix == None:
        prefix = rname
    if not stem.startswith(prefix):
        print '** how to split this into prefix + relativename?', stem
        return fail
    i = len(prefix)
    if stem[i] == '-' or stem[i] == '.':
        i += 1
    prefix = stem[0:i]

    relative_name = stem[len(prefix):] # Unique within resource.
    cmeta['_relative_name'] = relative_name

    cmeta['_filename'] = stem + suffix   # Filename, globally unique
    return (cmeta, prefix, suffix)

# For a given issue blob, find and canonicalize the canonical capture blob

def normalize_issue_capture(issue, rmeta):
    def norm(capture_field, suffix_field, suffix_default, legal):
        cmeta = issue.get(capture_field)
        if cmeta == None:
            return None
        # Validate before doing any munging
        for key in cmeta:
            if not key in capture_allowed_keys:
                print 'echo @@ unrecognized key in capture', key
        if not ("locations" in cmeta or "bytes" in cmeta):
            return None
        suffix = rmeta.get(suffix_field)
        if suffix == None: suffix = suffix_default
        cmeta['legal'] = legal
        return (cmeta, suffix)
    legal = issue.get("legal")
    if legal == None: legal = rmeta.get("legal")
    s1 = norm("original", "original_suffix", ".original", legal)
    s2 = norm("derived", "derived_suffix", ".derived", "cc0")
    if s2 != None:
        if s1 != None:
            print '** Issue has both original and target captures', issue
        (cmeta, suffix) = s2
    elif s1 != None:
        (cmeta, suffix) = s1
    else:
        return (None, None)
    issue['_capture'] = cmeta   # Cache it for issue_capture
    return (cmeta, suffix)


allowed_keys = ["name", "retrieved_from", "original_suffix", "derived_suffix", "legal", "===", "ott_idspace"]
resource_allowed_keys = allowed_keys + ["issues", "description", "capture_description", "issue_prefix", "doi"]
issue_allowed_keys = allowed_keys + ["original", "derived", "derived_from"]
capture_allowed_keys = allowed_keys + ["locations", "bytes", "doi",
                                        "references", "from", "commit",
                                        "retrieved_from", "retrievable_from",
                                        "generated_on", "last_modified", "publication_date"]

# This is called *after* a fair amount of processing has been done on
# the JSON

def seed_meta_to_registry_commands(seed_meta):
    events = []
    for rmeta in seed_meta["resources"]:
        events.extend(resource_meta_to_registry_commands(rmeta))
    return fix_corpus(events)

def resource_meta_to_registry_commands(rmeta):
    events = []
    new_rmeta = {'name': rmeta['name'], '_type': 'resource'}
    for prop in ["description", "capture_description", "===", "ott_idspace", "doi",
                 '_prefix', '_suffix']:
        if prop in rmeta:
            new_rmeta[prop] = rmeta[prop]
    # Set the date field of the resource - needed for sorting
    new_rmeta['date'] = rmeta['date']
    events.append(new_rmeta)

    # Move suffix and prefix from capture to resource
    suffix = None
    prefix = None
    for issue in rmeta["issues"]:
        cmeta = issue['_capture']
        if cmeta != None:
            events.append(cmeta)
    return events

# Things to do with the sorted list of operations (resources + captures).

def fix_corpus(events):
    # Index by name
    by_name = index_by_name(events)

    events = by_name.values()
    events = sorted(events, key=event_sort_key)

    # not needed any more?
    flush_by_doi(events)

    # Make sure there are no broken or forward references
    seen = {}
    def check_forward_refs(name, event):
        if name == None:
            True                # no reference at all
        elif name in seen:
            True                # backward reference
        elif name in by_name:
            print '** forward reference', event_name(event), name
        else:
            print '** dangling reference', event_name(event), name
    for event in events:
        for prop in ["derived_from", 'capture_of']:
            if prop in event:
                check_forward_refs(event.get(prop), event)
        if 'references' in event:
            for key in event['references']:
                check_forward_refs(event['references'][key], event)
        seen[event_name(event)] = True

    # Reconstruct resource-to-capture bindings for OTT
    sources = {}                # maps idspace to capture
    for event in events:
        # we only care about the captures, not the resources
        if event.get('capture_of') == 'ott':
            refs = {}
            for key in sources: # copy sources dict
                r = sources[key]
                refs[key] = r['name']
            event['proposed_references'] = refs
        if "_ott_idspace" in event and ('locations' in event or 'bytes' in event):
            # chronological update
            sources[event["_ott_idspace"]] = event

    return events

# One DOI per resource

def flush_by_doi(events):
    # Eliminate redundant entries by DOI
    #  (could use other identity criteria too, later)
    by_doi = {}    # doi to name
    merges = {}    # name to name
    for event in events:
        if 'doi' in event:
            doi = event['doi']
            name = event_name(event)
            if doi in by_doi:
                merges[name] = by_doi[doi]
            else:
                by_doi[doi] = name
    for merged_name in merges:
        print '** deleting', merged_name, '->', merges[merged_name]
        del by_name[merged_name]
    # Snap merge pointers
    for event in events:
        to_name = event.get("derived_from")
        if to_name in merges:
            # print '** snapping', to_name, merges[to_name]
            event["derived_from"] = merges[to_name]

def issue_captures(issue):
    cmeta = issue.get('_capture')
    if cmeta == None:
        return []
    else:
        return [cmeta]

def event_sort_key(event):
    lateness = 1
    if event.get('_type') == 'resource': lateness = 0
    if event.get('capture_of') == 'ott': lateness = 2
    return (generic_date(event), lateness)

def index_by_name(events):
    by_name = {}
    for event in events:
        name = event['name']
        if name in event:
            print '** duplicate!', name
        else:
            by_name[name] = event
    return by_name

date_keys = ["date", "generated_on", "last_modified", "publication_date", "start_date"]

def generic_date(artifact):
    for key in artifact:
        if key in date_keys:
            return artifact[key]
    print '** no date found', artifact.get('name')

def event_name(event):
    return event['name']

def capture_name(artifact):
    return artifact['name']

def capture_filename(artifact):
    return artifact['_filename']

# Path on files.opentreeoflife.org

def capture_path(cmeta):
    if 'capture_of' in cmeta:
        return os.path.join(cmeta['capture_of'], capture_name(cmeta), capture_filename(cmeta))
    else:
        print '** no resource for this capture', cmeta['name']

# reserve magic ids for future use
# or maybe all of them below 100 or below 50

magic_ids = [1, 3, 5, 7, 9, 11, 13, 15, 17, 18, 19, 21, 23, 26, 27, 28, 29, 30,
             31, 32, 33, 34, 36, 37, 39, 40, 43, 45, 46]

available_ids = [51, 52, 55, 57, 61, 62, 64, 66, 68, 70, 72, 74, 75,
                 76, 77, 78, 79, 80, 81, 83, 84, 97, 99, 101, 106,
                 108, 111, 113, 115, 116, 117, 120, 144, 145, 147,
                 151, 153, 154, 156, 157, 158, 160, 161, 162, 163,
                 165, 166, 168, 170, 172, 174, 202, 203, 204, 205,
                 228, 229, 232, 233, 238, 240, 241, 242, 243, 244,
                 245, 247, 249, 253, 255, 256]

def doit(seed_meta_path):
    arts = load_seed_events(seed_meta_path)
    out = sys.stdout
    json.dump(arts, out, indent=2)
    

if __name__ == '__main__':
    doit(seed_meta_path)
