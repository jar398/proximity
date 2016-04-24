# TBD: Assign ids.

# Artifact = registry operation = a resource or a capture.

# Loads and validates the capture list organized by resource and issue.
#
# When invoked as shell command, writes out normalized form.

import json, os, sys, csv, codecs

default_setup_data_path = 'one_time_setup_data.json'

# Convert 'setup data' form (resource / issue / capture from setup data json file)
# to list of registry commands (interleaved resource registration
# and capture registration).

def get_setup_commands(setup_data_path=default_setup_data_path):
    setup_data = load_setup_data(setup_data_path)
    return setup_data_to_registry_commands(setup_data)

def load_setup_data(setup_data_path=default_setup_data_path):
    with open(setup_data_path) as infile:
        return json.load(infile)

# For validating the setup_data

allowed_keys = ["name", "retrieved_from", "original_suffix", "derived_suffix", "legal", "===", "ott_idspace"]
resource_allowed_keys = allowed_keys + ["issues", "description", "capture_description", "issue_prefix", "doi"]
issue_allowed_keys = allowed_keys + ["original", "derived", "derived_from"]
capture_allowed_keys = allowed_keys + ["locations", "bytes", "doi",
                                        "sources", "from", "commit",
                                        "retrieved_from", "retrievable_from",
                                        "generated_on", "last_modified", "publication_date"]

def setup_data_to_registry_commands(setup_data):
    commands = []
    for rsetup in setup_data["resources"]: # Resource dictionary / blob
        # Performs lots of side effects on rmeta
        commands.extend(process_resource_setup(rsetup))
    return fix_corpus(commands)

# Validate the metadata from the setup_data and clobber all the
# dictionaries with new fields.  Copy some fields down to issues and
# captures (inheritance).  No return value.

def process_resource_setup(rsetup):
    for key in rsetup:
        if not key in resource_allowed_keys:
            print 'echo @@ invalid resource field', key
    if not "name" in rsetup:
        print 'echo @@ resource has no name', rsetup
        return
    commands = []
    issues = rsetup["issues"]
    rmeta = dict(rsetup)
    del rmeta["issues"]

    rmeta['_type'] = 'resource'
    earliest_date = None
    latest_prefix = None
    latest_suffix = None
    commands.append(rmeta)
    for issue in issues:
        (cmeta, prefix, suffix) = process_issue(issue, rsetup)
        commands.append(cmeta)
        if cmeta != None and earliest_date == None:
            earliest_date = cmeta['date']
            rmeta['date'] = earliest_date
        if prefix != None: latest_prefix = prefix
        if suffix != None: latest_suffix = suffix
    # rmeta['_prefix'] = latest_prefix
    # rmeta['_suffix'] = latest_suffix
    if earliest_date != None:
        rmeta['date'] = earliest_date
    rmeta['_name_template'] = '%s{cap}' % (latest_prefix,)
    rmeta['_filename_template'] = '%s{cap}%s' % (latest_prefix, latest_suffix)
    return commands

# Moves resource and issue metadata down into capture metadata.
# Returns {capture metadata blob or None}, prefix, suffix.

def process_issue(issue, rsetup):

    cname = issue["name"]

    # Validate issue fields
    for key in issue:
        if not key in issue_allowed_keys:
            print 'echo @@ unrecognized key in issue', key, cname

    fail = (None, None, None)

    # Get initial capture metadata for the (now) unique capture for this issue
    (cmeta, suffix) = normalize_issue_capture(issue, rsetup)
    if cmeta == None:
        print 'echo @@ no archivable capture', cname
        return fail

    # Compute some new field values
    cmeta['_type'] = 'capture'
    cmeta['name'] = cname   # Globally unique
    cmeta['capture_of'] = rsetup["name"]  # Resource that this capture is a part of

    # Legal is a bit complicated
    legal = cmeta["legal"]
    if not legal in ["pd", "cc0", "public"]:
        # print 'echo @@ not known to be archivable', cname, suffix
        cmeta['_archivable'] = False

    # Inherit idspace from resource
    if "ott_idspace" in rsetup:
        cmeta['_ott_idspace'] = rsetup["ott_idspace"]

    # Inheritance comment from issue
    if "===" in issue:
        if "===" in cmeta:
            cmeta["==="] = cmeta["==="] + u' - ' + issue["==="]
        else:
            cmeta["==="] = issue["==="]

    # Set the date field for the capture - needed for sorting
    cmeta['date'] = generic_date(cmeta)

    # Set file name from issue/capture name plus suffix
    cmeta['_filename'] = cname + suffix   # Filename, globally unique

    # Resource needs prefix for generating new capture names
    prefix = rsetup.get("issue_prefix")
    if prefix == None:
        prefix = rsetup["name"]
    if not cname.startswith(prefix):
        print '** how to split this into prefix + relativename?', cname
        return fail
    i = len(prefix)
    if cname[i] == '-' or cname[i] == '.':
        i += 1
    prefix = cname[0:i]

    cmeta['_capture_label'] = cname[len(prefix):]  # Unique within resource.

    return (cmeta, prefix, suffix)

# For a given issue blob, find and canonicalize the canonical capture blob
# Bletcherous conversion from abandoned data model to simpler one

def normalize_issue_capture(issue, rsetup):
    def norm(capture_field, suffix_field, suffix_default, legal):
        csetup = issue.get(capture_field)
        if csetup == None:
            return None
        # Validate before doing any munging
        for key in csetup:
            if not key in capture_allowed_keys:
                print 'echo @@ unrecognized key in capture', key
        if not ("locations" in csetup or "bytes" in csetup):
            return None
        suffix = issue.get(suffix_field)
        if suffix == None: suffix = rsetup.get(suffix_field)
        if suffix == None: suffix = suffix_default
        cmeta = dict(csetup)
        cmeta['legal'] = legal
        return (cmeta, suffix)
    legal = issue.get("legal")
    if legal == None: legal = rsetup.get("legal")
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


# Things to do with the sorted list of commands (resources + captures).

def fix_corpus(commands):

    # Index by name
    by_name = index_by_name(commands)

    commands = by_name.values()
    commands = sorted(commands, key=event_sort_key)

    # not needed any more?
    flush_by_doi(commands)

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
    for event in commands:
        for key in ["derived_from", 'capture_of']:
            if key in event:
                check_forward_refs(event.get(key), event)
        if 'references' in event:
            for key in event['references']:
                check_forward_refs(event['references'][key], event)
        seen[event_name(event)] = True

    set_ott_sources(commands)

    return map(clean_command, commands)

def set_ott_sources(commands):
    # Kludge to reconstruct resource-to-capture bindings for OTT
    # Go through resources and captures chronologically
    sources = {}                # maps idspace to capture label
    resource_to_idspace = {}
    for command in commands:

        if 'ott_idspace' in command:
            # Command is a resource that contributes to OTT
            resource_to_idspace[command['name']] = command['ott_idspace']

        elif command.get('capture_of') == 'ott':
            # OTT capture (i.e. build) presumed to use latest captures of sources
            if len(sources) < 2:
                print '** not enough OTT sources', sources, command['name']
            else:
                command['sources'] = dict(sources)

        elif 'locations' in command or 'bytes' in command:
            # Capture is potential OTT source
            rname = command['capture_of']
            if rname in resource_to_idspace:
                sources[resource_to_idspace[rname]] = command['name']


def clean_command(command):
    new_command = {}
    for key in command:
        if not key in disallowed_keys:
            new_command[key] = command[key]
    return new_command

disallowed_keys = ["issues", "original_suffix", "derived_suffix",
                   '_ott_idspace', "locations"]

#    for key in ['name', '_type', "description", "capture_description", "===", "ott_idspace", "doi",
#                '_name_template', '_filename_template', 'sources', 'proposed_sources', 'date']:


# One DOI per resource

def flush_by_doi(commands):
    # Eliminate redundant entries by DOI
    #  (could use other identity criteria too, later)
    by_doi = {}    # doi to name
    merges = {}    # name to name
    for event in commands:
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
    for event in commands:
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

def index_by_name(commands):
    by_name = {}
    for event in commands:
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

def doit(setup_data_path):
    commands = get_setup_commands(setup_data_path)
    out = sys.stdout
    json.dump(commands, out, indent=2)
    

if __name__ == '__main__':
    # doit(default_setup_data_path)
    doit(sys.argv[1])
