# python audit.py /Users/jar/otrepo/files.opentreeoflife.org /Users/jar question: varela:/home/opentree/files.opentreeoflife.org/

import argparse
p = argparse.ArgumentParser()
# The default= clauses seem to have no effet
p.add_argument("repo",
               help="root of local files.opentreeoflife.org repo checkout, e.g. '/Users/jar/otrepo/files.opentreeoflife.org'",
               default='/Users/jar/otrepo/files.opentreeoflife.org')
p.add_argument("base",
               help="base directory for local paths, e.g. '/Users/jar'",
               default='/Users/jar')
p.add_argument("prefix",
               help="prefix that says a path is relative to base, e.g. 'question:'",
               default='question:')
p.add_argument("files",
               help="location base for files host, e.g. 'http://files.opentreeoflife.org/' or 'varela:/home/opentree/files.opentreeoflife.org/'",
               default='http://files.opentreeoflife.org/')

# os.path.realpath(symlink)

# Value of "legal" field:
#   OK to redistribute:
#     pd = public domain because US govt or out of copyright
#     cc0 = cc0
#     public = distributed to public on web without click-through license
#   anything else = license terms require something, do not redistribute:
#     handoff = received privately without nondisclosure or license
#     cc-by-v3.0 = copying requires attribution
#     $ = access requires payment, etc.

#   series/
#     issue1/
#       issue1.original
#       issue1.derived
#     issue2/
#       ...
#     ...

import os
import registry

# shuffle bits around to get local artifact store into shape

def audit(the_registry, repo, prefix, local, files_prefix):
    commands = []
    for rmeta in the_registry.all_resources():
        commands.extend(audit_resource(rmeta, repo))
    for cmeta in the_registry.all_captures():
        commands.extend(audit_capture(cmeta, repo, prefix, local, files_prefix))
    print 'set -e'
    for command in commands:
        print command

def audit_resource(rmeta, repo):
    resource_dir = os.path.join(repo, rmeta['name'])
    if os.path.isdir(resource_dir):
        print rmeta['name'], 'OK'
        return []
    else:
        return ["mkdir %s" % resource_dir]

def full_path(cmeta, repo):
    otpath = os.path.join(cmeta['capture_of'], cmeta['name'], cmeta['_filename'])
    return os.path.join(repo, otpath)

# Returns list of strings for commands needed to fix things

def quick_audit_capture(cmeta, repo):

    # otpath is relative to local repo clone.
    otpath = os.path.join(cmeta['capture_of'], cmeta['name'], cmeta['_filename'])
    dst = full_path(cmeta, repo)
    if os.path.exists(dst):
        if os.path.islink(dst):
            print '** expected a file but found symlink', dst
            return False
        elif os.path.isdir(dst):
            print '** expected a file but found directory', dst
            return False
        elif 'bytes' in cmeta:
            have = os.stat(dst).st_size
            want = cmeta['bytes']
            if have == want:
                # print 'file sizes match - good', cmeta['name'], want, have
                return True
            else:
                print '** exists but size is wrong', cmeta['name'], want, have
                return False
        else:
            print 'exists and might be OK', cmeta['name']
            return True


def audit_capture(cmeta, repo, prefix, local, files_prefix):

    if quick_audit_capture(cmeta, repo):
        # OK, nothing to be done.
        return []

    # Need to copy it from one of the source locations

    # Can move the file there from a different local location?
    def local_path(loc):
        path = None
        if not ':' in loc:
            # elsewhere in repo
            return os.path.join(repo, loc)
        elif loc.startswith(prefix):
            # somewhere else on this machine
            return os.path.join(local, loc[len(prefix):])
        else:
            # on the interwebs
            return None

    def location_sort_order(loc):
        if '://' in loc:
            return 2            # access using curl
        elif ':' in loc:
            if loc.startswith(local):
                return 1        # local file
            else:
                return 3        # access using ssh
        else:
            return 0            # local file name

    if "locations" in cmeta:
        locations = sorted(cmeta["locations"], key=location_sort_order)
    else:
        locations = []

    dst = full_path(cmeta, repo)

    # Returns commands or None
    def try_location(loc):

        tbd = None

        # Local file cases
        lpath = local_path(loc)
        if lpath != None:
            if lpath == dst:
                # success (should have been caught earlier)
                print "** source and target are the same:", lpath

            elif not os.path.exists(lpath):
                if os.path.lexists(lpath):
                    print "** broken link:", lpath
                else:
                    print "** no such local file or directory:", lpath

            elif os.path.islink(lpath):
                print '** location is a link to who knows what ... copy it?', lpath

            elif os.path.isdir(lpath):
                if lpath.endswith('/') and dst.endswith('.tgz'):
                    dir = lpath[0:-1]
                    # create tarball, contents = files in path
                    tbd = "./make-tarball %s %s" % (dir, dst)
                else:
                    print '** source file is a directory, but no /, so not making tarball', lpath

            else:
                # move file and leave symbolic link behind.
                if not dst.startswith('/'):
                    print '** relative links probably will not work!', dst
                tbd = "mv %s %s && ln -sf %s %s" % (lpath, dst, dst, lpath)

        else:
            # Remote via ssh or http
            if '://' in loc:
                # print 'curl -s -I "%s" | (if ! grep -q 200.OK; then echo Not found: "%s"; fi)' % (loc, loc)
                tbd = 'curl -s "%s" >%s.tmp && mv %s.tmp %s' % (loc, dst, dst, dst)
            elif ':' in loc:
                # print "if ! ssh %s test -r %s; then echo Not found: %s; fi" % (loc[0:i], loc[i+1:], loc)
                if prefix != 'files:':
                    tbd = "scp -p %s %s" % (loc, dst)
                else:
                    tbd = 'echo on-question-scp %s files:files.opentreeoflife.org/%s' % (otpath, otpath)
        return tbd

    # Only one command per capture.  Throw away all but the first.
    command = None
    for loc in locations:
        maybe_command = try_location(loc)
        if maybe_command != None and command == None:
            command = maybe_command

    if command == None:
        print '** no candidate locations for this capture', cmeta['name']
        return []

    commands = []

    dir = os.path.dirname(dst)
    if not os.path.isdir(dir):
        commands.append("mkdir -p %s" % dir)

    commands.append(command)
    return commands


args = p.parse_args()

if __name__ == '__main__':
    audit(registry.Registry(), args.repo, args.prefix, args.base, args.files)
