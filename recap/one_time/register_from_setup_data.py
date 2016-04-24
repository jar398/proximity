# One time resource/capture registry setup

# 1. Read commands (generated from setup data)
# 2. Do registrations per type (resource or capture)

import sys

import process_seed, recap.registry

do_ids = False

def register_from_setup_data(setup_file, re, cap):

    commands = process_seed.get_setup_commands(setup_file)

    if do_ids:
        registry = recap.registry.Registry(re, cap, 'unused_ids.csv')
    else:
        registry = recap.registry.Registry(re, cap)
    registry.wipe()

    for command in commands:
        type = command['_type']
        if type == 'resource':
            registry.register_resource(command)
        elif type == 'capture':
            registry.register_capture(command)
        # maybe taxa too?
        else:
            print '** unrecognized command type', type

# when done, eyeball the registry files, for fun.

if __name__ == '__main__':
    register_from_setup_data(sys.argv[1], sys.argv[2], sys.argv[3])
