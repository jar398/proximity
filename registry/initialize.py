
# 1. Read 'artifacts'

# 2. Register all of them

import process_seed, registry

events = process_seed.load_seed_events()

registry = registry.Registry()

registry.wipe()

for event in events:
    type = event['_type']
    if type == 'resource':
        registry.register_resource(event)
    elif type == 'capture':
        registry.register_capture(event)
    # maybe taxa too?
    else:
        print '** unrecognized event type', type

# when done, eyeball the registry files, for fun.

