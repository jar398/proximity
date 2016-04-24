#  $^ = all prerequisites
#  $< = first prerequisite
#  $@ = file name of target

all: var/recap/initial_resources.json var/recap/initial_captures.json

# Turn the setup data into initial registry state using the same
# library that will be used to register additional resources and
# captures.

var/recap/initial_resources.json var/recap/initial_captures.json: recap/one_time/one_time_setup_data.json \
            recap/one_time/register_from_setup_data.py \
            recap/one_time/process_seed.py \
            recap/registry.py
	PYTHONPATH=. python recap/one_time/register_from_setup_data.py $< \
	  var/recap/initial_resources.json var/recap/initial_captures.json

audit:
	python recap/audit.py '/Users/jar/otrepo/files.opentreeoflife.org' '/Users/jar' 'question:' \
	   'http://files.opentreeoflife.org/' \
	   'var/recap/initial_resources.json' \
	   'var/recap/initial_captures.json'
