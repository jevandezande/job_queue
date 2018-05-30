import os
import re
import math
import getpass
import argparse
import subprocess

from socket import gethostname
from collections import OrderedDict

from configparser import ConfigParser


SUPPORTED_PROGRAMS = ['cfour', 'cfour_v2', 'nbo', 'orca3', 'orca', 'orca_current', 'psi4']


class SubmitJob:
    def __init__(self, options=None):
        self.parse_config()
        self.set_defaults()

        if options is None:
            options = SubmitJob.parse_args(self.default_options)
        self.parse_options(options)

        self.cwd = os.getcwd()
        self.get_host()
        self.get_user()
        self.check_queue()
        self.select_input()
        self.select_output()
        self.name_job()
        self.select_resources()
        self.select_important_files()

    def parse_config(self):
        """
        Parse the config file
        """
        # Add defaults from config file
        self.config = ConfigParser()
        config_file = os.path.join(os.path.expanduser("~"), '.config', 'job_queue', 'config')
        if os.path.exists(config_file):
            self.config.read(config_file)

    def set_defaults(self):
        """
        Add defaults from the config file
        """
        self.default_options = {
            'program': '',
            'input': '{autoselect}',
            'output': '{autoselect}',
            'queue': '',
            'nodes': 1,
            'name': '{autoselect}',
            'debug': False,
            'job_array': False,
            'walltime': 8760,  # 365 days
            'email': False,
            'email_address': None,
            'hold': False,
            'name_length': 20,
        }
        if 'queues' in self.config:
            self.default_options['name_length'] = self.config['queues'].getint('name_length', 20)

        if 'submitjob' in self.config:
            config_defaults = self.config['submitjob']

            # Avoid adding extra options, otherwise it could accidentally
            # overwrite pre-existing functions or variables!
            extra_options = set(config_defaults) - set(self.default_options)
            if extra_options:
                raise ValueError(f'Invalid option(s) in config file: {extra_options}')

            self.default_options.update(config_defaults)

    @staticmethod
    def parse_args(default_options):
        """
        Parse the command line arguments.
        """
        options_str = '\n'.join(f'{k}: {v}' for k, v in default_options.items())
        parser = argparse.ArgumentParser(description=f"""
Submit jobs to a queue.
Default Options (as configured by .config/job_queue/config)
{options_str}
        """, formatter_class=argparse.RawTextHelpFormatter)
        parser.add_argument('-p', '--program', help='The program to run.',
                            type=str, default=default_options['program'],
                            choices=SUPPORTED_PROGRAMS)
        parser.add_argument('-i', '--input', help='The input file to run.',
                            type=str, default=default_options['input'])
        parser.add_argument('-o', '--output', help='Where to put the output.',
                            type=str, default=default_options['output'])
        parser.add_argument('-q', '--queue', help='What queue to use.',
                            type=str, default=default_options['queue'])
        parser.add_argument('-n', '--nodes', help='The number of nodes to be used.',
                            type=int, default=1)
        parser.add_argument('-N', '--name', help='The name of the job.',
                            type=str, default=default_options['name'])
        parser.add_argument('-d', '--debug', help='Generate but don\'t submit .sh script.',
                            action='store_true', default=False)
        parser.add_argument('-a', '--job_array', help='Submit as a job array',
                            type=int, default=False)
        parser.add_argument('-t', '--walltime', help='Max walltime of job',
                            type=int, default=8760)
        parser.add_argument('-m', '--email', help='Send email upon completion.',
                            action='store_true', default=default_options['email'])
        parser.add_argument('-M', '--email_address', help='Email address to send to.',
                            type=str, default=default_options['email_address'])
        parser.add_argument('--hold', help='submit the job in a held status',
                            action='store_true', default=default_options['hold'])

        return parser.parse_args().__dict__

    def parse_options(self, options):
        """
        Parse the options passed in.
        Currently trusts all option settings, but does not allow setting of unknown options.
        """

        extra_options = set(options) - set(self.default_options)
        if extra_options:
            raise Exception(f'Unsupported options passed to SubmitJob: {extra_options}')

        my_options = self.default_options
        my_options.update(options)

        # Convert options from strings to the correct type
        for option, value in my_options.items():
            if isinstance(value, str):
                if value == 'True':
                    my_options[option] = True
                elif value == 'False':
                    my_options[option] = False
                elif value.isdigit():
                    my_options[option] = int(value)
                else:
                    try:
                        my_options[option] = float(value)
                    except ValueError:
                        pass

        self.check_options(my_options)

        self.__dict__.update(my_options)

        self.pbs_options = OrderedDict()
        if self.email and self.email != 'False':
            if self.email_address is None:
                raise ValueError('No email address specified.')
            self.pbs_options['-m'] = 'e'
            self.pbs_options['-M'] = self.email_address

    def check_options(self, options):
        """
        Checks that options are valid.
        WARNING: this is not an exhaustive check, instead it is merely a sanity
        check for a few options
        TODO: Implement
        """
        pass

    def get_host(self):
        """
        Determine where this is running
        """
        self.host = None
        hostname = gethostname()
        if hostname in ['zeusln1', 'zeusln2']:
            self.host = 'zeus'
        elif hostname in ['master1', 'master2']:
            self.host = 'hera'
        elif hostname in ['icmaster1.mpi-bac', 'icmaster2.mpi-bac']:
            self.host = 'hermes'
        elif self.debug:
            print(f'No suitable host found for hostname {hostname}')

    def get_user(self):
        """
        Get the user
        """
        self.user = getpass.getuser()

    def check_queue(self):
        """
        Check to make sure the queue is valid
        """
        queues = {
            'zeus': ['small', 'batch'],
            'hera': ['small', 'batch'],
            'hermes': ['small', 'batch'],
        }
        if self.host is None or self.queue in queues[self.host]:
            return True
        else:
            raise AttributeError(f'No queue named {self.queue} on {self.host}')

    def select_input(self):
        """
        Select the appropriate input file
        """
        # Select the input file name if not specified
        if self.input == '{autoselect}':
            if self.program[:5] == 'cfour':
                self.input = 'ZMAT'
            elif self.program == 'gamess':
                self.input = 'input.inp'
            else:
                self.input = 'input.dat'

        if self.job_array:
            for i in range(self.job_array):
                if not os.path.isfile(f'{i}/{self.input}'):
                    print(f'{i}/{self.input}')
                    print(os.getcwd())
                    raise Exception(f'Unable to find job_array input file, {i}.')
        elif not os.path.exists(self.input):
            raise Exception('Unable to find input file')
        self.input_root = '.'.join(self.input.split('.')[:-1])

    def select_output(self):
        """
        Select the appropriate output file
        """
        if self.output == '{autoselect}':
            if self.input in ['input.dat', 'ZMAT']:
                self.output = 'output.dat'
            else:
                self.input_root + '.out'
        # make full path
        self.output = f'$PBS_O_WORKDIR/$PBS_ARRAYID/{self.output}'

    def name_job(self):
        """
        If not defined, name the job after the path to it (up to 20 chars long)
        """
        if self.name == '{autoselect}':
            try:
                path = self.cwd[-self.name_length:]
                self.name = path[path.index('/') + 1:]
            except (ValueError, IndexError) as e:
                dirs = self.cwd.split('/')
                self.name = dirs[-1]

    def select_resources(self):
        """
        Set the number of processors to that in input file, else 1
        TODO: integrate with number of nodes flag
        Set the memory
        """
        self.nprocs = 1
        self.memory = 10  # GB

        if 'orca' in self.program and not self.job_array:
            nprocs_re = r'%\s*pal\n?\s*nprocs\s+(\d+)\n?\s*end'
            maxcore_re = r'%\s*maxcore\s*(\d+)'
            with open(self.input) as f:
                inp_file = f.read()
            nprocs = re.search(nprocs_re, inp_file)
            maxcore = re.search(maxcore_re, inp_file)
            self.nprocs = int(nprocs.group(1)) if nprocs else 1
            core_memory = int(maxcore.group(1))/1024 if maxcore else 1
            self.memory = math.ceil(core_memory * self.nprocs)

        if self.nprocs % self.nodes:
            raise Exception(f'Cannot divide {self.nprocs} processes evenly between {self.nodes} nodes.')
        self.ppn = int(self.nprocs/self.nodes)

    def select_important_files(self):
        """
        Selects the files to be copied back after a job is run.
        Utilizes ZSH syntax (literally pasted into a for loop)
        """
        self.important_files = ''
        if 'orca' in self.program:
            self.important_files = '^(*.(tmp*|out|inp|hostnames))'
        elif self.program[:5] == 'cfour':
            self.important_files = 'ZMATnew FCMINT FCM ANH'
        else:
            print("Don't know what files to copy back")

    def submit(self):
        """
        Generate and submit the subfile
        """
        qsubopt = ''
        error_file = 'error'

        pbs_options_str = f'#PBS -t 0-{self.job_array-1}' if self.job_array else ''
        pbs_options_str += '\n'.join(f'#PBS {flag} {value}' for flag, value in self.pbs_options.items())

        cleanup_func = f'''
# Function to delete unnecessary files
cleanup () {{
    # Copy the important stuff
    rm *.proc* 2> /dev/null
    mkdir data/
    for file in {self.important_files};
    {{
        cp $file data/
    }}
    tar cvzf $PBS_O_WORKDIR/$PBS_ARRAYID/data.tgz data/

    # Delete everything in the temporary directory
    for node in $nodes; {{ ssh $node "rm -rf $tdir" }}
}}
'''
        trap = '''
# Calls cleanup if the world falls apart
trap '
"Job terminated from outer space!" >> {self.output}
cleanup
echo "$PBS_JOBID_int: {self.name} - $PBS_O_WORKDIR" >> $HOME/.config/job_queue/failed
exit
' TERM
'''
        program_header = f'''\
##{"#"*len(self.program)}##
# {self.program.upper()} #
##{'#'*len(self.program)}##'''

        sub_file = f"""#!/bin/zsh
#PBS -S /bin/zsh
#PBS -l nodes={self.nodes}:ppn={self.ppn}
#PBS -l mem={self.memory}GB
#PBS -l walltime={self.walltime}:00:00
#PBS -q {self.queue}
#PBS -j oe
#PBS -e {error_file}
#PBS -N {self.name}
{pbs_options_str}

# Only the number part
PBS_JOBID_int=$(echo $PBS_JOBID | cut -d '.' -f 1)

{program_header}

if [ -z $PBS_ARRAYID ] || [ $PBS_ARRAYID = 0 ]
then
    echo "$PBS_JOBID_int: {self.name} - $PBS_O_WORKDIR" >> $HOME/.config/job_queue/submitted
fi
setopt EXTENDED_GLOB
setopt NULL_GLOB
export MKL_NUM_THREADS=1
export OMP_NUM_THREADS=1
export OMPI_MCA_btl_tcp_if_include=192.168.2.0/24
export RSH_COMMAND="/usr/bin/ssh -x"

ulimit -u 8191

# Move old output file to output.#
# Done here as the generation script is not always called
if [ -f {self.output} ]
then
    for i in {{1..1000}};
    {{
        if [ ! -f {self.output}.$i ]
        then
            mv {self.output} {self.output}.$i
            break
        fi
    }}
fi

mkdir -p /scratch/{self.user}
tdir=$(mktemp -d /scratch/{self.user}/{self.input_root}__XXXXXX)

nodes=$(sort -u $PBS_NODEFILE)
"""
        if 'orca' in self.program:
            orca_paths = {
                'orca':         '/opt/orca',
                'orca_current': '/opt/orca_current',
                'orca3':        '/home1/vandezande/progs/orca_3',
            }
            orca_path = orca_paths[self.program]
            mpi_path = '/opt/openmpi_1.10.2/bin'
            mpi_lib = '/opt/openmpi_1.10.2/lib'

            # TODO: remove hardcoded options
            moinp_files_array = ''
            xyz_files_array = ''
            self.nodes = 1
            sub_file += f"""

export PATH=$tdir/orca:{mpi_path}:{orca_path}:$PBS_O_PATH

export LD_LIBRARY_PATH=$tdir/orca:{mpi_lib}:/opt/intel/mkl/lib/intel64:/opt/intel/lib/intel64:$LD_LIBRARY_PATH

for node in $nodes;
{{
    ssh $node "mkdir -p $tdir && cp -r {orca_path} $tdir/orca"
}}

# Setup for helper applications...
# For NBO 6.0:
export NBOEXE=$tdir/orca/nbo6.exe
export GENEXE=$tdir/orca/gennbo.exe

cp $PBS_O_WORKDIR/$PBS_ARRAYID/{self.input_root}.* $tdir

cd $PBS_O_WORKDIR/$PBS_ARRAYID/
for file in {moinp_files_array} {xyz_files_array} *.gbw *.pc *.opt *.hess *.rrhess *.bas *.pot *.rno *.LJ *.LJ.Excl;
{{
    cp -v $file $tdir >>& {self.output}
}}

cd $tdir
{cleanup_func}
{trap}

echo "Start: $(date)
Job running on $PBS_O_HOST, running $(which orca) copied from {orca_path} on $(hostname) in $tdir
Shared library path: $LD_LIBRARY_PATH
PBS Job ID $PBS_JOBID_int is running on $(echo $a | wc -l) nodes:" >> {self.output}
echo $nodes | tr "\\n" ", " |  sed "s|,$|\\n|" >> {self.output}

# = calls full path in zsh
=orca {self.input} >>& {self.output}
"""

        elif self.program == 'nbo':
            sub_file += f"""
# For NBO 6.0:
export NBOEXE=$tdir/orca/nbo6.exe
export GENEXE=$tdir/orca/gennbo.exe

cp $PBS_O_WORKDIR/$PBS_ARRAYID/{self.input_root}.* $tdir/
cd $tdir
echo "Start: $(date)
Job running on $PBS_O_HOST, running $(which gennbo.exe) on $(hostname) in $tdir
PBS Job ID $PBS_JOBID_int is running on $(echo $a | wc -l) nodes:" >> {self.output}
echo $nodes | tr "\\n" ", " |  sed "s|,$|\\n|" >> {self.output}

gennbo.exe < {self.input} > {self.output}
"""

        elif self.program[:5] == 'cfour':
            cfour_path = f'/home1/vandezande/.install/{self.program}'
            inp_files = ''
            start_dir = '$PBS_O_WORKDIR/$PBS_ARRAYID/'
            sub_file += f"""
##################
# CFour specific #
##################
PATH=$PATH:{cfour_path}/bin

# Copy everything
cd {start_dir}
if [ ! -e GENBAS ]
then
    echo "Using default GENBAS" >> {self.output}
    cp {cfour_path}/basis/GENBAS $tdir
else
    echo "Using GENBAS in directory" >> {self.output}
    cp GENBAS $tdir
fi

for file in ZMAT FCM FCMINT {inp_files}
{{
    cp -v $file $tdir >>& {self.output}
}}

cd $tdir
{cleanup_func}
{trap}

###############
# Run the job #
###############

echo "Start: $(date)
Job running on $PBS_O_HOST, running $(which xcfour) on $(hostname) in $tdir
PBS Job ID $PBS_JOBID_int is running on $(echo $a | wc -l) nodes:" >> {self.output}
echo $nodes | tr "\\n" ", " |  sed "s|,$|\\n|" >> {self.output}

xcfour {self.input} >>& {self.output}

echo "Finished"
"""
        elif self.program == 'psi4':
            psi4 = '/home1/vandezande/.bin/psi4'
            inp_files = ''
            start_dir = '$PBS_O_WORKDIR/$PBS_ARRAYID/'
            sub_file += f"""
#################
# Psi4 specific #
#################
# Copy everything

cp -v {self.input} $tdir >>& blabla
echo $tdir
echo {self.input}

cd $tdir
pwd
{cleanup_func}
{trap}

###############
# Run the job #
###############

echo "Start: $(date)
Job running on $PBS_O_HOST, running $(which {psi4}) on $(hostname) in $tdir
PBS Job ID $PBS_JOBID is running on $(echo $a | wc -l) nodes:" >> {self.output}
echo $nodes | tr "\\n" ", " |  sed "s|,$|\\n|" >> {self.output}

{psi4} -i {self.input} -o {self.output}

echo "Finished"
"""
        else:
            raise AttributeError(f'Only {SUPPORTED_PROGRAMS} currently supported.')

        sub_file += f"""
###########
# Cleanup #
###########
cleanup

# At job to log of completed jobs
if [ -z $PBS_ARRAYID ] || [ $PBS_ARRAYID = 0 ]
then
    echo "$PBS_JOBID_int: {self.name} - $PBS_O_WORKDIR" >> $HOME/.config/job_queue/completed
fi"""

        self.sub_script_name = 'job.zsh'
        with open(self.sub_script_name, 'w') as f:
            f.write(sub_file)

        if not self.debug:
            subprocess.check_call(f'qsub {qsubopt} {self.sub_script_name}', shell=True)
