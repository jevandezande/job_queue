from socket import gethostname
import getpass
import os
import argparse
import re
import subprocess
import math


class SubmitJob:
    def __init__(self, options=None):
        self.supported_programs = ['orca', 'orca_old', 'nbo']
        if options is not None:
            self.parse_options(options)
        else:
            self.parse_args()
        self.cwd = os.getcwd()
        self.get_host()
        self.get_user()
        self.check_queue()
        self.select_input()
        self.select_output()
        self.name_job()
        self.select_resources()

    def parse_options(self, options):
        """
        Parse the options passed in, currently trusts all options
        """
        default_options = {
            'program': 'orca',
            'input': 'input.dat',
            'output': '{autoselect}',
            'queue': 'small',
            'nodes': 1,
            'name': '{autoselect}',
            'debug': False,
            'job_array': False,
        }

        if any(k not in default_options for k in options):
            raise Exception(f'Unsupported options passed to SubmitJob: {options}')

        self.__dict__.update(default_options)
        self.__dict__.update(options)

    def parse_args(self):
        """
        Parse the command line arguments
        """
        parser = argparse.ArgumentParser(description='Get the geometry from an output file.')
        parser.add_argument('-p', '--program', help='The program to run.', type=str,
                            default='orca', choices=self.supported_programs)
        parser.add_argument('-i', '--input', help='The input file to run.', type=str,
                            default='input.dat')
        parser.add_argument('-o', '--output', help='Where to put the output.',
                            type=str, default='{autoselect}')
        parser.add_argument('-q', '--queue', help='What queue to use.',
                            type=str, default='small')
        parser.add_argument('-n', '--nodes', help='The number of nodes to be used.',
                            type=int, default=1)
        parser.add_argument('-N', '--name', help='The name of the job.', type=str,
                            default='{autoselect}')
        parser.add_argument('-d', '--debug', help='Generate but don\'t submit .sh script.',
                            action='store_true', default=False)
        parser.add_argument('-a', '--job_array', help='Submit as a job array',
                            type=int, default=False)
        self.__dict__.update(parser.parse_args().__dict__)

    def parse_config(self):
        """
        Parse the config file
        """
        pass

    def get_host(self):
        """
        Determine where this is running
        """
        hostname = gethostname()
        if hostname in ['zeusln1', 'zeusln2']:
            self.host = 'zeus'
        elif hostname in ['master1', 'master2']:
            self.host = 'hera'
        else:
            raise AttributeError(f'No suitable host found for hostname {hostname}')

        return self.host

    def get_user(self):
        """
        Get the user
        """
        self.user = getpass.getuser()

    def check_queue(self):
        """
        Check to make sure the queue is invalid
        """
        queues = {
            'zeus': ['small', 'batch'],
            'hera': ['small', 'batch'],
        }
        if self.queue in queues[self.host]:
            return True
        else:
            raise AttributeError(f'No queue named {self.queue} on {self.host}')

    def select_input(self):
        """
        Select the appropriate input file
        """
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
            self.output = 'output.dat' if self.input == 'input.dat' else self.input_root + '.out'
        # make full path
        self.output = f'$PBS_O_WORKDIR/$PBS_ARRAYID/{self.output}'

    def name_job(self):
        """
        If not defined, name the job after the path to it (up to 20 chars long)
        """
        if self.name == '{autoselect}':
            try:
                path = self.cwd[-20:]
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

        if self.program in ['orca', 'orca_old'] and not self.job_array:
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

    def submit(self):
        """
        Generate and submit the subfile
        """
        qsubopt = ''
        error_file = 'error'
        job_array_str = f'#PBS -t 0-{self.job_array-1}' if self.job_array else ''
        trap = f"""trap '
echo "Job terminated from outer space!" >> {self.output}
cleanup
echo "${{PBS_JOBID:r}}: {self.name} - $PBS_O_WORKDIR" >> $HOME/.failed_jobs
exit
' TERM
"""

        sub_file = f"""#!/bin/zsh
#PBS -S /bin/zsh
#PBS -l nodes={self.nodes}:ppn={self.ppn}
#PBS -l mem={self.memory}GB
#PBS -l walltime=8760:00:00
#PBS -q {self.queue}
#PBS -j oe
#PBS -e {error_file}
#PBS -N {self.name}
{job_array_str}

if [ -z $PBS_ARRAYID ] || [ $PBS_ARRAYID = 0 ]
then
    echo "${{PBS_JOBID:r}}: {self.name} - $PBS_O_WORKDIR" >> $HOME/.jobs
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
        if self.program in ['orca', 'orca_old']:
            orca_path = '/opt/orca' if self.program == 'orca_old' else '/opt/orca_current'
            mpi_path = '/opt/openmpi_1.10.2/bin'
            mpi_lib = '/opt/openmpi_1.10.2/lib'

            # TODO: remove hardcoded options
            moinp_files_array = ''
            xyz_files_array = ''
            self.nodes = 1
            sub_file += f"""
export PATH={mpi_path}:{orca_path}:$PBS_O_PATH

export LD_LIBRARY_PATH=$tdir/orca:{mpi_lib}:/opt/intel/mkl/lib/intel64:/opt/intel/lib/intel64:$LD_LIBRARY_PATH

for node in $nodes;
{{
    ssh $node "mkdir -p $tdir && cp -r {orca_path} $tdir/orca"
}}

{trap}

# Setup for helper applications...
# For NBO 6.0:
export NBOEXE=$tdir/orca/nbo6.exe
export GENEXE=$tdir/orca/gennbo.exe

export PATH=$tdir/orca:{mpi_path}:$PATH

# Function to delete unnecessary files
cleanup () {{
    # Copy the important stuff
    rm *.proc* 2> /dev/null
    mkdir data/
    cp ^(*.(tmp*|out|inp|hostnames)) data/
    tar cvzf $PBS_O_WORKDIR/$PBS_ARRAYID/data.tgz data/

    # Delete everything in the temporary directory
    for node in $nodes; {{ ssh $node "rm -rf $tdir" }}
}}


cp $PBS_O_WORKDIR/$PBS_ARRAYID/{self.input_root}.* $tdir/

cd $PBS_O_WORKDIR/$PBS_ARRAYID/
for file in {moinp_files_array} {xyz_files_array} *.gbw *.pc *.opt *.hess *.rrhess *.bas *.pot *.rno *.LJ *.LJ.Excl;
{{
    cp -v $file $tdir/ >>& {self.output}
}}

cd $tdir

echo "Start: $(date)
Job running on $PBS_O_HOST, running $(which orca) copied from {orca_path} on $(hostname) in $tdir
Shared library path: $LD_LIBRARY_PATH
PBS Job ID $PBS_JOBID is running on $(echo $a | wc -l) nodes:" >> {self.output}
echo $nodes | tr "\\n" ", " |  sed "s|,$|\\n|" >> {self.output}

# = calls full path in zsh
=orca {self.input} >>& {self.output}

cleanup
"""
        elif self.program == 'nbo':
            sub_file += f"""
{trap}

# For NBO 6.0:
export NBOEXE=$tdir/orca/nbo6.exe
export GENEXE=$tdir/orca/gennbo.exe

cp $PBS_O_WORKDIR/$PBS_ARRAYID/{self.input_root}.* $tdir/
cd $tdir
echo "Start: $(date)
Job running on $PBS_O_HOST, running $(which gennbo.exe) on $(hostname) in $tdir
PBS Job ID $PBS_JOBID is running on $(echo $a | wc -l) nodes:" >> {self.output}
echo $nodes | tr "\\n" ", " |  sed "s|,$|\\n|" >> {self.output}

gennbo.exe < {self.input} > {self.output}
"""
        else:
            raise AttributeError(f'Only {self.supported_programs} currently supported.')

        sub_file += f"""
if [ -z $PBS_ARRAYID ] || [ $PBS_ARRAYID = 0 ]
then
    echo "${{PBS_JOBID:r}}: {self.name} - $PBS_O_WORKDIR" >> $HOME/.completed_jobs
fi"""

        with open(f'{self.input_root}.zsh', 'w') as f:
            f.write(sub_file)

        if not self.debug:
            subprocess.check_call(f'qsub {qsubopt} {self.input_root}.zsh', shell=True)
