#!/usr/bin/env python3

import argparse
import os
import re
from subprocess import check_call

parser = argparse.ArgumentParser(description='Submit a job to run.')
parser.add_argument('-i', '--input', help='The input file to run.', type=str,
                    default='input.dat')
parser.add_argument('-o', '--output', help='Where to put the output.',
                    type=str, default='{autoselect}')
parser.add_argument('-q', '--queue', help='What queue to use.',
                    type=str, default='small')
parser.add_argument('-n', '--name', help='The name of the job.', type=str,
                    default='{autoselect}')
parser.add_argument('-N', '--nodes', help='The number of nodes to be used.',
                    type=int, default=1)
parser.add_argument('-O', '--old', help="Request old version.",
                    action='store_true', default=False)
parser.add_argument('-d', '--debug', help="Generate but don't submit .sh script.",
                    action='store_true', default=False)


args = parser.parse_args()

cwd = os.getcwd()
orca_path = '/opt/orca' if args.old else '/opt/orca_current'
mpi_path = '/opt/openmpi_1.10.2/bin'
mpi_lib = '/opt/openmpi_1.10.2/lib'

queue = args.queue
if queue not in ['small', 'batch']:
    raise Exception('Invalid queue')

inp = args.input
if not os.path.exists(inp):
    raise Exception("Unable to find input file")
inp_root = '.'.join(inp.split('.')[:-1])

output = args.output
if output == '{autoselect}':
    output = 'output.dat' if inp == 'input.dat' else inp_root + '.out'
output = f'{cwd}/{output}'

# If not defined, name the job after the directory
name = cwd.split('/')[-1] if args.name == '{autoselect}' else args.name
# Job name can't start with a number
if name.isdigit():
    name = 'J' + name

# Set the number of processors to that in input file, else 1
nodes = args.nodes
nprocs_re = r'%\s*pal\n?\s*nprocs\s+(\d+)\n?\s*end'
with open(inp) as f:
    res = re.search(nprocs_re, f.read())
nprocs = int(res.group(1)) if res else 1
if nprocs % nodes:
    raise Exception(f'Cannot divide {nprocs} processes evenly between {nodes} nodes.')
ppn = int(nprocs/nodes)


# TODO: remove hardcode
error_file = 'error'
qsubopt = ''
user = 'vandezande'
moinp_files_array = ''
xyz_files_array = ''


sub_file = f"""#!/bin/zsh
#PBS -S /bin/zsh
#PBS -l nodes={nodes}:ppn={ppn}
#PBS -l walltime=8760:00:00
#PBS -q {queue}
#PBS -j oe
#PBS -e {error_file}
#PBS -N {name}
#PBS -r n


setopt EXTENDED_GLOB
setopt NULL_GLOB
export MKL_NUM_THREADS=1
export OMP_NUM_THREADS=1
export OMPI_MCA_btl_tcp_if_include=192.168.2.0/24

ulimit -u 8191


export PATH={mpi_path}:{orca_path}:$PBS_O_PATH

export LD_LIBRARY_PATH={mpi_lib}:/opt/intel/mkl/lib/intel64:/opt/intel/lib/intel64:$LD_LIBRARY_PATH
export RSH_COMMAND="/usr/bin/ssh -x"


if [[ ! -d /scratch/{user} ]] then
  mkdir -p /scratch/{user}
fi

tdir=$(mktemp -d /scratch/{user}/{inp_root}__XXXXXX)
export LD_LIBRARY_PATH=$tdir/orca:$LD_LIBRARY_PATH

foreach node ($(sort -u $PBS_NODEFILE)) {{ ssh $node "mkdir -p $tdir" }}
foreach node ($(sort -u $PBS_NODEFILE)) {{ ssh $node "cp -r {orca_path} $tdir/orca" }}

# Setup for helper applications...
# For NBO 6.0:
export NBOEXE=$tdir/orca/nbo6.exe
export GENEXE=$tdir/orca/gennbo.exe

export PATH=$tdir/orca:{mpi_path}:$PATH

trap '
echo "Job terminated from outer space!" >> {output}
# Delete the ORCA executable
foreach node ($(sort -u $PBS_NODEFILE)) {{ ssh $node "rm -rf $tdir/orca" }}

# Copy the important stuff
cp -v ^(*.(tmp*|out|inp))  $PBS_O_WORKDIR/ >>& {output}
cp -v {inp_root}.asa.inp  $PBS_O_WORKDIR/ >>& {output}

# Delete everything in the temporary directory
foreach node ($(sort -u $PBS_NODEFILE)) {{ ssh $node "rm -rf $tdir" }}
exit
' TERM


cp $PBS_O_WORKDIR/{inp_root}.* $tdir/

cd $PBS_O_WORKDIR
foreach file ({moinp_files_array} {xyz_files_array} *.pc *.opt *.hess *.rrhess *.bas *.pot *.rno *.LJ *.LJ.Excl)
  cp -v $file $tdir/ >>& {output}
end

cd $tdir

echo "! Job execution by suborca.py on ZEUS" > {output}
echo "Job started from $PBS_O_HOST, running $(which orca) copied from {orca_path} on $(hostname) in $tdir" >> {output}
echo "Job execution start: $(date)" >> {output}
echo "Shared library path: $LD_LIBRARY_PATH" >> {output}
echo "PBS Job ID is: $PBS_JOBID" >> {output}
cat $PBS_NODEFILE >> {output}
#echo "Going to copy the following really necessary files: {moinp_files_array} {xyz_files_array}" >> {output}
# = calls full path in zsh
=orca {inp} >>& {output}

# Delete the ORCA executable
foreach node ($(sort -u $PBS_NODEFILE)) {{ ssh $node "rm -rf $tdir/orca" }}

# Copy the import stuff
cp -v ^(*.(tmp*|out|inp)) $PBS_O_WORKDIR/ >>& {output}
cp -v {inp_root}.asa.inp $PBS_O_WORKDIR/ >>& {output}

# Delete everything in the temporary directory
foreach node ($(sort -u $PBS_NODEFILE)) {{ ssh $node "rm -rf $tdir" }}
"""


with open(f'{inp_root}.zsh', 'w') as f:
    f.write(sub_file)

if not args.debug:
    check_call(f'qsub {qsubopt} {inp_root}.zsh', shell=True)
