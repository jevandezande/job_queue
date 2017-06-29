from socket import gethostname
import argparse


class SQParser(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        if 'description' not in kwargs:
            kwargs['description'] = 'Submit a job to run.'
        super().__init__(*args, **kwargs)

        self.add_argument('-i', '--input', help='The input file to run.', type=str,
                          default='input.dat')
        self.add_argument('-o', '--output', help='Where to put the output.',
                          type=str, default='{autoselect}')
        self.add_argument('-q', '--queue', help='What queue to use.',
                          type=str, default='small')
        self.add_argument('-N', '--name', help='The name of the job.', type=str,
                          default='{autoselect}')
        self.add_argument('-n', '--nodes', help='The number of nodes to be used.',
                          type=int, default=1)
        self.add_argument('-d', '--debug', help='Generate but don\'t submit .sh script.',
                            action='store_true', default=False)


def setup_cluster(args):
    host = gethostname()
    if host in ['zeusln1', 'zeusln2']:
        return Zeus(args)
    raise Exception(f'Cluster {host} not currently supported.')

class Cluster:
    def __init__(self, args):
        self.queue = args.queue

class Zeus(Cluster):
    def __init__(self, args):
        super().__init__(args)     
        if args.queue not in ['small', 'batch']:
            raise Exception('Invalid queue')

