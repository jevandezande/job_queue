import getpass

from .queues import Queues
from subprocess import check_call
from os.path import expanduser
from os import environ


"""
Collection of random commands that are needed in random places
"""


def grid_engine():
    """
    Determine the grid engine
    """
    engines = {
        "PBS": "PBS_ROOT",
        "SGE": "SGE_ROOT",
    }
    for prog, variable in engines.items():
        if variable in environ:
            return prog
    return None


def hold_job(*job_ids):
    """
    Tell grid engine to hold specified job numbers
    :param job_ids: the id of the jobs to hold
    Warning: if job does not exist or cannot be held, no notice is given
    """
    for id in job_ids:
        check_call(f'qhold {id}', shell=True)


def release_job(*job_ids):
    """
    Tell grid engine to release specified job numbers
    :param job_ids: the id of the jobs to release
    Warning: if job does not exist or cannot be released, no notice is given
    """
    for id in job_ids:
        check_call(f'qrls {id}', shell=True)


def hold_jobs(jobs='queueing', queues='all'):
    """
    Hold defined jobs (safer wrapper to hold_job)
    :param jobs: 'queueing' or list corresponding to jobs to hold
    :param queues: queues to target

    Prints a warning if job does not exist
    """
    ge_queues = Queues()

    # Find all jobs that match
    if jobs == 'queueing':
        jobs = []
        if queues == 'all':
            jobs = ge_queues.queueing.keys()
        else:
            for queue in queues:
                jobs += ge_queues.queues[queue].queueing.keys()

        # Only user's jobs (they can't mess with the jobs of others)
        jobs = filter_user_job_ids(jobs)

    # check to see if job exists
    found_jobs = [job for job in jobs if job in ge_queues.jobs]
    missing = sorted(set(jobs) - set(found_jobs))
    if missing:
        print(f'Missing jobs: {", ".join(missing)}')

    print(found_jobs)

    hold_job(*found_jobs)


def release_jobs(jobs='holding', queues='all'):
    """
    Release defined jobs (safer wrapper to release_job)
    :param jobs: 'queueing' or list corresponding to jobs to release
    :param queues: queues to target

    Prints a warning if job does not exist
    """
    ge_queues = Queues()

    # Find all jobs that match
    if jobs == 'holding':
        jobs = []
        if queues == 'all':
            jobs = ge_queues.holding.keys()
        else:
            for queue in queues:
                jobs += ge_queues.queues[queue].holding.keys()

        # Only user's jobs (they can't mess with the jobs of others)
        jobs = filter_user_job_ids(jobs)

    # check to see if job exists
    found_jobs = [job for job in jobs if job in ge_queues.jobs]
    missing = sorted(set(jobs) - set(found_jobs))
    if missing:
        print(f'Missing jobs: {", ".join(missing)}')

    release_job(*found_jobs)


def filter_user_job_ids(job_ids, user=None):
    """
    Filter user job ids from a list of job ids
    :param jobs: list of jobs
    :param user: user, if None, selects current user
    """
    ge_queues = Queues()
    if user is None:
        user = getpass.getuser()

    jfi = ge_queues.job_from_id
    return [id for id in job_ids if jfi(id).owner == user]


def current_job_id():
    """
    Find current job in completed list
    :return: job_id
    """
    with open(expanduser('~/.config/job_queue/completed_list_current')) as f:
        return int(f.readline())


def next_job():
    """
    Find next completed job
    :return: job_id, name, directory
    """
    c_jid_match = f'{current_job_id()}:'
    print(c_jid_match)
    with open(expanduser('~/.config/job_queue/completed')) as f:
        match = False
        for line in f:
            if c_jid_match == line[:len(c_jid_match)]:
                match = True
                break
        if not match:
            print('Cannot find job')
            return None, None, None

        try:
            job_id, name, hyphen, directory = next(f).split()
            job_id = job_id[:-1]  # strip colon
        except StopIteration as e:
            print('At last job')
            return None, None, None

        return job_id, name, directory
