#!/usr/bin/env python3
# coding: utf-8
# Ce programme a été écrit par Damien LE BORGNE le 28/01/2021
# Utilisation rapide, du module bdd_interact

import argparse
import logging
import pandas as pd

import config
from bdd_interact import BddTransaction

log = logging.getLogger()
stream_handler = logging.StreamHandler()
log.addHandler(stream_handler)

bddXml = config.readBddXml()

header_list = ['qname', 'host', 'group', 'owner', 'job_name', 'job_id', 'account', 'priority', 'submit_time', 'start', 'end', 'fail', 'exit_status', 'ru_wallclock', 'ru_utime', 'ru_stime', 'ru_maxrss', 'ru_ixrss', 'ru_ismrss', 'ru_idrss', 'ru_isrss', 'ru_minflt', 'ru_majflt', 'ru_nswap', 'ru_inblock', 'ru_oublock', 'ru_msgsnd', 'ru_msgrcv', 'ru_nsignals', 'ru_nvcsw', 'ru_nivcsw', 'project', 'department', 'granted_pe', 'slots', 'task_number', 'cpu', 'mem', 'io', 'category', 'iow', 'pe_taskid', 'maxvmem', 'arid', 'ar_submission_time']


def get_args(helper=False):
    """ read parser and return args (as args namespace),
        if helper=True, show usage() or help()
    """
    parser = argparse.ArgumentParser(description='upload accounting file into database')
    parser.add_argument('-d', '--debug', action='store_true', help='toggle debug ON')
    # parser.add_argument('-n', '--dryrun', action='store_true', help='dry run')
    parser.add_argument('-i', '--input', nargs=1, type=str, help='input (accounting file')

    if helper:
        return parser.print_usage()
        # return parser.print_help()
    else:
        return parser.parse_args()


if __name__ == '__main__':
    """ """

    args = get_args()
    log.debug(get_args(helper=True))

    log.debug(bddXml)

    if args.input:
        fichier = ''.join(args.input)
        log.debug('input: {}'.format(fichier))
    else:
        log.warning('no input file!')
        exit(1)

    e = BddTransaction()
    e.configCon()  # configCon(filename, section)
    e.establishCon()
    if(e.conn is not None and bddXml is not None):
        for line in pd.read_csv(fichier, sep=":", names=header_list, skip_blank_lines=True, index_col=False, chunksize=1, comment='#'):
            e.insertData(bddXml, line)
            e.executeCommand()

    e.disconnectCon()
