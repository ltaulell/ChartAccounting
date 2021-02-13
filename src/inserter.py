#!/usr/bin/env python3
# coding: utf-8

# $Id$
# SPDX-License-Identifier: BSD-2-Clause

# Copyright 2021 Loïs Taulelle
# Copyright 2021 Damien LE BORGNE

"""
Insert data from accounting file into database

go easy, go simple.

TODO:
- find a way around transaction(s)
- yielder/getter ? -> multiprocessing inserts ? (faster)
    https://www.psycopg.org/docs/usage.html#thread-safety
    https://www.psycopg.org/docs/advanced.html#green-support

"""

import argparse
import logging
import pandas as pd
import psycopg2

import config

log = logging.getLogger()
stream_handler = logging.StreamHandler()
log.addHandler(stream_handler)


HEADER_LIST = ['qname', 'host', 'group', 'owner', 'job_name', 'job_id', 'account', 'priority', 'submit_time', 'start', 'end', 'fail', 'exit_status', 'ru_wallclock', 'ru_utime', 'ru_stime', 'ru_maxrss', 'ru_ixrss', 'ru_ismrss', 'ru_idrss', 'ru_isrss', 'ru_minflt', 'ru_majflt', 'ru_nswap', 'ru_inblock', 'ru_oublock', 'ru_msgsnd', 'ru_msgrcv', 'ru_nsignals', 'ru_nvcsw', 'ru_nivcsw', 'project', 'department', 'granted_pe', 'slots', 'task_number', 'cpu', 'mem', 'io', 'category', 'iow', 'pe_taskid', 'maxvmem', 'arid', 'ar_submission_time']


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
    """ for line in pd.read_csv(fichier, sep=":", names=HEADER_LIST, skip_blank_lines=True, index_col=False, chunksize=1, comment='#'): """

    args = get_args()

    if args.debug:
        log.setLevel('DEBUG')
        log.debug(get_args(helper=True))

    if args.input:
        fichier = ''.join(args.input)
        log.debug('input: {}'.format(fichier))
    else:
        log.warning('no input file!')
        exit(1)

    param_conn_db = config.parserIni(filename='infodb.ini', section='postgresql')
    log.debug(param_conn_db)

    conn = psycopg2.connect(**param_conn_db)
    # conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    # conn.set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE, autocommit=True)
    log.debug(conn)

    for line in pd.read_csv(fichier, sep=':', names=HEADER_LIST, skip_blank_lines=True, index_col=False, chunksize=1, comment='#'):

        print(line)

        with conn:
            with conn.cursor() as cur:
                idQueue = cur.execute("SELECT id_queue FROM queues WHERE queue_name LIKE (%s);", (line['qname']))
                idQueue = cur.fetchone()
                print('select:', idQueue)

                if idQueue is None:
                    cur.execute("INSERT INTO queues(queue_name) VALUES (%s) RETURNING id_queue;", (line['qname']))
                    idQueue = cur.fetchone()
                    #conn.commit()
                    # cur.execute("SELECT id_queue FROM queues WHERE queue_name LIKE (%s);", (line['qname']))
                    # idQueue = cur.fetchone()
                    print('insert:', idQueue)
            print('idQueue:', idQueue)

        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id_host FROM hosts WHERE hostname LIKE (%s);", (line['host']))
                idHost = cur.fetchone()
                print('select:', idHost)

                if idHost is None:
                    cur.execute("INSERT INTO hosts(hostname) VALUES (%s) RETURNING id_host;", (line['host']))
                    idHost = cur.fetchone()
                    #conn.commit()
                    print('insert:', idHost)
            print('idHost:', idHost)

        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id_groupe FROM groupes WHERE group_name LIKE (%s);", (line['group']))
                idGroup = cur.fetchone()
                print('select:', idGroup)

                if idGroup is None:
                    cur.execute("INSERT INTO groupes(group_name) VALUES (%s) RETURNING id_groupe;", (line['group']))
                    idGroup = cur.fetchone()
                    # conn.commit()
                    print('insert:', idGroup)
            print('idGroup:', idGroup)

        with conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id_user FROM users WHERE login LIKE (%s);", (line['owner']))
                idUser = cur.fetchone()
                print('select:', idUser)

                if idUser is None:
                    cur.execute("INSERT INTO users(login) VALUES (%s);", (line['owner']))
                    #conn.commit()
                    print(cur.statusmessage)
                    idUser = cur.fetchone()
                    #conn.commit()
                    print('insert:', idUser)
            print('idUser:', idUser)

        with conn:
            with conn.cursor() as cur:
                cur.execute("INSERT INTO job_(id_queue, id_host, id_groupe, id_user, job_name, job_id, submit_time, start_time, end_time, failed, exit_status, ru_wallclock, ru_utime, ru_stime, project, slots, cpu, mem, io, maxvmem) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);",
                            (idQueue[0],
                            idHost[0],
                            idGroup[0],
                            idUser[0],
                            str(line['job_name']),
                            int(line['job_id']),
                            int(line['submit_time']),
                            int(line['start']),
                            int(line['end']),
                            int(line['fail']),
                            int(line['exit_status']),
                            float(line['ru_wallclock']),
                            float(line['ru_utime']),
                            float(line['ru_stime']),
                            str(line['project']),
                            int(line['slots']),
                            float(line['cpu']),
                            float(line['mem']),
                            float(line['io']),
                            float(line['maxvmem'])
                            ))
            conn.commit()

    conn.close()

"""
HEADER_LIST = ['qname', 'host', 'group', 'owner', 'job_name', 'job_id', 'account', 'priority', 'submit_time', 'start', 'end', 'fail', 'exit_status', 'ru_wallclock', 'ru_utime', 'ru_stime', 'ru_maxrss', 'ru_ixrss', 'ru_ismrss', 'ru_idrss', 'ru_isrss', 'ru_minflt', 'ru_majflt', 'ru_nswap', 'ru_inblock', 'ru_oublock', 'ru_msgsnd', 'ru_msgrcv', 'ru_nsignals', 'ru_nvcsw', 'ru_nivcsw', 'project', 'department', 'granted_pe', 'slots', 'task_number', 'cpu', 'mem', 'io', 'category', 'iow', 'pe_taskid', 'maxvmem', 'arid', 'ar_submission_time']
"""
