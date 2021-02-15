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
import csv
import psycopg2
import yaml
import sys

import config

log = logging.getLogger()
stream_handler = logging.StreamHandler()
log.addHandler(stream_handler)

CLUSTERS_FILE = 'config/clusters.yml'
METAGROUPES_FILE = 'config/metagroups.yml'
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


def decomment(csvfile):
    for row in csvfile:
        raw = row.split('#')[0].strip()
        if raw:
            yield raw


def coincoin(connexion, sql, data, commit=False):
    """ execute sql, always return id
    SQL inserts MUST returning ids """
    with connexion.cursor() as cursor:
        cursor.execute(sql, data)
        if commit:
            connexion.commit()
        log.debug(cursor.statusmessage)
        return cursor.fetchone()


def load_yaml_file(yamlfile):
    """ Load yamlfile, return a dict

        yamlfile is mandatory, using safe_load
        Throw yaml errors, with positions, if any, and quit.
        return a dict
    """
    try:
        with open(yamlfile, 'r') as fichier:
            contenu = yaml.safe_load(fichier)
            return contenu
    except IOError:
        log.critical('Unable to read/load config file: {}'.format(fichier.name))
        sys.exit(1)
    except yaml.MarkedYAMLError as erreur:
        if hasattr(erreur, 'problem_mark'):
            mark = erreur.problem_mark
            msg_erreur = "YAML error position: ({}:{}) in ".format(mark.line + 1,
                                                                   mark.column)
            log.critical('{} {}'.format(msg_erreur, fichier.name))
        sys.exit(1)


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

    # prepare yaml dictionnaries
    CLUSTERS = load_yaml_file(CLUSTERS_FILE)
    METAGROUPES = load_yaml_file(METAGROUPES_FILE)

    param_conn_db = config.parserIni(filename='infodb.ini', section='postgresql')
    log.debug(param_conn_db)

    conn = psycopg2.connect(**param_conn_db)
    # conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    # conn.set_session(isolation_level=psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE, autocommit=True)
    log.debug(conn)

    with open(fichier, "r", encoding='latin1') as csvfile:
        reader = csv.DictReader(decomment(csvfile), fieldnames=HEADER_LIST, delimiter=':')
        for line in reader:
            log.debug('{}, {}, {}, {}'.format(line['qname'], line['host'], line['group'], line['cpu']))
            # exit(0)

            with conn:
                # queue
                sql = ("SELECT id_queue FROM queues WHERE queue_name LIKE (%s);")
                data = (line['qname'],)
                idQueue = coincoin(conn, sql, data)
                log.debug('select: {}'.format(idQueue))

                if idQueue is None:
                    sql = ("INSERT INTO queues(queue_name) VALUES (%s) RETURNING id_queue;")
                    data = (line['qname'],)
                    idQueue = coincoin(conn, sql, data, commit=True)
                    log.debug('insert: {}'.format(idQueue))
                log.debug('idQueue: {}'.format(idQueue))

                # host
                sql = ("SELECT id_host FROM hosts WHERE hostname LIKE (%s);")
                data = (line['host'],)
                idHost = coincoin(conn, sql, data)
                log.debug('select: {}'.format(idHost))

                if idHost is None:
                    sql = ("INSERT INTO hosts(hostname) VALUES (%s) RETURNING id_host;")
                    data = (line['host'],)
                    idHost = coincoin(conn, sql, data, commit=True)
                    log.debug('insert: {}'.format(idHost))
                log.debug('idHost: {}'.format(idHost))

                # group
                sql = ("SELECT id_groupe FROM groupes WHERE group_name LIKE (%s);")
                data = (line['group'],)
                idGroup = coincoin(conn, sql, data)
                log.debug('select: {}'.format(idGroup))

                if idGroup is None:
                    sql = ("INSERT INTO groupes(group_name) VALUES (%s) RETURNING id_groupe;")
                    data = (line['group'],)
                    idGroup = coincoin(conn, sql, data, commit=True)
                    log.debug('insert: {}'.format(idGroup))
                log.debug('idGroup: {}'.format(idGroup))

                # user/login/owner
                sql = ("SELECT id_user FROM users WHERE login LIKE (%s);")
                data = (line['owner'],)
                idUser = coincoin(conn, sql, data)
                log.debug('select: {}'.format(idUser))

                if idUser is None:
                    sql = ("INSERT INTO users(login) VALUES (%s) RETURNING id_user;")
                    data = (line['owner'],)
                    idUser = coincoin(conn, sql, data, commit=True)
                    log.debug('insert: {}'.format(idUser))
                log.debug('idUser: {}'.format(idUser))

                # hosts_in_queues
                sql = ("SELECT id_queue, id_host FROM hosts_in_queues WHERE id_queue = (%s) AND id_host = (%s);")
                data = (idQueue, idHost)
                id_HostinQueue = coincoin(conn, sql, data)
                log.debug('select: {}'.format(id_HostinQueue))
                if id_HostinQueue is None:
                    sql = ("INSERT INTO hosts_in_queues(id_queue, id_host) VALUES (%s, %s) RETURNING id_queue, id_host;")
                    data = (idQueue, idHost)
                    id_HostinQueue = coincoin(conn, sql, data, commit=True)
                    log.debug('insert: {}'.format(id_HostinQueue))

                # users_in_groupes
                sql = ("SELECT id_groupe, id_user FROM users_in_groupes WHERE id_groupe = (%s) AND id_user = (%s);")
                data = (idGroup, idUser)
                id_UserinGroupe = coincoin(conn, sql, data)
                log.debug('select: {}'.format(id_UserinGroupe))
                if id_UserinGroupe is None:
                    sql = ("INSERT INTO users_in_groupes(id_groupe, id_user) VALUES (%s, %s) RETURNING id_groupe, id_user;")
                    data = (idGroup, idUser)
                    id_UserinGroupe = coincoin(conn, sql, data, commit=True)
                    log.debug('insert: {}'.format(id_UserinGroupe))

                # hosts_in_clusters
                cluster = [key for key in CLUSTERS for value in CLUSTERS[key].split() if value in line['host']][0]
                sql = ("SELECT id_cluster FROM clusters WHERE cluster_name LIKE (%s);")
                data = (cluster,)
                idCluster = coincoin(conn, sql, data)
                if idCluster:
                    sql = ("SELECT id_cluster, id_host FROM hosts_in_clusters WHERE id_cluster = (%s) AND id_host = (%s) ;")
                    data = (idCluster, idHost)
                    id_HostinCluster = coincoin(conn, sql, data)
                    log.debug('select: {}'.format(id_HostinCluster))
                    if id_HostinCluster is None:
                        sql = ("INSERT INTO hosts_in_clusters(id_cluster, id_host) VALUES (%s, %s) RETURNING id_cluster, id_host;")
                        data = (idCluster, idHost)
                        id_HostinCluster = coincoin(conn, sql, data, commit=True)
                        log.debug('insert: {}'.format(id_HostinCluster))

                # groupes_in_metagroupes TODO
                metagroupe_group = [key for key in METAGROUPES for value in METAGROUPES[key].split() if value in line['group']][0]

                # users_in_metagroupes TODO
                metagroupe_user = [key for key in METAGROUPES for value in METAGROUPES[key].split() if value in line['owner']][0]

                # finally, job
                # TODO: recherche d'abord, insert ensuite
                sql = (""" INSERT INTO job_(id_queue,
                                            id_host,
                                            id_groupe,
                                            id_user,
                                            job_name,
                                            job_id,
                                            submit_time,
                                            start_time,
                                            end_time,
                                            failed,
                                            exit_status,
                                            ru_wallclock,
                                            ru_utime,
                                            ru_stime,
                                            project,
                                            slots,
                                            cpu,
                                            mem,
                                            io,
                                            maxvmem)
                            VALUES (%s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s,
                                    %s)
                            RETURNING job_id; """)
                data = (idQueue[0],
                        idHost[0],
                        idGroup[0],
                        idUser[0],
                        line['job_name'],
                        line['job_id'],
                        line['submit_time'],
                        line['start'],
                        line['end'],
                        line['fail'],
                        line['exit_status'],
                        line['ru_wallclock'],
                        line['ru_utime'],
                        line['ru_stime'],
                        line['project'],
                        line['slots'],
                        line['cpu'],
                        line['mem'],
                        line['io'],
                        line['maxvmem'],
                        )

                coincoin(conn, sql, data, commit=True)

    conn.close()


"""
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
                        float(line['maxvmem'],)

"""

"""
HEADER_LIST = ['qname', 'host', 'group', 'owner', 'job_name', 'job_id', 'account', 'priority', 'submit_time', 'start', 'end', 'fail', 'exit_status', 'ru_wallclock', 'ru_utime', 'ru_stime', 'ru_maxrss', 'ru_ixrss', 'ru_ismrss', 'ru_idrss', 'ru_isrss', 'ru_minflt', 'ru_majflt', 'ru_nswap', 'ru_inblock', 'ru_oublock', 'ru_msgsnd', 'ru_msgrcv', 'ru_nsignals', 'ru_nvcsw', 'ru_nivcsw', 'project', 'department', 'granted_pe', 'slots', 'task_number', 'cpu', 'mem', 'io', 'category', 'iow', 'pe_taskid', 'maxvmem', 'arid', 'ar_submission_time']
"""
