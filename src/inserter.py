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
- GROS GROS refactor + nettoyage
- find a way around transaction(s) ?
- yielder/getter ? -> multiprocessing inserts ? (maybe faster?)
    https://www.psycopg.org/docs/usage.html#thread-safety
    https://www.psycopg.org/docs/advanced.html#green-support

"""

import argparse
import logging
import csv
import sys
import psycopg2
import yaml

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
    parser.add_argument('-v', '--verbose', action='store_true', help='toggle verbose ON')
    # parser.add_argument('-n', '--dryrun', action='store_true', help='dry run')
    parser.add_argument('-i', '--input', nargs=1, type=str, help='input (accounting file')

    if helper:
        return parser.print_usage()
        # return parser.print_help()
    else:
        return parser.parse_args()


def decomment(fichiercsv):
    """ do not yield row containing '#' in first place """
    for row in fichiercsv:
        raw = row.split('#')[0].strip()
        if raw:
            yield raw


def coincoin(connexion, commande, payload, commit=False):
    """ execute commande, always return id
    SQL inserts MUST returning ids """
    with connexion.cursor() as cursor:
        cursor.execute(commande, str(payload))
        if commit:
            connexion.commit()
        log.debug(cursor.statusmessage)
        return cursor.fetchone()


def select_or_insert(conn, table, id_name, payload, name=None, multi=False):
    """ FIXME: ajouter un insert=True ? """

    payload_str = ''.join(['(', payload, ',)'])
    log.debug('payload: {}'.format(payload_str))

    if multi is False:
        sql_str = ''.join(['"SELECT ', id_name, ' FROM ', table, ' WHERE ', name, ' LIKE (%s);"'])
        result = coincoin(conn, sql_str, payload_str)
        log.debug('select: {}'.format(result))

        if result is None:
            sql_str = ''.join(['"INSERT INTO ', table, '(', name, ') VALUES (%s) RETURNING ', id_name, ';"'])
            result = coincoin(conn, sql_str, payload_str, commit=True)
            log.debug('insert: {}'.format(result))

    else:
        id1, id2 = id_name
        sql_str = ''.join(['"SELECT ', id1, ',', id2, ' FROM ', table, ' WHERE ', id1, ' = (%s) AND ', id2, ' = (%s);"'])
        result = coincoin(conn, sql_str, payload_str)
        log.debug('select: {}'.format(result))

        if result is None:
            sql_str = ''.join(['"INSERT INTO ', table, '(', id1, ',', id2, ') VALUES (%s, %s) RETURNING ', id1, ',', id2, ';"'])
            result = coincoin(conn, sql_str, payload_str, commit=True)
            log.debug('insert: {}'.format(result))

    return result


def load_yaml_file(yamlfile):
    """ Load yamlfile, return a dict

        yamlfile is mandatory, using safe_load
        Throw yaml errors, with positions, if any, and quit.
        return a dict
    """
    try:
        with open(yamlfile, 'r') as f:
            contenu = yaml.safe_load(f)
            return contenu
    except IOError:
        log.critical('Unable to read/load config file: {}'.format(f.name))
        sys.exit(1)
    except yaml.MarkedYAMLError as erreur:
        if hasattr(erreur, 'problem_mark'):
            mark = erreur.problem_mark
            msg_erreur = "YAML error position: ({}:{}) in ".format(mark.line + 1,
                                                                   mark.column)
            log.critical('{} {}'.format(msg_erreur, f.name))
        sys.exit(1)


if __name__ == '__main__':
    """
        pd.read_csv autodetecte des types qui sont ensuite poussés vers la base,
        je reviens sur un open simple (de toute façon, c'est du ligne à ligne)
    """

    args = get_args()

    if args.debug:
        log.setLevel('DEBUG')
        log.debug(get_args(helper=True))
    elif args.verbose:
        log.setLevel('INFO')

    if args.input:
        fichier = ''.join(args.input)
        log.debug('input: {}'.format(fichier))
    else:
        log.warning('no input file!')
        exit(1)

    # prepare yaml dictionnaries
    CLUSTERS = load_yaml_file(CLUSTERS_FILE)
    METAGROUPES = load_yaml_file(METAGROUPES_FILE)

    # prepare la config locale pgsql
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

            with conn:
                # queue
                idQueue = select_or_insert(conn, table='queues', id_name='id_queue', name='queue_name', payload=line['qname'])
                """
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
                """

                # host
                idHost = select_or_insert(conn, table='hosts', id_name='id_host', name='hostname', payload=line['host'])
                """
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
                """

                # group
                idGroup = select_or_insert(conn, table='groupes', id_name='id_groupe', name='group_name', payload=line['group'])
                """
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
                """

                # user/login/owner
                idUser = select_or_insert(conn, table='users', id_name='id_user', name='login', payload=line['owner'])
                """
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
                """

                # hosts_in_queues
                newload = ''.join([idQueue, ',', idHost])
                id_HostinQueue = select_or_insert(conn, table='hosts_in_queues', id_name=['id_queue', 'id_host'], payload=newload, multi=True)
                """
                sql = ("SELECT id_queue, id_host FROM hosts_in_queues WHERE id_queue = (%s) AND id_host = (%s);")
                data = (idQueue, idHost)
                id_HostinQueue = coincoin(conn, sql, data)
                log.debug('select: {}'.format(id_HostinQueue))

                if id_HostinQueue is None:
                    sql = ("INSERT INTO hosts_in_queues(id_queue, id_host) VALUES (%s, %s) RETURNING id_queue, id_host;")
                    data = (idQueue, idHost)
                    id_HostinQueue = coincoin(conn, sql, data, commit=True)
                    log.debug('insert: {}'.format(id_HostinQueue))
                log.debug('id_HostinQueue : {}'.format(id_HostinQueue))
                """

                # users_in_groupes
                newload = ''.join([idGroup, ',', idUser])
                id_HostinQueue = select_or_insert(conn, table='users_in_groupes', id_name=['id_groupe', 'id_user'], payload=newload, multi=True)
                """
                sql = ("SELECT id_groupe, id_user FROM users_in_groupes WHERE id_groupe = (%s) AND id_user = (%s);")
                data = (idGroup, idUser)
                id_UserinGroupe = coincoin(conn, sql, data)
                log.debug('select: {}'.format(id_UserinGroupe))

                if id_UserinGroupe is None:
                    sql = ("INSERT INTO users_in_groupes(id_groupe, id_user) VALUES (%s, %s) RETURNING id_groupe, id_user;")
                    data = (idGroup, idUser)
                    id_UserinGroupe = coincoin(conn, sql, data, commit=True)
                    log.debug('insert: {}'.format(id_UserinGroupe))
                log.debug('id_UserinGroupe: {}'.format(id_UserinGroupe))
                """

                # hosts_in_clusters
                try:
                    cluster = [key for key in CLUSTERS for value in CLUSTERS[key].split() if value in line['host']][0]
                except IndexError:
                    cluster = 'default'

                # pas d'insert!
                # idCluster = select_or_insert(conn, table='clusters', id_name='id_cluster', name='cluster_name', payload=cluster)
                sql = ("SELECT id_cluster FROM clusters WHERE cluster_name LIKE (%s);")
                data = (cluster,)
                idCluster = coincoin(conn, sql, data)
                # log.debug('select: {}'.format(idCluster))

                if idCluster:
                    newload = ''.join([idCluster, ',', idHost])
                    id_HostinCluster = select_or_insert(conn, table='hosts_in_clusters', id_name=['id_cluster', 'id_host'], payload=newload, multi=True)
                    """
                    sql = ("SELECT id_cluster, id_host FROM hosts_in_clusters WHERE id_cluster = (%s) AND id_host = (%s) ;")
                    data = (idCluster, idHost)
                    id_HostinCluster = coincoin(conn, sql, data)
                    log.debug('select: {}'.format(id_HostinCluster))

                    if id_HostinCluster is None:
                        sql = ("INSERT INTO hosts_in_clusters(id_cluster, id_host) VALUES (%s, %s) RETURNING id_cluster, id_host;")
                        data = (idCluster, idHost)
                        id_HostinCluster = coincoin(conn, sql, data, commit=True)
                        log.debug('insert: {}'.format(id_HostinCluster))
                log.debug('id_HostinCluster: {}'.format(id_HostinCluster))
                    """

                # groupes_in_metagroupes
                try:
                    metagroupe_group = [key for key in METAGROUPES for value in METAGROUPES[key].split() if value in line['group']][0]
                except IndexError:
                    metagroupe_group = 'autres_ENS'

                # pas d'insert!
                # idMetaGroup = select_or_insert(conn, table='metagroupes', id_name='id_metagroupe', name='meta_name', payload=metagroupe_group)
                sql = ("SELECT id_metagroupe FROM metagroupes WHERE meta_name LIKE (%s);")
                data = (metagroupe_group,)
                idMetaGroup = coincoin(conn, sql, data)
                # log.debug('select: {}'.format(idMetaGroup))

                if idMetaGroup:
                    newload = ''.join([idMetaGroup, ',', idGroup])
                    id_GroupinMeta = select_or_insert(conn, table='groupes_in_metagroupes', id_name=['id_metagroupe', 'id_groupe'], payload=newload, multi=True)
                    """
                    sql = ("SELECT id_metagroupe, id_groupe FROM groupes_in_metagroupes WHERE id_metagroupe = (%s) AND id_groupe = (%s);")
                    data = (idMetaGroup, idGroup)
                    id_GroupinMeta = coincoin(conn, sql, data)
                    log.debug('select: {}'.format(id_GroupinMeta))

                    if id_GroupinMeta is None:
                        sql = ("INSERT INTO groupes_in_metagroupes(id_metagroupe, id_groupe) VALUES (%s, %s) RETURNING id_metagroupe, id_groupe;")
                        data = (idMetaGroup, idGroup)
                        id_GroupinMeta = coincoin(conn, sql, data, commit=True)
                        log.debug('insert: {}'.format(id_GroupinMeta))
                log.debug('id_GroupinMeta: {}'.format(id_GroupinMeta))
                    """

                # users_in_metagroupes
                try:
                    metagroupe_user = [key for key in METAGROUPES for value in METAGROUPES[key].split() if value in line['owner']][0]
                    # pas d'insert!
                    sql = ("SELECT id_metagroupe FROM metagroupes WHERE meta_name LIKE (%s);")
                    data = (metagroupe_user,)
                    idMetaUser = coincoin(conn, sql, data)
                    # log.debug('select: {}'.format(idMetaUser))

                    if idMetaUser:
                        newload = ''.join([idMetaUser, ',', idUser])
                        id_UserinMeta = select_or_insert(conn, table='users_in_metagroupes', id_name=['id_metagroupe', 'id_user'], payload=newload, multi=True)
                        """
                        sql = ("SELECT id_metagroupe, id_user FROM users_in_metagroupes WHERE id_metagroupe = (%s) AND id_user = (%s);")
                        data = (idMetaUser, idUser)
                        id_UserinMeta = coincoin(conn, sql, data)
                        log.debug('select: {}'.format(id_UserinMeta))

                        if id_UserinMeta is None:
                            sql = ("INSERT INTO users_in_metagroupes(id_metagroupe, id_user) VALUES (%s, %s) RETURNING id_metagroupe, id_user;")
                            data = (idMetaUser, idUser)
                            id_UserinMeta = coincoin(conn, sql, data, commit=True)
                            log.debug('insert: {}'.format(id_UserinMeta))
                    log.debug('id_UserinMeta: {}'.format(id_UserinMeta))
                        """

                except IndexError:
                    # là, par contre, c'est pas obligatoire
                    pass

                # finally, job
                # FIXME: select d'abord, insert ensuite

                sql = ("SELECT id_queue, id_host, id_user, job_id, start_time, end_time FROM job_ WHERE id_queue = (%s) AND id_host = (%s) AND id_user = (%s) AND job_id = (%s) AND start_time = (%s) AND end_time = (%s);")
                data = (idQueue[0], idHost[0], idUser[0], line['job_id'], line['start'], line['end'])
                jobExist = coincoin(conn, sql, data)

                if jobExist is None:
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

                    jobCommit = coincoin(conn, sql, data, commit=True)
                    log.info('commited: {}, {}, {}'.format(line['job_id'], line['qname'], line['host']))

                else:
                    log.info('job {} already exist in database'.format(line['job_id']))

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
