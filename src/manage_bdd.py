#!/usr/bin/env python3
# coding: utf-8

# $Id$
# SPDX-License-Identifier: BSD-2-Clause

# Copyright 2021 Damien LE BORGNE
# Copyright 2021 Loïs Taulelle

"""
Test du module bdd_interact

create, or purge, database.

"""

import argparse
import logging
import distutils.util

import config
from bdd_interact import BddTransaction

log = logging.getLogger()
stream_handler = logging.StreamHandler()
log.addHandler(stream_handler)

# bddXml = config.readBddXml()
# bddXml = config.readBddXml(insert=False)  # pour la creation de la bdd
# print(bddXml)


def get_args(helper=False):
    """ read parser and return args (as args namespace),
        if helper=True, show usage() or help()
    """
    parser = argparse.ArgumentParser(description='upload accounting file into database')
    parser.add_argument('-d', '--debug', action='store_true', help='toggle debug ON')
    # parser.add_argument('-n', '--dryrun', action='store_true', help='dry run')
    parser.add_argument('-c', '--create', action='store_true', help='create tables')
    parser.add_argument('-p', '--purge', action='store_true', help='purge tables (drop)')

    if helper:
        return parser.print_usage()
        # return parser.print_help()
    else:
        return parser.parse_args()


def query_yesno(question):
    """ Ask a yes/no question via input() and return 1 if yes, 0 elsewhere.

    No default. See query_default_yes_no().

    import distutils(.util)

    """
    print("\n" + question + " [y/n]?")
    while True:
        try:
            return distutils.util.strtobool(input().lower())
        except ValueError:
            print('Please reply "y" or "n".')


if __name__ == '__main__':
    """ """

    args = get_args()
    log.debug(get_args(helper=True))

    if args.create:
        bddXml = config.readBddXml(insert=False)
    elif args.purge:
        bddXml = config.readBddXml()
    else:
        get_args(helper=True)
        exit(1)

    log.debug(bddXml)

    e = BddTransaction()
    e.configCon()  # configCon(filename, section)
    e.establishCon()
    # if(e.conn != None and bddXml != None):
    if(e.conn is not None and bddXml is not None):
        if args.create:
            e.createTable(bddXml)
            gonogo = query_yesno('create tables')

        elif args.purge:
            e.dropBdd(['job_', 'queue', 'clusters', 'metagroups', 'groupes', 'hosts', 'users'])
            gonogo = query_yesno('drop tables')

        if gonogo == 1:
            e.executeCommand()

        e.disconnectCon()
