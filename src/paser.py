#!/usr/bin/env python3
# coding: utf-8
# Ce programme a été écrit par Damien LE BORGNE le 28/01/2021
# Utilisation rapide, du module bdd_interact

import pandas as pd
from bdd_interact import BddTransaction
import config 

bddXml = config.readBddXml() #insert=False pour la creation de la bdd
#bddXml = config.readBddXml(insert=False) #insert=False pour la creation de la bdd
#print(bddXml)


header_list = ['qname', 'host', 'group', 'owner', 'job_name', 'job_id', 'account', 'priority', 'submit_time', 'start', 'end', 'fail', 'exit_status', 'ru_wallclock', 'ru_utime', 'ru_stime', 'ru_maxrss', 'ru_ixrss', 'ru_ismrss', 'ru_idrss', 'ru_isrss', 'ru_minflt', 'ru_majflt', 'ru_nswap', 'ru_inblock', 'ru_oublock', 'ru_msgsnd', 'ru_msgrcv', 'ru_nsignals', 'ru_nvcsw', 'ru_nivcsw', 'project', 'department', 'granted_pe', 'slots', 'task_number', 'cpu', 'mem', 'io', 'category', 'iow', 'pe_taskid', 'maxvmem', 'arid', 'ar_submission_time']

e = BddTransaction()
e.configCon() #configCon(filename, section)
e.establishCon()
if(e.conn != None and bddXml != None):
    #e.createTable(bddXml)
    for line in pd.read_csv('accounting-2017-modif', sep=":", names=header_list, skip_blank_lines=True, index_col=False, chunksize=1):
        e.insertData(bddXml, line)
    #e.dropBdd(['job_', 'queue', 'clusters', 'metagroups', 'groupes', 'hosts', 'users'])
    e.executeCommand()

e.disconnectCon()