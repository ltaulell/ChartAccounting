#!/usr/bin/env python3
# coding: utf-8
# Ce programme a été écrit par Damien LE BORGNE le 28/01/2021
# Gestion base de données

import psycopg2
from psycopg2.sql import SQL, Identifier
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

import logging
from config import parserIni

fichierConfig = 'infodb.ini'
logging.basicConfig(**parserIni(filename=fichierConfig, section='logs'), level=logging.DEBUG)


class BddTransaction(object):
    """ """
    def __init__(self):
        self.paramsDb = None
        self.conn = None
        self.commandEnCours = None

    def configCon(self, fichierConfig=fichierConfig):
        try:
            self.paramsDb = parserIni(filename=fichierConfig, section='postgresql')
        except(Exception) as Err:
            print(Err)
            logging.warning(Err)

    def establishCon(self):
        """
            Etablir la connexion avec la base de données
        """
        try:
            self.conn = psycopg2.connect(**self.paramsDb)  # Connexion à la base de données
            self.conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            logging.info('Connect with {0}'.format(self.paramsDb))
        except (Exception, psycopg2.OperationalError) as Err:
            print(Err)
            logging.warning(Err)

    def version(self):
        cur = self.conn.cursor()  # Ouvrir le cursor
        cur.execute('SELECT version()')  # Executer une commande
        db_version = cur.fetchone()  # Recuperer la réponse
        cur.close()  # Fermer le cursor
        return db_version  # Retourner la réponse

    def createTable(self, listeXml):
        """
            Création des tables selon un fichier xml
        """
        command = []

        for table in listeXml:
            com1 = "CREATE TABLE IF NOT EXISTS " + table[0] + " ("
            for i, col in enumerate(table[1]):
                com1 += col[0] + " " + col[1]
                if(i < len(table[1]) - 1):
                    com1 += ", "
            com1 += ")"
            command.append(com1)
        self.commandEnCours = command

    def executeCommand(self):
        correct = True
        cur = self.conn.cursor()
        try:
            for command in self.commandEnCours:
                cur.execute(command)
            cur.close()
        except (Exception, psycopg2.errors.SyntaxError) as Err:
            print(Err)
            logging.warning(Err)
            correct = False
        if(correct):
            self.commandEnCours = None

    def printCommand(self):
        """
            Afficher ma liste de commande en cours
        """
        print(self.commandEnCours)

    def useCommand(self, command=""):
        """
            Ecrire une requete personnalisé
        """
        self.commandEnCours = command

    def deleteCommand(self):
        """
            Supprimer la comande en cours
        """
        self.commandEnCours = None

    def disconnectCon(self):
        """
            Fermer la liaison avec la base de données
        """
        self.conn.close()

    def insertData(self, bddXml, data, table=None):
        cur = self.conn.cursor()

        column = ""
        colPanda = []
        fkList = []
        fk = False
        for tables in bddXml:
            for test in range(len(tables[1])):
                if(column != ""):
                    column += ", "
                if(len(tables[1][test]) == 3):
                    fk = True
                    column += '"' + (tables[1][test][0]) + '"'
                    fkList.append(tables[1][test])
                else:
                    column += '"' + (tables[1][test][0]) + '"'
                    colPanda.append(tables[1][test][1])

            values = list(data[colPanda].values.astype(str))

            for v in values:
                if(fk):
                    fkValue = []
                    if(len(fkList) == 1):
                        fetch = "SELECT {colonne} FROM {table} WHERE {colWanted}='%s'" % data[fkList[0][1]].values.astype(str)[0]
                        cur.execute(SQL(fetch).format(
                            colonne=Identifier(str(fkList[0][0])),  # Generer les requetes SQL de maniere dynamique, (sure) avoid sql eject
                            table=Identifier(str(fkList[0][0][3:])),
                            colWanted=Identifier(str(fkList[0][2]))
                        ))
                        row = cur.fetchone()
                        if(row != None):
                            fkValue.append(str(row[0]))
                        else:
                            raise Exception('Erreur lors de liaison foreign key')
                    else:
                        for fkl in fkList:
                            fetch = "SELECT {colonne} FROM {table} WHERE {colWanted}='%s'" % data[str(fkl[1])].values.astype(str)[0]
                            cur.execute(SQL(fetch).format(
                                colonne=Identifier(str(fkl[0])),  # Generer les requetes SQL de maniere dynamique, (sure) avoid sql eject
                                table=Identifier(str(fkl[0][3:])),
                                colWanted=Identifier(str(fkl[2]))
                            ))
                            row = cur.fetchone()
                            if(row != None):
                                fkValue.append(str(row[0]))
                            else:
                                raise Exception('Erreur lors de liaison foreign key')

                if(len(tables[1]) > 1):
                    query = ("""INSERT INTO {table}({column}) VALUES%s;""").format(table=tables[0], column=column)  # Dans la requete slq, les données sont pas dans un tuple d'où %s

                    vlue = tuple(fkValue) + tuple(v)  # concaténation des id et des valeurs des colonnes
                    try:
                        cur.execute(query, (vlue, ))
                    except(psycopg2.errors.UniqueViolation):
                        pass
                    except(Exception) as ex:
                        logging.warning(ex)

                else:
                    query = ("""INSERT INTO {table}({column}) VALUES(%s);""").format(table=tables[0], column=column)  # Dans la requete sql, les données ne sont pas dans un tuple d'où (%s)
                    vlue = str(v[0])
                    try:
                        cur.execute(query, (vlue, ))
                    except(psycopg2.errors.UniqueViolation):
                        pass  # Nous le savons, plusieurs meme utilisateur ou groupes ou autre effectue un job, dans notre bdd user..., nous ne voulons que notre utilisateurs se retrouve une seule fois, donc si nous tentons de la rajouter une autre fois, nous ne relevons pas dexception et nous poursuivons le programme
                    except(Exception) as ex:
                        logging.warning(ex)

            # Reset variables
            colPanda = []
            column = ""
            fk = False
            fkList = []

    def dropBdd(self, args):
        """
            Drop table, pour éviter toute erreur Il faut d'abord générer cette commande, puis l'exécuter
        """
        cur = self.conn.cursor()
        args = list(args)  # Chaque table, est passé en liste pour être par la suite ajouter dans une liste, contenant les commandes SQL
        cmd = []
        for i in args:
            cmd.append(SQL("DROP TABLE {table}").format(table=Identifier(i)))
        self.commandEnCours = cmd
        cur.close()


"""
sql.SQL and sql.Identifier are needed to avoid SQL injection attacks.
cur.execute(sql.SQL('CREATE DATABASE {};').format(
    sql.Identifier(self.db_name)))
"""
