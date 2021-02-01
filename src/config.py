#!/usr/bin/env python3
# coding: utf-8
# Ce programme a été écrit par Damien LE BORGNE le 28/01/2021
# Lecture des fichiers .ini et .xml

from configparser import ConfigParser
import xml.dom.minidom

def parserIni(filename, section):
    parserData = ConfigParser()
    #Avec le parser, lire le fichier
    parserData.read("config/"+filename)
    paramsDb = {}
    if parserData.has_section(section):
        params = parserData.items(section)
        for param in params:
            paramsDb[param[0]] = param[1]
    else:
        raise Exception('Section [{0}] not found in file {1}'.format(section, filename))
        #créer un declencheur
    return paramsDb


def readBddXml(filename="config/bdd.xml", insert=True):
    command = []
    try:
        doc = xml.dom.minidom.parse(filename)
        if(doc.firstChild.tagName == "data"):
            for table in doc.getElementsByTagName("table"):
                colNom = []
                if(not insert):
                    for col in table.getElementsByTagName("column"):
                        colNom.append([col.getAttribute("name"), col.firstChild.nodeValue])
                    command.append([table.getAttribute("name"), colNom])
                else:
                    #Creer un tableau pour la gestion des table, colonne...
                    for col in table.getElementsByTagName("column"):
                        if(col.getAttribute("name").startswith("id") and col.getAttribute("datadb")):
                            colNom.append([col.getAttribute("name"), col.getAttribute("data"), col.getAttribute("datadb")])
                        elif(col.getAttribute("data")):
                            colNom.append([col.getAttribute("name"), col.getAttribute("data")])
                    command.append([table.getAttribute("name"), colNom])
                
        return command
    except FileNotFoundError:
        print("Indicate a correct path")
    except Exception as Err:
        print(Err)


    
