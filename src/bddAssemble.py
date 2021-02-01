import xml.etree.ElementTree as ET

separator = "----"



def inputError(test, phrase):   #Eviter les erreurs string/int de l'input
    while True:
        inputUser = input(phrase)

        if(inputUser.isdigit() and test==int):
            return inputUser
        elif(not(inputUser.isdigit()) and test==str):
            return inputUser
        print("Type demandé",test)

#Creation de la structure du fichier



print("-----Création du base de données simple-----")
nomBdd = inputError(str, "Saisir le nom de base: ")
nbTable = inputError(int, "Combien de table?: ")


#enumerationVariable = {"bigint":20, , "bigserial":}

data = ET.Element('base')
items = ET.SubElement(data, 'items')
item1 = ET.SubElement(items, 'item')
item2 = ET.SubElement(items, 'item')
item1.set('name','item1')
item2.set('name','item2')
item1.text = 'item1abc'
item2.text = 'item2abc'

#Ecrire le tout dans un fichier
mydata = ET.tostring(data)
myfile = open("config/bdd.xml", "w")
myfile.write(mydata.decode("utf_8"))


#ENUMERATEUR pour varchar.... 