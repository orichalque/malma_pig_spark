# coding: utf8
import re
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from collections import defaultdict
import time


def getDomain(url):
    url = regexBody.findall(url)[0]
    parsed_uri = urlparse( url )
    domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
    return domain

def count( counter ):
    counter = counter + 1
    return counter

##Filenames
rootDir = "../data/"

fileName = "file.test.txt"
logFileName = "log-"+fileName
outputFileName = rootDir+"/etl-file.txt"
test = "../data/test.txt"

##Regex
headWordMatch = "^WARC-Target-URI: (.*)$"
regexHead = re.compile(headWordMatch)
bodyWordMatch = "^https?://.*"
regexBody = re.compile(bodyWordMatch)

##Counters
counterHead = 0
counterBody = 0

##Variables
previous_match = ""
current_match = ""
href = {}

##Start
with open(rootDir+logFileName ,"w+", encoding='utf-8') as log:
    log.write("[*] Traitement en cours...\n")
    log.write("[**]Lecture en cours ...\n")
    t0 = time.clock()
    with open(rootDir+fileName , encoding='latin-1') as f:
        for line in f:
            for match in regexHead.findall(line):
                current_match = getDomain(match)
            if previous_match != current_match:
                previous_match = current_match
                counterHead = count(counterHead)
                if(previous_match not in href and previous_match != None):
                    href[previous_match]= []
            else:
                try:
                    soup = BeautifulSoup(line, 'html.parser')
                    for link in soup.find_all('a'):
                        url = link.get('href')
                        url = getDomain(url)
                        if (url is not None):                            
                            counterBody = count(counterBody)
                            href[previous_match].append(url)
                            ##Si l'url n'est pas dans les urls primaires on l'ajoute
                            if(url not in href):
                                href[url]= []
                except:
                    log.write("Une erreur a eu lieu !...("+ str(time.clock() - t0) +"s)")


    log.write("[**]Lecture terminée, temps écoulé : "+ str(time.clock() - t0) +" seconds\n")

    log.write("[**] Ecriture en cours\n")

    keys = list(href.keys())
    with open(outputFileName,"w+", encoding='utf-8') as outputFile:
        for url in keys:
            i = 0
            line_to_write = str(url+" 1 {")
            for neighbours in href[url] :                     	
              if (i != 0):
                line_to_write = str(line_to_write + ",(" + neighbours + ")")
              else:
                line_to_write = str(line_to_write + "(" + neighbours + ")")
              i = i +1
            line_to_write =str( line_to_write +"}\n")
            if(line_to_write != None):
                outputFile.write(line_to_write)
    log.write("[**] Ecriture en cours , temps écoulé : "+ str(time.clock() - t0) +" seconds\n")
    ##End

    log.write("[*] Traitement fini...")
    log.write("Il y a "+ str(counterHead)+"référence pour la chaine :"+str(headWordMatch)+"\n")
    log.write("Il y a "+ str(counterBody)+ "référence pour la chaine :"+ str(bodyWordMatch)+"\n")
    log.write("Il y a "+ str(len(keys))+ "url primaire dans le fichier"+"\n")
    log.write("Temps écoulé : "+ str(time.clock() - t0) +" seconds"+"\n")
