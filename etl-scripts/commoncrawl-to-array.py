import re
from urllib.parse import urlparse
import time


def getDomain(url):
    parsed_uri = urlparse( url )
    domain = '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_uri)
    return domain

def count( counter ):
    counter = counter + 1
    return counter

fileName = "../data/CC-MAIN-20160924173739-00000-ip-10-143-35-109.ec2.internal.warc"
test = "../data/test.txt"

headWordMatch = "^WARC-Target-URI: (.*)$"
regexHead = re.compile(headWordMatch)

bodyWordMatch = "(?<=\<a)(.*)(?<=href)(.*)(http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+)"
regexBody = re.compile(bodyWordMatch)
    
counterHead = 0
counterBody = 0

previous_match = ""
current_match = ""

href = []

print("[*] Traitement en cours...")
t0 = time.clock()

with open(fileName , encoding='latin-1') as f:
    for line in f:
        for match in regexHead.findall(line):
            current_match = getDomain(match)
        if previous_match != current_match:
            previous_match = getDomain(current_match)
            counterHead = count(counterHead)
        else:
            for url in regexBody.findall(line):
                counterBody = count(counterBody)

        
        
            

                
            

print("[*] Traitement fini...")
print("Il y a ", counterHead, "référence pour la chaine :", headWordMatch)
print("Il y a ", counterBody, "référence pour la chaine :", bodyWordMatch)
print("Temps écoulé : ",time.clock() - t0 , " seconds")
