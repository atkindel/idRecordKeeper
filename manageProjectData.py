#!/usr/bin/env python

import urllib2
import os.path
import sys
import ConfigParser
import json
import StringIO as sio
import zipfile as z
from pypodio2 import api

### Script automatically refreshes Podio projects and consultation data
### Pulls from Qualtrics API

class ProjectDataManager(object):
    '''
    Refreshes Podio project and consultation data from Qualtrics forms.
    '''

    def __init__(self):
        '''
        Initialize Podio and Qualtrics API credentials from .ssh
        '''
        home = os.path.expanduser("~")

        self.qtUser, self.qtToken = self.__configQualtrics()
        self.pdETL, self.pdKey, self.pdApp = self.__configPodio(home + '/.ssh/idrk.cfg')


    def __configQualtrics(self):
        '''
        Method retrieves Qualtrics credentials from .ssh directory
        '''
        home = os.path.expanduser("~")

        userFile = home + '/.ssh/qualtrics_user'
        tokenFile = home + '/.ssh/qualtrics_token'
        if os.path.isfile(userFile) == False:
            sys.exit("User file not found: " + userFile)
        if os.path.isfile(tokenFile) == False:
            sys.exit("Token file not found: " + tokenFile)

        user = None
        token = None
        with open(userFile, 'r') as f:
            user = f.readline().rstrip()
        with open(tokenFile, 'r') as f:
            token = f.readline().rstrip()

        return user, token


    def __configPodio(self, cfgpath):
        '''
        Method retrieves Podio app IDs from .ssh directory.
        '''
        try:
            config = ConfigParser.RawConfigParser()
            config.read(cfgpath)
            etl = config.get('APIKey', 'etl') # api app id
            key = config.get('APIKey', 'key') # api key
            app = config.get('APIKey', 'app') # podio internal app id
            return etl, key, app
        except IOError:
            print ("File %s not found." % config)


    def extractQualtrics(self):
        '''
        Pull form data down from Qualtrics.
        '''

        idRRF = 'SV_78KTbL61clEWsO9'
        idPRF = 'SV_bftcKQJ9cGUyPI1'

        dataRRF = self.__getFormData(idRRF)
        dataPRF = self.__getFormData(idPRF)

        return dataRRF, dataPRF
        

    def __getFormData(self, surveyID):
        '''
        Pull PRF form data down from Qualtrics. From qualtrics_etl.
        Returns JSON object containing untransformed survey data.
        '''

        urlTemp = Template("https://dc-viawest.qualtrics.com:443/API/v1/surveys/${svid}/responseExports?apiToken=${tk}&fileType=JSON")
        reqURL = urlTemp.substitute(svid=surveyID, tk=self.qtToken)
        req = json.loads(urllib2.urlopen(reqURL).read())

        statURL = req['result']['exportStatus'] + "?apiToken=" + self.qtToken
        percent, tries = 0, 0
        while percent != 100 and tries < 20:
            time.sleep(5) # Wait 5 seconds between attempts to acquire data
            try:
                stat = json.loads(urllib2.urlopen(statURL).read())
                percent = stat['result']['percentComplete']
            except:
                print "Extractor recovered from HTTP error."
                continue
            finally:
                tries += 1
        if tries >= 20:
            logging.error("Survey %s timed out." % surveyID)
            return None

        dataURL = stat['result']['fileUrl']
        remote = urllib2.urlopen(dataURL).read()
        dataZip = sio.StringIO(remote)
        archive = z.ZipFile(dataZip, 'r')
        dataFile = archive.namelist()[0]
        data = json.loads(archive.read(dataFile), object_pairs_hook=OrderedDict)

        if not data['responses']:
            return None
        else:
            return data



if __name__ == '__main__':
    pdm = ProjectDataManager()
    rrf, prf = pdm.extractQualtrics()
    try:
        print rrf
    except:
        print "rrf failed"
        continue
    try:
        print prf
    except:
        print "prf failed"
        continue
