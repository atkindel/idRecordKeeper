#!/usr/bin/env python

import urllib2
import os.path
import sys
import ConfigParser

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
        Pull form data down from Qualtrics
        '''
        print self.qtUser, self.qtToken, self.pdETL, self.pdKey, self.pdApp



if __name__ == '__main__':
    pdm = ProjectDataManager()
    pdm.extractQualtrics()
