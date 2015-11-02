#!/usr/bin/env python

import urllib2
import os.path
import sys
import ConfigParser
import json
import time
import datetime as dt
import StringIO as sio
import zipfile as z
from datetime import timedelta
from collections import OrderedDict
from pypodio2 import api
from string import Template

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
        self.pdETL, self.pdKey, self.pdApp3, self.pdApp2, self.pdUsr, self.pdPwd = self.__configPodio(home + '/.ssh/idrk.cfg')


## API config methods

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
            app3 = config.get('APIKey', 'ap3') # podio internal app id for projects; ap1 deprecated
            app2 = config.get('APIKey', 'ap2') # podio internal app id for consults
            usr = config.get('PodioUser', 'p_user') # podio username
            pwd = config.get('PodioUser', 'p_pass') # password
            return etl, key, app3, app2, usr, pwd
        except IOError:
            print ("File %s not found." % config)


## Generic extract method

    def __getFormData(self, surveyID):
        '''
        Pull PRF/CRF form data down from Qualtrics. From qualtrics_etl.
        Only requests responses that are less than one day old.
        Returns JSON object containing untransformed survey data.
        '''

        today = dt.datetime.today()
        yesterday = today - timedelta(days=1)
        date = "%d-%d-%d" % (yesterday.year, yesterday.month, yesterday.day) # Responses should be <=1 day old

        urlTemp = Template("https://dc-viawest.qualtrics.com:443/API/v1/surveys/${svid}/responseExports?apiToken=${tk}&fileType=JSON&startDate=${dt}+00:00:00")
        reqURL = urlTemp.substitute(svid=surveyID, tk=self.qtToken, dt=date)
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
            print "Survey %s timed out." % surveyID
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


## Project transform helper switches

    def __mapProjType(self, number):
        '''
        Helper method for translating project type between formats.
        '''
        typemap = {
            '1': "Repeat",
            '2': "Derivative",
            '3': "First Run"
        }
        return typemap.get(number)

    def __backoutProjType(self, projtype):
        '''
        Takes a project type and changes it to the correct number.
        '''
        typemap = {
            "Repeat": 2,
            "Derivative": 3,
            "First Run": 1
        }
        return typemap.get(projtype)


## Transform and load projects

    def __transformProjects(self, dataPRF):
        '''
        Transform PRF data from Qualtrics to Podio schema.
        Returns an array of dicts containing project data.
        '''
        projects = []
        if dataPRF is None:
            return None
        for rawProj in dataPRF['responses']:
            parsedProj = dict()
            parsedProj['project-name'] = "TBD_%s" % rawProj.pop('Q2')
            if parsedProj['project-name'] == "TBD_":
                continue # Reject this entry if no project name was provided

            parsedProj['course-offering-type.text'] = self.__mapProjType(rawProj.pop('Q9'))
            parsedProj['course-offering-type.id'] = self.__backoutProjType(parsedProj['course-offering-type.text'])
            parsedProj['current-status'] = "<p>[%s]: PRF submitted.<br/></p>" % rawProj.pop('EndDate')
            parsedProj['short-description'] = "<p><b>Name of Primary Contact:</b> %s<br/><br/><b>Primary Contact SUNet ID:</b> %s<br/><br/><b>Name/Title/SUNet ID of Project Lead(s)/Instructor(s):</b> %s</p>" % (rawProj.pop('Q3'), rawProj.pop('Q27'), rawProj.pop('Q6'))

            if (parsedProj['course-offering-type.text'] == 'Repeat'):
                parsedProj['derivative-of'] = rawProj.pop('Q12')
                parsedProj['audience-notes'] = rawProj.pop('Q29')
                parsedProj['short-description'] += "<p><b>Intended changes:</b> %s<br/><br/><b>Desired launch:</b> %s</p>" % (rawProj.pop('Q14'), rawProj.pop('Q13'))

            if (parsedProj['course-offering-type.text'] == 'Derivative' or parsedProj['course-offering-type.text'] == 'First Run'):
                parsedProj['audience-notes'] = rawProj.pop('Q35')
                parsedProj['short-description'] += "<p><b>Project description:</b> %s<br/><br/><b>Impact:</b> %s<br/><br/><b>Support needed:</b> %s<br/><br/><b>Research/evaluation plans:</b> %s<br/><br/><b>Schedule:</b> %s</p>" % (rawProj.pop('Q15'), rawProj.pop('Q16'), rawProj.pop('Q17'), rawProj.pop('Q20'), rawProj.pop('Q21'))
                parsedProj['funding-stipulations'] = rawProj.pop('Q36')
                parsedProj['consult'] = rawProj.pop('Q18', "No one")

            projects.append(parsedProj)
        return projects


    def __loadProjects(self, projects):
        '''
        Load transformed PRF data to Podio.
        Returns number of projects loaded.
        '''

        c = api.OAuthClient(self.pdETL, self.pdKey, self.pdUsr, self.pdPwd)
        status = 0

        for proj in projects:
            item = {
                    'fields':[
                        {
                         'external_id':'project-name',
                         'values':[
                            {'value': "%s" % proj.pop('project-name')}
                         ]
                        },
                        {
                         'external_id':'derivative-of',
                         'values':[
                            {'value': "%s" % proj.pop('derivative-of', 'n/a')}
                         ]
                        },
                        {
                         'external_id':'course-offering-type',
                         'values':[
                            {'value': proj.pop('course-offering-type.id')}
                         ]
                        },
                        {
                         'external_id':'overall-health',
                         'values':[
                            {'value': 17} # "New"
                         ]
                        },
                        {
                         'external_id':'quarter-offered',
                         'values':[
                            {'value': 17} # "TBD"
                         ]
                        },
                        {
                         'external_id':'platform',
                         'values':[
                            {'value': 14} # "TBD"
                         ]
                        },
                        {
                         'external_id':'school',
                         'values':[
                            {'value': 7} # "Other"
                         ]
                        },
                        {
                         'external_id':'course-type',
                         'values':[
                            {'value': 8} # "TBD"
                         ]
                        },
                        {
                         'external_id':'delivery-format',
                         'values':[
                            {'value': 16} # "TBD"
                         ]
                        },
                        {
                         'external_id':'current-status',
                         'values':[
                            {'value': "%s" % proj.pop('current-status')}
                         ]
                        },
                        {
                         'external_id':'audience-notes',
                         'values':[
                            {'value': "%s" % proj.pop('audience-notes')}
                         ]
                        },
                        {
                         'external_id':'short-description',
                         'values': [
                            {'value': "%s" % proj.pop('short-description')}
                         ]
                        },
                        {
                         'external_id':'for-future-reference',
                         'values': [
                            {'value': "Prior VPTL contacts, if any: %s" % proj.pop('consult', 'N/A')}
                         ]
                        },
                        {
                         'external_id':'funding-stipulations',
                         'values': [
                            {'value': "%s" % proj.pop('funding-stipulations', 'n/a')}
                         ]
                        }
                    ]
            }
            while tries < 20:
                try:
                    c.Item.create(int(self.pdApp3), item)
                except:
                    tries += 1
                    continue
                else:
                    print "API call failed, dumping JSON to log file."
                    print json.dumps(item)
            status += 1

        return status


## Transform and load consults

    def __transformConsults(self, dataCRF):
        '''
        Transform CRF data from Qualtrics to Podio schema.
        Returns an array of dicts containing consult data.
        '''
        consults = []
        if dataCRF is None:
            return None
        for rawCons in dataCRF['responses']:
            parsedCons = dict()
            parsedCons['contact-name'] = "%s" % rawCons.pop('Q10')
            parsedCons['title'] = "<p>%s</p>" % rawCons.pop('Q6')
            parsedCons['email'] = "%s" % rawCons.pop('Q13')
            parsedCons['school'] = "<p>%s</p>" % rawCons.pop('Q14')
            parsedCons['description'] = "<p>%s</p>" % rawCons.pop('Q8')
            parsedCons['link-to-crf'] = 'https://stanforduniversity.qualtrics.com/CP/Report.php?SID=SV_78KTbL61clEWsO9&R='+rawCons.pop('ResponseID')
            parsedCons['comments'] = "<p><b>Contact name:</b> %s<br/><br/><b>Contact SUNet ID:</b> %s" % (parsedCons['contact-name'], rawCons.pop('Q11'))
            consults.append(parsedCons)
        return consults


    def __loadConsults(self, consults):
        '''
        Load transformed CRF data to Podio.
        Returns number of projects loaded.
        '''

        c = api.OAuthClient(self.pdETL, self.pdKey, self.pdUsr, self.pdPwd)
        status = 0

        for cons in consults:
            item = {
                    'fields':[
                        {
                         'external_id':'contact',
                         'values':[
                            {'value': "%s" % cons.pop('contact-name')}
                         ]
                        },
                        {
                         'external_id':'what-is-the-title-of-your-project-or-course',
                         'values':[
                            {'value': "%s" % cons.pop('title')}
                         ]
                        },
                        {
                         'external_id':'what-is-the-school-department-and-program-if-relevant-t',
                         'values':[
                            {'value': "%s" % cons.pop('school')}
                         ]
                        },
                        {
                         'external_id':'what-would-you-like-to-discuss-during-this-consultation',
                         'values':[
                            {'value': "%s" % cons.pop('description')}
                         ]
                        },
                        {
                         'external_id':'status',
                         'values':[
                            {'value': 10} # New
                         ]
                        },
                        {
                         'external_id':'link-to-crf',
                         'values':[
                            {'url': "%s" % cons.pop('link-to-crf')}
                         ]
                        },
                        {
                         'external_id':'comments',
                         'values':[
                            {'value': "%s" % cons.pop('comments')}
                         ]
                        },
                        {
                         'external_id':'email',
                         'values':[
                            {'value': "%s" % cons.pop('email'),
                             'type': "work"}
                         ]
                        }
                    ]
                }
            while tries < 20:
                try:
                    c.Item.create(int(self.pdApp2), item)
                except:
                    tries += 1
                    continue
                else:
                    print "API call failed, dumping JSON to log file."
                    print json.dumps(item)
            status += 1

        return status

## Inspect item JSON

    def inspectItem(self, itemID):
        '''
        Pull item from Qualtrics and print JSON to stdout.
        '''
        c = api.OAuthClient(self.pdETL, self.pdKey, self.pdUsr, self.pdPwd)
        item = c.Item.find(itemID)
        print json.dumps(item, indent=4)


## User interface

    def extractTransformLoad(self):
        '''
        Pull form data down from Qualtrics, transform, and load to Podio.
        '''

        # Extract step
        idCRF = 'SV_78KTbL61clEWsO9'
        idPRF = 'SV_bftcKQJ9cGUyPI1'
        dataCRF = self.__getFormData(idCRF)
        dataPRF = self.__getFormData(idPRF)

        # Transform step
        consults = self.__transformConsults(dataCRF)
        projects = self.__transformProjects(dataPRF)

        # Load step
        if consults is not None:
            consStatus = self.__loadConsults(consults)
        else:
            consStatus = 0
        if projects is not None:
            projStatus = self.__loadProjects(projects)
        else:
            projStatus = 0

        return consStatus, projStatus



if __name__ == '__main__':
    pdm = ProjectDataManager()

    # Test transform method
    consults, projects = pdm.extractTransformLoad()
    print "Consults loaded: %d" % consults
    print "Projects loaded: %d" % projects
