#!/usr/bin/env python

import xml.etree.ElementTree as ET
import sqlite3
import urllib


class TopPlayers:
    def __init__(self):

        # Init variables etc.
        ####

        # URL TEMPLATES
        # set :tournamentId to get matches for the tournament
        self.urlTemplateTournament = 'http://fumbbl.com/xml:group?id=%(tournamentId)s&op=matches'
        # set :groupId to get matches for the group
        self.urlTemplateGroup = 'http://fumbbl.com/xml:group?id=%(groupId)s&op=matches'
        # set :teamId
        self.urlTemplateTeam = 'http://fumbbl.com/xml:team?id=%(teamId)s&past=1'

        # Define the SQL statements here
        self.createPlayersTableSql = 'CREATE TABLE players (id TEXT, teamid TEXT, spp INT, completions INT, touchdowns INT, interceptions INT, casualties INT, mvps INT, passing INT, rushing INT, blocks INT, fouls INT, turns INT)'
        self.createCalcStatsTableSql = 'CREATE TABLE calculatedStats(player TEXT, team TEXT, scoringThrower INT, blockingScorer INT, blockingThrower INT, triple INT, allRounder INT)'

        # Data related variables
        self.matchList = None


        # Init the database, etc.
        ####

        # Init the database connection
        self.conn = sqlite3.connect(':memory:')
        self.cur = self.conn.cursor()
        self.ex = self.cur.execute

        self.initDatabase()


    def initDatabase(self):
        # Create the player table
        self.ex(self.createPlayersTableSql)
        
        # Create table for the calculated statistics
        self.ex(self.createCalcStatsTableSql)


    def resetDatabase(self):
        self.ex('DROP TABLE players')
        self.initDatabase()
        

    def readMatchData(self):
        # Read the matches xml, in testing phase from the file
        mats = ET.parse('matsit.xml')
        root = mats.getroot()
        self.matchList = root.findall(".//matches/match")

    def getTournamentMatchData(self, tourneys):
        for tourney in tourneys:
            reqUrl = self.urlTemplateTournament % {'tournamentId': tourney}
            #print "Url to fetch:", reqUrl
            print "Fetching matches for Tournament", tourney

            matchXml = self.fetchUrl(reqUrl)
            root = ET.fromstring(matchXml)
            self.matchList = root.findall(".//matches/match")
            # process the match data
            self.processMatches()

    def getGroupMatchData(self, groups):
        for group in groups:
            reqUrl = self.urlTemplateGroup % {'groupId': group}
            #print "Url to fetch:", reqUrl
            print "Fetching matches for Group", group

            matchXml = self.fetchUrl(reqUrl)
            
            root = ET.fromstring(matchXml)
            self.matchList = root.findall(".//matches/match")
            # process the match data
            self.processMatches()

    def getTeamData(self, teamId):
        # first fetch the team info from FUMBBL API
        reqUrl = self.urlTemplateTeam % {'teamId': teamId}
        teamXml = self.fetchUrl(reqUrl)

        # parse the xml
        root = ET.fromstring(teamXml)
        return root
        

    def fetchUrl(self, url):
        u = urllib.urlopen(url)
        buf = u.read()
        u.close()
        return buf

    def processMatches(self):
        totalMatches = len(self.matchList)
        currentMatch = 0
        for match in self.matchList:
            currentMatch += 1
            print "Handling match", currentMatch, "of", totalMatches
            self.recordMatchPerformances(match)


    def recordMatchPerformances(self, match):
        # Handle the home team
        teamId = match.find('home').get('id')
        performanceList = match.findall(".//home/performances/performance")
        self.recordPerformances(teamId, performanceList)
        # Handle the away team
        teamId = match.find('away').get('id')
        performanceList = match.findall(".//away/performances/performance")
        self.recordPerformances(teamId, performanceList)


    def recordPerformances(self, teamId, performanceList):
        for perf in performanceList:
            # Get the information from the performance
            #   player
            playerId = perf.get('player')
            #   completions
            completions = perf.get('completions')
            #   touchdowns
            touchdowns = perf.get('touchdowns')
            #   interceptions
            interceptions = perf.get('interceptions')
            #   casualties
            casualties = perf.get('casualties')
            #   mvps
            mvps = perf.get('mvps')
            #   passing
            passing = perf.get('passing')
            #   rushing
            rushing = perf.get('rushing')
            #   blocks
            blocks = perf.get('blocks')
            #   fouls
            fouls = perf.get('fouls')
            #   turns
            turns = perf.get('turns')
    
            # calculate spp
            spp = 5 * int(mvps) + 3 * int(touchdowns) + 2 * int(casualties) + 2 * int(interceptions) + 1 * int(completions)


            # construct the dict
            playerDict = { 'id': playerId,
                        'teamid': teamId,
                        'spp': spp,
                        'completions': int(completions),
                        'touchdowns': int(touchdowns),
                        'interceptions': int(interceptions),
                        'casualties': int(casualties),
                        'mvps': int(mvps),
                        'passing': int(passing),
                        'rushing': int(rushing),
                        'blocks': int(blocks),
                        'fouls': int(fouls),
                        'turns': int(turns) }
    
            # SQL to insert/update the data
            insertPlayerSql = 'INSERT INTO players values( :id, :teamid, :spp, :completions, :touchdowns, :interceptions, :casualties, :mvps, :passing, :rushing, :blocks, :fouls, :turns )'
            updatePlayerSql = 'UPDATE players SET spp=spp+:spp, completions=completions+:completions, touchdowns=touchdowns+:touchdowns, interceptions=interceptions+:interceptions, casualties=casualties+:casualties, mvps=mvps+:mvps, passing=passing+:passing, rushing=rushing+:rushing, blocks=blocks+:blocks, fouls=fouls+:fouls, turns=turns+:turns WHERE id=:id'

            # Check if the player is already added to the records
            v = self.ex('SELECT 1 FROM players WHERE id=:id', { "id":playerDict["id"] } ).fetchall()
            if len(v) == 0:
                # insert new row
                self.ex(insertPlayerSql, playerDict)
            else:
                #  add to the existing info
                self.ex(updatePlayerSql, playerDict)

    def calculateSpecialStats(self):
        # get list of the player performance tuples
        selectPlayersSql = 'SELECT * from players'
        playerList = self.ex(selectPlayersSql).fetchall()

        # for each player
        for player in playerList:

            # get the relevant numbers
            completions = player[3]
            touchdowns = player[4]
            intercepts = player[5]
            casualties = player[6]

            # calculate the statistics
            scoringThrower = min(touchdowns, completions)
            blockingScorer = min(casualties, touchdowns)
            blockingThrower = min(casualties, completions)
            triple = min(completions, touchdowns, casualties)
            allRounder = min(completions, touchdowns, casualties, intercepts)

            # construct the dict
            statsDict = { 'player': player[0],
                          'team': player[1],
                          'scoringThrower': scoringThrower,
                          'blockingScorer': blockingScorer,
                          'blockingThrower': blockingThrower,
                          'triple': triple,
                          'allRounder': allRounder }

            # insert the stats to the table
            insertPlayerStatsSql = 'INSERT INTO calculatedStats values( :player, :team, :scoringThrower, :blockingScorer, :blockingThrower, :triple, :allRounder )'
            self.ex(insertPlayerStatsSql, statsDict)

    def resetSpecialStats(self):
        # drop the calculated stats table
        self.ex('DROP TABLE calculatedStats')
        # and re-create it
        self.ex(self.createCalcStatsTableSql)

    def getTopSpecialStats(self):
        selectTopScoringThrowerSql = 'SELECT * FROM calculatedStats WHERE scoringThrower = (SELECT MAX(scoringThrower) FROM calculatedStats)'
        selectTopBlockingThrowerSql = 'SELECT * FROM calculatedStats WHERE blockingThrower = (SELECT MAX(blockingThrower) FROM calculatedStats)'
        selectTopBlockingScorerSql = 'SELECT * FROM calculatedStats WHERE blockingScorer = (SELECT MAX(blockingScorer) FROM calculatedStats)'
        selectTopTripleSql = 'SELECT * FROM calculatedStats WHERE triple = (SELECT MAX(triple) FROM calculatedStats)'
        selectTopAllRounderSql = 'SELECT * FROM calculatedStats WHERE allRounder = (SELECT MAX(allRounder) FROM calculatedStats)'

        topScoringThrower = self.ex(selectTopScoringThrowerSql).fetchall()
        topBlockingThrower = self.ex(selectTopBlockingThrowerSql).fetchall()
        topBlockingScorer = self.ex(selectTopBlockingScorerSql).fetchall()
        topTriple = self.ex(selectTopTripleSql).fetchall()
        topAllRounder = self.ex(selectTopAllRounderSql).fetchall()

        for player in topScoringThrower:
            (name, team) = self.getPlayerTeamNames(player[0], player[1])
            topValue = player[2]
            print "Top Scoring Thrower:", name, "(", team, ") ", "#", topValue

        for player in topBlockingScorer:
            (name, team) = self.getPlayerTeamNames(player[0], player[1])
            topValue = player[3]
            print "Top Blocking Scorer:", name, "(", team, ") ", "#", topValue

        for player in topBlockingThrower:
            (name, team) = self.getPlayerTeamNames(player[0], player[1])
            topValue = player[4]
            print "Top Blocking Thrower:", name, "(", team, ") ", "#", topValue

        for player in topTriple:
            (name, team) = self.getPlayerTeamNames(player[0], player[1])
            topValue = player[5]
            print "Top Triple:", name, "(", team, ") ", "#", topValue

        for player in topAllRounder:
            (name, team) = self.getPlayerTeamNames(player[0], player[1])
            topValue = player[6]
            print "Top All-Rounder:", name, "(", team, ") ", "#", topValue
            
        #topThreeSql = 'SELECT * FROM calculatedStats WHERE triple >= (SELECT MIN(triple) FROM (SELECT triple FROM calculatedStats ORDER BY triple DESC LIMIT 3)) ORDER BY triple DESC'
        #topThree = self.ex(topThreeSql).fetchall()
        #for player in topThree:
        #    (name, team) = self.getPlayerTeamNames(player[0], player[1])
        #    val = player[5]
        #    print "Top3 Triple:", name, "(", team, ") ", "#", val

    def getPlayerTeamNames(self, playerId, teamId):
        # get the team xml
        teamTree = self.getTeamData(teamId)
        # get the team name
        teamName = teamTree.find('name').text
        # search for the player name
        playerNameXPath = './/player[@id="%(playerId)s"]/name' % {'playerId': playerId}
        playerName = teamTree.findall(playerNameXPath)[0].text

        return (playerName, teamName)

    def getPlayerUrl(self, playerId):
        return 'http://fumbbl.com/FUMBBL.php?page=player&player_id=' + playerId

    def getTeamUrl(self, teamId):
        return 'http://fumbbl.com/FUMBBL.php?page=team&op=view&team_id=' + teamId

    def printTopList(self):
        """ in development, assumes all data has been fetched
        """


if __name__ == '__main__':

    # create the TopPlayers
    ts = TopPlayers()
    #ts.readMatchData()
    #ts.processMatches()
    
    # Get data for groups
    groupsToGet = ('8011', '8341')
    ts.getGroupMatchData(groupsToGet)

    # calculate the special stats
    ts.calculateSpecialStats()

    # print out the top specials
    ts.getTopSpecialStats()


