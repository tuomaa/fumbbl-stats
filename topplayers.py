#!/usr/bin/env python

import xml.etree.ElementTree as ET
import sqlite3
import urllib


class TopPlayers:
	def __init__(self):

		# Init the database connection
		self.conn = sqlite3.connect(':memory:')
		self.cur = self.conn.cursor()
		self.ex = self.cur.execute

		self.initDatabase()

		# Data related variables
		self.matchList = None

		# URL TEMPLATES
		# set :tournamentId to get matches for the tournament
		self.urlTemplateTournament = 'http://fumbbl.com/xml:group?id=%(tournamentId)s&op=matches'
		# set :groupId to get matches for the group
		self.urlTemplateGroup = 'http://fumbbl.com/xml:group?id=%(groupId)s&op=matches'

	def initDatabase(self):
		# Create the player table
		createPlayersTableSql = 'CREATE TABLE players (id TEXT, teamid TEXT, spp INT, completions INT, touchdowns INT, interceptions INT, casualties INT, mvps INT, passing INT, rushing INT, blocks INT, fouls INT, turns INT)'
		self.ex(createPlayersTableSql)

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
			self.loopMatches()

	def getGroupMatchData(self, groups):
		for group in groups:
			reqUrl = self.urlTemplateGroup % {'groupId': group}
			#print "Url to fetch:", reqUrl
			print "Fetching matches for Group", group

			matchXml = self.fetchUrl(reqUrl)
			
			root = ET.fromstring(matchXml)
			self.matchList = root.findall(".//matches/match")
			# process the match data
			self.loopMatches()

	def fetchUrl(self, url):
		u = urllib.urlopen(url)
		buf = u.read()
		u.close()
		return buf

	def loopMatches(self):
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
			#	player
			playerId = perf.get('player')
			#	completions
			completions = perf.get('completions')
			#	touchdowns
			touchdowns = perf.get('touchdowns')
			#	interceptions
			interceptions = perf.get('interceptions')
			#	casualties
			casualties = perf.get('casualties')
			#	mvps
			mvps = perf.get('mvps')
			#	passing
			passing = perf.get('passing')
			#	rushing
			rushing = perf.get('rushing')
			#	blocks
			blocks = perf.get('blocks')
			#	fouls
			fouls = perf.get('fouls')
			#	turns
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
	

if __name__ == '__main__':

	# create the TopPlayers
	ts = TopPlayers()
	#ts.readMatchData()
	#ts.loopMatches()
	
	# Get data for groups
	#groupsToGet = ('8000', '8001')
	#ts.getGroupMatchData(groupsToGet)

