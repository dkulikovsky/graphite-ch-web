import re
import ConfigParser

import time
import urllib
import requests
import itertools
import traceback

from django.conf import settings
from graphite.logger import log

try:
	from graphite_api.intervals import Interval, IntervalSet
	from graphite_api.node import LeafNode, BranchNode
except ImportError:
	from graphite.intervals import Interval, IntervalSet
	from graphite.node import LeafNode, BranchNode

class ClickHouseFinder(object):
	braces_re = re.compile('({[^{},]*,[^{}]*})')

	def _expand_braces_part(self, part):
		match = self.braces_re.search(part)
		if not match:
			return [part]

		result = set()

		startPos, endPos = match.span(1)
		for item in match.group(1).strip('{}').split(','):
			result.update(self._expand_braces_part(part[:startPos] + item + part[endPos:]))

		return list(result)

	def expand_braces(self, query):
		parts = query.split('.')
		for (index, part) in enumerate(parts):
			parts[index] = self._expand_braces_part(part)

		result = set(['.'.join(p) for p in itertools.product(*parts)])
		return list(result)

	def find_nodes(self, query, reqkey):
		metricsearch = getattr(settings, 'METRICSEARCH', '127.0.0.1')

		queries = self.expand_braces(query.pattern)

		result = []
		for query in queries:
			request = requests.get('http://%s:7000/search?%s' % (metricsearch, urllib.urlencode({'query': query})))
			request.raise_for_status()

			result += request.text.split('\n')

		for metric in result:
			if not metric:
				continue

			if metric.endswith('.'):
				yield BranchNode(metric[:-1])
			else:
				yield LeafNode(metric, ClickHouseReader(metric, reqkey))

class ClickHouseReader(object):
	__slots__ = ('path', 'nodes', 'reqkey')

	def __init__(self, path, reqkey = ''):
		self.nodes = [self]
		self.path  = None

		if hasattr(path, '__iter__'):
			self.nodes = path
		else:
			self.path  = path

		self.reqkey = reqkey

		if not hasattr(self, 'schema'):
			self.load_storage_schema()

	def load_storage_schema(self):
		config = ConfigParser.ConfigParser()
		try:
			configFile = getattr(settings, 'GRAPHITE_SCHEMA', '/etc/cacher/storage_schema.ini')
			config.read(configFile)
		except Exception, e:
			log.info('Failed to read storage_schema file %s: %s' % (configFile, e))
			return

		if not config.sections():
			log.info('Corrupted storage_schema file %s' % configFile)
			return

		schema  = {}
		periods = []
		for section in config.sections():
			if section == 'main':
				periods = [ int(x.strip()) for x in config.get('main', 'periods').split(',') ]
				continue

			schema.setdefault(section, {})
			schema[section]['pattern'] = re.compile(config.get(section, 'pattern'))
			schema[section]['retentions'] = [ int(x.strip()) for x in config.get(section, 'retentions').split(',') ]

		ClickHouseReader.schema  = schema
		ClickHouseReader.periods = periods

	def get_intervals(self):
		return IntervalSet([Interval(0, int(time.time()))])

	def fetch(self, startTime, endTime):
		(step, aggregate) = self.get_step(startTime, endTime)

		startTime -= startTime % step
		endTime   -= endTime % step

		log.info('DEBUG:clickhouse_range:[%s] start = %s, end = %s, step = %s' % (self.reqkey, startTime, endTime, step))

		withPath = self.path is None

		query = self.get_query(startTime, endTime, step, aggregate, withPath)
		log.info('DEBUG:clickhouse_query:[%s] query = %s' % (self.reqkey, query))

		profilingTime = {
			'start': time.time()
		}

		result = []
		binddict = {} # A dictionary to access per-node data in result table without searching.
		timeInfo = (startTime, endTime, step)

		# First we initialize array with Nones
		for node in self.nodes:
			inner = [ None for ts in xrange(startTime, endTime + 1, step) ]
			binddict[node.path] = inner
			result.append((
				node,
				(
					timeInfo,
					inner
				)
			))

		profilingTime['init'] = time.time()

		try:
			# With stream=True data will be parsed while being downloaded.
			request = requests.post("http://%s:8123" % ''.join(getattr(settings, 'CLICKHOUSE_SERVER', ['127.0.0.1'])), query, stream=True)
			request.raise_for_status()
		except Exception as e:
			log.info("Failed to fetch data, got exception:\n %s" % traceback.format_exc())
			return []

		profilingTime['fetch'] = time.time()
		
		if withPath:
			oldkey = None
			# Read data in chunks of 128KiB.
			for line in request.iter_lines(chunk_size=131072):
				if not line:
					continue
				line = line.split('\t')
				# Records received from database are sortedby path.
				# That means that only when the path changes it is necessary to perform lookup on binddict.
				if oldkey != line[0]:
					oldkey = line[0]
					bindinner = binddict[line[0]]
				bindinner[ (int(line[1])-startTime) / step ] = float(line[2])
		else:
			bindinner = binddict[self.path]
			# Read data in chunks of 128KiB.
			for line in request.iter_lines(chunk_size=131072):
				if not line:
					continue
				line = line.split('\t')
				bindinner[ (int(line[0])-startTime) / step ] = float(line[1])

		profilingTime['parse'] = time.time()

		log.info('DEBUG:clickhouse_time:[%s] init = %.2f, fetch = %.2f, parse = %.2f' % (
			self.reqkey,
			profilingTime['init'] - profilingTime['start'],
			profilingTime['fetch'] - profilingTime['init'],
			profilingTime['parse'] - profilingTime['fetch'],
		))

		if self.path:
			return result[0][1]

		return result

	def get_step(self, startTime, endTime):
		step = 0
		aggregate = 0

		if not hasattr(self, 'schema'):
			return (step, aggregate)

		for node in self.nodes:
			for schema in self.schema.itervalues():
				if not schema['pattern'].search(node.path):
					continue

				delta = 0
				for (index, retention) in enumerate(schema['retentions']):
					# ugly month average 365/12 ~= 30.5
					# TODO: fix to real delta
					delta += int(self.periods[index]) * 30.5 * 86400
					if startTime > (time.time() - delta):
						step      = max(step, retention)
						aggregate = max(aggregate, index)
						break

				if aggregate > 0:
					aggregate = 1
				elif startTime < (time.time() - delta):
					retention = schema['retentions'][-1]
					step      = max(step, retention)
					aggregate = max(aggregate, 1)

				break
		if not step:
			step = 60

		return (step, aggregate)

	def get_query(self, startTime, endTime, step, aggregate, withPath):
		paths = [node.path.replace('\'', '\\\'') for node in self.nodes]
		paths = ["'%s'" % path for path in paths]

		if len(paths) > 1:
			pathExpr = 'Path IN ( %s )' % ', '.join(paths)
		else:
			pathExpr = 'Path = %s' % paths[0]

		args = {
			'table': getattr(settings, 'GRAPHITE_TABLE', 'default.graphite_d'),
			'paths': pathExpr,
			'from':  startTime,
			'until': endTime,
			'step':  step,
		}

		if aggregate:
			args['table'] = """(SELECT Path, Time, Date, argMax(Value, Timestamp) as Value FROM {table}
					WHERE {paths}
					AND Time >= {from} AND Time <= {until}
					AND Date >= toDate(toDateTime({from})) AND Date <= toDate(toDateTime({until}))
					GROUP BY Path, Time, Date)""".format(**args)

		args['fields'] = ''
		if withPath:
			args['fields'] += 'anyLast(Path), '
		if aggregate:
			args['fields'] += 'kvantT, avg(Value)'
		else:
			args['fields'] += 'kvantT, argMax(Value, Timestamp)'

		# Having results sorted by path allows optimized access to returned list.
		return """SELECT {fields} FROM {table}
				WHERE {paths}
				AND kvantT >= {from} AND kvantT <= {until}
				AND Date >= toDate(toDateTime({from})) AND Date <= toDate(toDateTime({until}))
				GROUP BY Path, intDiv(toUInt32(Time), {step}) * {step} as kvantT
				ORDER BY Path
				""".format(**args)

import graphite.readers
graphite.readers.MultiReader = ClickHouseReader
