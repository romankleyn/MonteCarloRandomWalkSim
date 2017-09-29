import pandas, numpy, sqlite3, json, urllib.request
import matplotlib.pyplot as mlp
from matplotlib.dates import date2num
from pandas.tseries.offsets import BDay
from time import time
from re import split
from tqdm import tqdm

class RandomWalk:
	def __init__(self, ticker = 'MSFT', interval = 'EOD', MAn = 90, DaysOut = 15, sims = 100, apiKey = 'demo'):
		#Start
		self.startTime = time()
		print('starting...')
		
		#Names
		self.ticker, self.interval, self.MAn, self.sims, self.DaysOut = ticker.upper(), interval, MAn, range(1, sims+1), range(1, DaysOut+1)
		self.avg, self.median,self.std = '{}avg'.format(self.MAn), '{}med'.format(self.MAn), '{}std'.format(self.MAn)
		self.simDic = {}
		self.apiKey = apiKey
		
		#Database Connection
		self.conn = sqlite3.connect('stock.db')
		print('connected to database')
		
		#Inital Run
		self.alphaVantage
		if type(None) == type(self.dataset):
			print('Simulation Incomplete: No Internet or Database Data')
			if apiKey == 'demo' and ticker != 'MSFT':
				print('Can only use MSFT ticker with "demo", check out www.alphavantage.co for an API key.')
				print('*not affiliated, fan of the free data.')
			return None
		else:
			pass
		
		self.tech
		self.sim
		self.plot

		
	@property
	def DBQuery(self):
		#Query DB
		print('querying data')
		try:
			self.dataset = pandas.read_sql_query("select * from {};".format(self.ticker.replace('.', '_')), self.conn)
			self.dataset = self.dataset.set_index('DATE')
			print('data retrieved')
		
		except:
			raise Exception('No Historic Data Exists')
			
	@property
	def DBMgmt(self):
		#Write to DB
		self.dataset.to_sql(self.ticker.replace('.', '_'), self.conn, if_exists='replace')
		print('data stored and updated')
		   
	@property
	def alphaVantage(self):
		print('attempting to download data')
		
		if self.interval in [1, 5, 15, 30, 60]:
			url = 'http://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={0}&interval={1}min&outputsize=full&apikey={2}'\
			.format(self.ticker, self.interval, self.apiKey)
			TimeSeries = str(self.interval)+'min'
		else: 
			url = 'http://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={0}&outputsize=full&apikey={1}'\
			.format(self.ticker, self.apiKey)
			TimeSeries = 'Daily'
			
		try:
			urlData = json.load(urllib.request.urlopen(url))['Time Series ({})'.format(TimeSeries)]
			dataset = pandas.DataFrame.from_dict(urlData, orient='index').rename(index=str, columns=\
					  {'1. open':'OPEN', '2. high':'HIGH', '3. low': 'LOW', '4. close':'CLOSE', '5. volume':'VOLUME'}).apply(pandas.to_numeric)
			
			dataset.index.name = 'DATE'
			self.dataset = dataset
			print('downloaded data')
			
			self.DBMgmt
			
		except urllib.request.URLError:
			print('FAILED data downloaded')
			print('using historic data if exists')
			pass
		
		self.DBQuery

	@property
	def tech(self): 
		self.dataset[self.avg] = self.dataset.CLOSE.rolling(self.MAn).mean()
		self.dataset[self.median] = self.dataset.CLOSE.rolling(self.MAn).median()
		self.dataset[self.std] = self.dataset.CLOSE.rolling(self.MAn).std()
		print('technical data calculated')
		
	@property
	def sim(self):
		LTime = time()
		print('beginning simulation, {} seconds after initation'.format(LTime - self.startTime))
		
		#Day
		data = self.dataset.tail(1)
		std = data[self.std]
		
		### Random
		nrand = numpy.random.uniform
		
		### Date List
		datelist = []
		
		for sim in tqdm(self.sims):
			simStart = time()
			workSet = data.copy().CLOSE
			
			for dayOut in self.DaysOut:
				daySet = workSet.tail(1)
				day = daySet.index.values.astype(str)
				ymd = split(' |T', day[0])[0].split('-')
				yr, mm, dd = int(ymd[0]), int(ymd[1]), int(ymd[2])
				date = pandas.datetime(yr, mm, dd)
				
				if date not in datelist:
					datelist.append(date)
				
				nDay = date + BDay(1)
				
				if nDay not in datelist:
					datelist.append(nDay)
					
				price = (daySet + nrand(-std,std))[0]
				workSet.loc[nDay] = price
			
			self.simDic[sim] = workSet
			
			simEnd = time()
			#print('simulation {}, {}'.format(sim, simEnd - simStart))
		
		self.datelist = datelist
		endTime = time()
		print('Monte Carlo Simulation of {} Complete in {} Seconds'.format(self.ticker, endTime-self.startTime))

	@property
	def plot(self):
		### History Plot
		history = self.dataset.CLOSE.tail(self.MAn)
		Hy = history.values
		Hx = [pandas.Timestamp(x) for x in history.index.values]
		mlp.plot(Hx, Hy)
		
		### Sim Plot
		x = self.datelist
		print('plotting...')
		for sim in tqdm(self.simDic):
			stime = time()
			y = self.simDic[sim].values
			mlp.plot(x, y)
			ftime = time()
		mlp.plot(x, self.path(y[0]), lw = 2.5, ls = '--', c="black", label='Average Path')
		path = mlp.plot(x, self.path(y[0]), lw = 2, ls = '--', c="red", label='Average Path')

		# Legend
		mlp.legend(path, ['Potential(mean) Path'])

		mlp.title('{} Monte Carlo Simulation ({} iterations)'.format(self.ticker, len(self.sims)))
		mlp.ylabel('Price for {} ($)'.format(self.ticker))
		mlp.xlabel('Last {} Days of Trading + {} Simulated Days'.format(self.MAn, len(self.DaysOut), self.ticker))
		mlp.show()
    
    #returns path
	def path(self, ip):
		l = []
		p = {0:ip}
		for day in list(self.DaysOut):
			for y in self.simDic:
				lp = self.simDic[y][day]
				l.append(lp)
			p[day] = sum(l)/len(l)
			l = []
		return [p[x] for x in p]

		

