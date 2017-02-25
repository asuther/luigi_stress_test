import numpy as np
import pandas as pd
import luigi
import os

date_folder = './data/date_files/'
item_folder = './data/item_files/'
id_folder = './data/id_files/'

class generateData(luigi.Task):
	ids_per_month = luigi.IntParameter(default=100)
	lines_per_item = luigi.IntParameter(default = 100)	
	start_date = luigi.DateParameter(default = pd.to_datetime('2015-04-01'))
	end_date = luigi.DateParameter(default = pd.to_datetime('2015-05-01'))

	def requires(self):
		current_date_range = pd.date_range(start = self.start_date, end = self.end_date, freq='M')	
	
		luigi.build([generateIDs(ids_per_month = self.ids_per_month, date = date) 
				for date in current_date_range], 
			local_scheduler=False)
		return [getSingleDate(date = current_date, lines_per_item = self.lines_per_item) for current_date in current_date_range]
	
	def output(self):
		return luigi.LocalTarget('./data/final_data_receipt.txt')
	
	def run(self):
		#with self.output().open('w') as fout:
		#	for input_date in self.input():
		#		with input_date.open('r') as file_in:
		#			for line in file_in.read():
		#				fout.write(line)a
		with self.output().open('w') as file_out:
			file_out.write('done')

class generateIDs(luigi.Task):
	date = luigi.DateParameter()
	ids_per_month = luigi.IntParameter()
	
	def requires(self):
		return []
	
	def output(self):
		return luigi.LocalTarget(id_folder + '/ids_{date}.csv'.format(date = self.date))
	
	def run(self):
		with self.output().open('w') as fout:
			for current_id in xrange(self.ids_per_month):
				fout.write(str(current_id) + "\n")


class getSingleDate(luigi.Task):
	date = luigi.DateParameter()	
	lines_per_item = luigi.IntParameter()

	def requires(self):
		# Get the app_ids for this date
		id_list = self.__get_ids_for_date(self.date)
		
		return [generate_id_date_data(date = self.date, 
									id_val = str(current_id), 
									lines_per_item = self.lines_per_item) for current_id in id_list] 

	def output(self):
		return luigi.LocalTarget(date_folder + '/{date}.csv'.format(date = self.date))

	def run(self):
		#full_date_data_list = []
		#for id_date_file in self.input():
	#		current_data = pd.read_csv(id_date_file.path, header=None)
	#		full_date_data_list.append(current_data)

	#	full_date_data = pd.concat(full_date_data_list, ignore_index=False)
		if not os.path.isdir(date_folder):
			os.mkdir(date_folder)
		pd.DataFrame([-1]).to_csv(self.output().path, index=False)

	def __get_ids_for_date(self, date):
		return pd.read_csv(id_folder + '/ids_{date}.csv'\
					.format(date = date), header=None)[0].tolist()

class generate_id_date_data(luigi.Task):
	date = luigi.DateParameter()
	id_val = luigi.Parameter()
	lines_per_item = luigi.IntParameter()

	def requires(self):
		return []

	def output(self):
		return luigi.LocalTarget(item_folder + '{id_val}_{date}.csv'\
				.format(id_val = self.id_val, date = self.date))

	def run(self):

		with self.output().open('w') as file_out:
			for random_value in np.random.randint(low = 0, high = 100, size=self.lines_per_item):
				file_out.write(str(random_value) + "\n") 




