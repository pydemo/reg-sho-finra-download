#!/usr/bin/python 
"""
Usage:
 cd /scripts/rest/
 python finra_download.py --url --settlement_date --show_url
 [-u] --url 			: HTTP endpoint
 [-d] --settlement_date : file date for download
 [-s] --show_url 		: just show URL containing for given date 	(False)
 [-w] --show_data 		: Print downloaded data to stdout 			(False)
 [-f] --date_format 	: Input date format 						("%d-%b-%Y", "20-Oct-2017")
 [-n] --download_url 	: Download URL prefix (http://otce.finra.org/)
 [-x] --delete_existing_out_file : Delete existing file before download.
 [-o] --out_file 		: Output file name							(out.csv)
 [-j] --job_name 		: Job name 									(finra_download)
 [-t] --log_timestamp 	: Timestamp
 [-r] --log_retention_days : Delete logs older than 				(3)
 [-L] --hide_log_output : Suppress stdout							(False)
  
"""
from __future__ import print_function
import os, time, sys
import urllib2 as urllib
import json
import logging
import pprint as pp
import atexit
from optparse import OptionParser
from pprint import pprint
import traceback
from datetime import datetime
#from dateutil import parser
import re
def formatExceptionInfo(maxTBlevel=5):
	cla, exc, trbk = sys.exc_info()
	excName = cla.__name__
	try:
		excArgs = exc.__dict__["args"]
	except KeyError:
		excArgs = "<no args>"
	excTb = traceback.format_tb(trbk, maxTBlevel)
	return (excName, excArgs, excTb)
e=sys.exit
DEFAULT_ENCODING = 'utf-8'
ERROR_EMPTY_DOC=99
def create_symlink(from_dir, to_dir):
	if (os.name == "posix"):
		os.symlink(from_dir, to_dir)
	elif (os.name == "nt"):
		os.system('mklink /J %s %s' % (to_dir, from_dir))
	else:
		log.error('Cannot create symlink. Unknown OS.', extra=d)
def unlink(dirname):
	if (os.name == "posix"):
		os.unlink(dirname)
	elif (os.name == "nt"):
		os.rmdir( dirname )
	else:
		log.error('Cannot unlink. Unknown OS.', extra=d)

JOB_NAME,_=os.path.splitext(os.path.basename(__file__))
assert JOB_NAME, 'Job name is not set'
#HOME= os.path.dirname(os.path.abspath(__file__))
HOME='/Bic/data/etb/data'
ts=time.strftime('%Y%m%d_%a_%H%M%S')
#ts=time.strftime('%Y%m%d_%a')
#dr=os.path.dirname(os.path.realpath(__file__))
dr='/Bic/log/etb'
#latest_dir =os.path.join(dr,'log',JOB_NAME,'latest')
ts_dir=os.path.join(dr,'log',JOB_NAME,ts)
config_home = os.path.join(HOME,'config')
latest_out_dir =os.path.join(HOME,'output',JOB_NAME,'latest')
ts_out_dir=os.path.join(HOME,'output',JOB_NAME,ts)
latest_dir =os.path.join(HOME,'log',JOB_NAME,'latest')
log_dir = os.path.join(HOME, 'log',JOB_NAME)
ts_dir=os.path.join(log_dir, ts)

done_file= os.path.join(ts_dir,'DONE.txt')
job_status_file=os.path.join(ts_dir,'%s.%s.status.py' % (os.path.splitext(__file__)[0],JOB_NAME))	
if not os.path.exists(ts_dir):
	os.makedirs(ts_dir)
if not os.path.exists(ts_out_dir):
	os.makedirs(ts_out_dir)

if  os.path.exists(latest_out_dir):
	unlink(latest_out_dir)
#os.symlink(ts_out_dir, latest_out_dir)
create_symlink(ts_out_dir, latest_out_dir)
if  os.path.exists(latest_dir):	
	unlink(latest_dir)
create_symlink(ts_dir, latest_dir)	

DEBUG=0
	
d = {'iteration': 0,'pid':os.getpid(), 'rows':0}
FORMAT = '|%(asctime)-15s|%(pid)-5s|%(iteration)-2s|%(rows)-9s|%(message)-s'
FORMAT = '|%(asctime)-15s%(pid)-5s|%(rows)-9s|%(name)s|%(levelname)s|%(message)s'
FORMAT = '|%(asctime)-15s%(pid)-5s|%(levelname)s|%(message)s'



logging.basicConfig(filename=os.path.join(ts_dir,'%s_%s.log' % (JOB_NAME,ts)),level=logging.INFO,format=FORMAT)
log = logging.getLogger(JOB_NAME)
log.setLevel(logging.DEBUG)



DEFAULT_URL 		= "http://otce.finra.org/RegSHO/Archives"
DEFAULT_DOWNLOAD_URL= 'http://otce.finra.org/'
DEFAULT_OUTPUT_FILE	= os.path.join(latest_dir,'out.csv')
DEFAULT_LOG_TIMESTAMP = ts
DEFAULT_LOG_RETENTION_DAYS	= 3
exit_status={}

def save_status():
	global job_status_file, exit_status
	if 1:
		p = pp.PrettyPrinter(indent=4)
		with open(job_status_file, "w") as py_file:			
			py_file.write('status=%s' % (p.pformat(exit_status)))
			#log.info (job_status_file, extra=d)
from HTMLParser import HTMLParser
class MyParser(HTMLParser):
    def __init__(self, output_list=None):
		HTMLParser.__init__(self)
		if output_list is None:
			self.output_list = {}
		else:
			self.output_list = output_list
		self.attrs=None
		self.tag=None
    def handle_starttag(self, tag, attrs):
		
		if tag == 'a':
			#print(dir(attrs))
			#print(dict(attrs))
			#self.output_list.append(dict(attrs).get('href'))			
			#e(0)
			self.tag=tag
			self.attrs=attrs
		else:
			self.tag=None
			self.attrs=None
			
    def handle_endtag(self, tag):
		self.tag=None
		self.attrs=None
		#if tag == 'a':
		#	print ("Encountered an end tag :", tag)
    def handle_data(self, data):
		if self.tag == 'a':
			#print ("Encountered some data  :", data)
			href=dict(self.attrs).get('href')
			if href.startswith('/RegSHO/DownloadFileStream?fileId='):
				sdate= data[10:18].strip()
				dto=datetime.strptime(sdate, '%Y%m%d')
				self.output_list[sdate]=[dto,href, data,dto.strftime(opt.date_format)]
				
				#e(0)
def url_read(url):
	try:
		urlResponse  = urllib.urlopen(url)
	except urllib.HTTPError, e:
		exit_status['HTTPError']=str(e.code)	
		log.error('HTTPError = ' + str(e.code))	
		raise e	
	except urllib.URLError, e:
		exit_status['URLError']=str(e.code)	
		log.error('URLError = ' + str(e.reason))
		raise e	
	except Exception , e:
		exit_status['Exception']=formatExceptionInfo()	
		log.error(formatExceptionInfo())
		raise e
	if hasattr(urlResponse.headers, 'get_content_charset'):
		encoding = urlResponse.headers.get_content_charset(DEFAULT_ENCODING)
	else:
		encoding = urlResponse.headers.getparam('charset') or DEFAULT_ENCODING
	#print (encoding)
	#e(0)
	return urlResponse.read()

	
EXIT_SUCCESS 	= 0
EXIT_FAILED 	=1
EXIT_MATCH_NOT_FOUND = 2
DEFAULT_SETTLEMENT_DATE_FORMAT ="%Y%m%d" #"20171020"
#DEFAULT_SETTLEMENT_DATE_FORMAT ="%d-%b-%Y" #"20-Oct-2017"
today=datetime.today().strftime(DEFAULT_SETTLEMENT_DATE_FORMAT) 				
if __name__ == "__main__":	
	##atexit.register(save_status)
	parser = OptionParser()
	parser.add_option("-u", "--url", dest="url", type=str, default=DEFAULT_URL)
	parser.add_option("-n", "--download_url", dest="download_url", type=str, default=DEFAULT_DOWNLOAD_URL)
	parser.add_option("-d", "--settlement_date", dest="settlement_date", type=str )
	parser.add_option("-f", "--date_format",  dest="date_format", type=str,  default=DEFAULT_SETTLEMENT_DATE_FORMAT, help="Format to parse settlement_date.")
	parser.add_option("-s", "--show_url",  action="store_true", dest="show_url", default=False, help="Just show URL (no download).")
	parser.add_option("-w", "--show_data",  action="store_true", dest="show_data", default=False, help="Show downloaded data (and save).")
	
	
	#maintenance
	parser.add_option("-x", "--delete_existing_out_file",  action="store_true", dest="delete_existing_out_file", default=True,
                  help="Delete existing out file before parse.")
	parser.add_option("-o", "--out_file", dest="out_file", type=str, default=DEFAULT_OUTPUT_FILE)
	parser.add_option("-j", "--job_name", dest="job_name", type=str, default=JOB_NAME)
	parser.add_option("-t", "--log_timestamp", dest="log_timestamp", type=str, default=DEFAULT_LOG_TIMESTAMP)
	parser.add_option("-r", "--log_retention_days", dest="log_retention_days", type=str, default=DEFAULT_LOG_RETENTION_DAYS)
	parser.add_option("-L", "--hide_log_output",  action="store_true", dest="hide_log_output", default=False, help="Suppress terminal log messages.")
	
 
  
	
	(opt, args) = parser.parse_args()
	if not opt.hide_log_output:
		ch = logging.StreamHandler(sys.stdout)
		ch.setLevel(logging.DEBUG)
		formatter = logging.Formatter(FORMAT)
		ch.setFormatter(formatter)
		log.addHandler(ch)

	#log cleanup
	if float(opt.log_retention_days):
		
		cmd ='find {log_dir}/* -type d -ctime {log_retention_days} | xargs rm -rf'.format(log_dir=log_dir,log_retention_days=opt.log_retention_days)
		exit_status['delete_log_retention'] = cmd
		status=os.system(cmd)
		assert not status, 'Log deletion failed with status {status}.'.format(status=status)

	data = url_read(opt.url)
	#print(len(data))
	log.info('Page size: %s B' % len(data), extra=d)
	#urls = re.findall('href="([a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', data)
	

	urls=re.findall('<a href="?\'?([^"\'>]*)', data)
	p = MyParser()
	p.feed(data)



	if not opt.settlement_date:
		log.info('Settlement date is not set. Exiting.', extra=d)
		for dt in sorted(p.output_list):
			print(dt, end=', ')
			
		print('\n\nToday: %s\n' % today)
		e(EXIT_FAILED)

	match=None
	for dt, v in p.output_list.items(): #'20170111 01-11-17'
		#print(v[3].upper() , opt.settlement_date.upper())
		if v[3].upper() == opt.settlement_date.upper():
			log.info('Match found: %s/%s' %(dt, v[3]), extra=d)
			match=v
	if not match:
		log.error('Match not found for settlement date: %s' %(opt.settlement_date.upper()), extra=d)
		e(EXIT_MATCH_NOT_FOUND)
	if opt.show_url:
		log.info('Show URL. Exiting.', extra=d)
		url=opt.download_url.rstrip('/')+'/'+match[1].lstrip('/')
		print(url)
		e(EXIT_SUCCESS)	
	#print(match)
	#e(0)		
	url=opt.download_url.rstrip('/')+'/'+match[1].lstrip('/')
	#print(url)
	settlement_data = url_read(url)
	if opt.show_data:
		log.info('Downloaded data:\n\n\n', extra=d)
		print (settlement_data)
	#e(EXIT_SUCCESS)
	#pprint(urls)
	#  <li><a href="/RegSHO/DownloadFileStream?fileId=560" target="_blank">otc-thresh20170118 01-18-17 11:00 PM</a></li>  
	#print (data)
	if 1:
		if opt.delete_existing_out_file and os.path.isfile(opt.out_file):
			os.remove(opt.out_file)
			log.info('Existing file removed.', extra=d)

		if settlement_data:
		
			with open(opt.out_file, "wb") as fh:
				fh.write(settlement_data)
			log.info('Settlement data is saved to:\n%s' % opt.out_file, extra=d)
			
		else:
			exit_status['Error']='Downloaded document is empty.'	
			log.error(exit_status['Error'], extra=d)
			e(ERROR_EMPTY_DOC)
	
	e(EXIT_SUCCESS)
