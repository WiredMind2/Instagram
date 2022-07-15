import uuid
from dateutil.parser import isoparse
import os
import pickle
import queue
from ssl import SSLError
import threading
import time
from urllib.parse import urlparse
from more_itertools import first

from tkinter import *

import requests
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

import secret

FILE_ROOT = os.path.abspath('./insta_data')
COOKIE_FILE = os.path.join(FILE_ROOT, 'cookies.pkl')

class XpathFinder():
	def __init__(self):
		executable_path = r'C:\Program Files\geckodriver\geckodriver.exe'
		opts = webdriver.FirefoxOptions()
		# opts.headless = True
		self.driver = webdriver.Firefox(executable_path=executable_path,options=opts)

		self.downloader = Downloader()

	def start_ui(self):
		def start_cmd(cmd):
			threading.Thread(target=cmd).start()

		commands = {
			'Main page': self.main_page,
			'Get stories': self.get_stories,
			'Get posts': self.get_posts,
			'Get followers': self.get_followers,
			'Quit': self.close
		}

		self.start()
		
		self.root = Tk()
		columns = 2

		cmd_frame = Frame(self.root)
		for i in range(columns):
			cmd_frame.grid_columnconfigure(i)

		for i, (name, cmd) in enumerate(commands.items()):
			Button(cmd_frame, text=name, command=lambda cmd=cmd: start_cmd(cmd)).grid(row=i//columns, column=i%columns, sticky="ew")
		cmd_frame.pack()

		self.root.mainloop()

	def start(self):
		self.main_page()

		self.init_cookies()

		if self.exists('//form[@id="loginForm"]', wait=False):
			self.login()
			self.save_user()

		self.use_notifs()

		# self.get_stories()

		# self.main_page()
		# self.get_followers()

		# input('click on first img')
		# self.get_posts()
		
		self.downloader.que.join()

	def close(self):
		self.driver.close()

	def main_page(self):
		self.driver.get('https://www.instagram.com/')

	def init_cookies(self):
		if os.path.exists(COOKIE_FILE):
			cookies = pickle.load(open(COOKIE_FILE, "rb"))
			for cookie in cookies:
				self.driver.add_cookie(cookie)
			self.driver.get('https://www.instagram.com')
			return True
		return False

	def login(self):
		if self.exists('/html/body/div[4]/div/div/button[2]'):
			self.xPth('/html/body/div[4]/div/div/button[2]').click() # Cookies button

		login_field = '//input[@name="username"]'
		login_field = self.wait(login_field)
		login_field.send_keys(secret.USERNAME)
		
		pwd_field = '//input[@name="password"]'
		pwd_field = self.wait(pwd_field)
		pwd_field.send_keys(secret.PASSWORD)
		
		validate_xpth = '//button[@type="submit"]'
		loop = True
		while loop:
			validate = self.wait(validate_xpth)
			try:
				validate.click()
			except:
				pass
			else:
				loop = False

	def save_user(self):
		self.wait('/html/body/div[1]/section/main/div/div/div/section')
		if self.exists('/html/body/div[1]/section/main/div/div'):
			self.xPth('/html/body/div[1]/section/main/div/div/div/div/button').click() # Refuse
			time.sleep(1)
		pickle.dump( self.driver.get_cookies() , open(COOKIE_FILE,"wb"))

	def use_notifs(self):
		self.wait('/html/body/div[@role="presentation"]/div/div/div') # Wait for panel
		self.driver.find_element(By.XPATH, "//button[contains(text(), 'Plus tard')]").click()

	def get_stories(self):
		self.xPth('/html/body/div[1]/section/main/section/div[1]/div[2]/div/div/div/div/ul/li[3]/div/button').click() # Click on first story

		# Wait for stuff to load
		self.wait('/html/body/div[1]/section/div[1]/div/div[5]')
		
		while True:
			# Create directory
			url = self.driver.current_url
			parsed = urlparse(url)
			urlpath = os.path.normpath(parsed.path)
			path = FILE_ROOT + urlpath
			path = os.path.normpath(path)
			# path = os.path.join(FILE_ROOT, urlpath)
			self.downloader.create_dir(path)

			# Get image
			img_path = os.path.join(path, 'img.jpg')
			if not os.path.exists(img_path):
				try:
					img = self.xPth('/html/body/div[1]/section/div[1]/div/div[5]/section/div/div[1]/div/div/img')
				except TimeoutException:
					# No more stories
					break
				srcset = img.get_attribute("srcset")
				srcset = list(map(lambda e: e.split(), srcset.split(',')))
				if len(srcset) >= 1 and len(srcset[0]) == 2:
					img_url, size = max(srcset, key=lambda e: int(e[1][:-1]) if len(e) == 2 else 0)

					self.downloader.ddl(img_url, img_path)

			# Get video - if exists
			vid_path = os.path.join(path, 'video.mp4')
			if not os.path.exists(vid_path):
				vid = '/html/body/div[1]/section/div[1]/div/div[5]/section/div/div[1]/div/div/video/source'
				if self.exists(vid, wait=False):
					vid = self.xPth(vid, wait=False)
					vid_url = vid.get_attribute('src')

					self.downloader.ddl(vid_url, vid_path)

			# Open next story
			buts = self.driver.find_elements(By.XPATH, '/html/body/div[1]/section/div[1]/div/div[5]/section/div/button')
			next_but = buts[-1]
			next_but.click() # Next story
			self.downloader.que.join()

	def get_posts(self):
		first_img = True
		while True:
			# Create directory
			username = self.xPth('//article//header/div[2]//a').text
			timestamp = self.xPth('//article//a/div/time[@datetime]').get_attribute('datetime')
			timestamp = isoparse(timestamp).strftime("%Y_%m_%d-%I_%M_%S_%p")

			path = os.path.join(FILE_ROOT, 'posts', username, timestamp)

			for elt in self.driver.find_elements(By.XPATH, '//body/div[@role="presentation"]//article[@role="presentation"]//ul/li//*[@src]'):
				if elt.tag_name == 'video':
					filename = os.path.join(path, 'img.mp4')
				elif elt.tag_name == 'img':
					filename = os.path.join(path, 'img.jpg')

				url = elt.get_attribute('src')
				if 'blob' in url:
					pass
				print(filename)
				self.downloader.ddl(url, filename)

			# Open next post
			buts = self.driver.find_elements(By.XPATH, "/html/body/div[6]/div[2]/div/div/button")
			if len(buts) == 2:
				first_img = False
			elif len(buts) <= 1 and not first_img:
				break
			
			next_but = buts[-1]
			next_but.click() # Next story
			self.downloader.que.join()

	def get_followers(self):
		self.xPth('/html/body/div[1]/section/nav/div[2]/div/div/div[3]/div/div[6]/span/img').click() # User menu thingy
		time.sleep(1)
		self.xPth('/html/body/div[1]/section/nav/div[2]/div/div/div[3]/div/div[6]/div[2]/div[2]/div[2]/a[1]/div/div[2]/div/div/div/div', wait=True).click() # Profile

		time.sleep(2)
		self.xPth('/html/body/div[1]/section/main/div/header/section/ul/li[3]/a/div', wait=True).click() # Following
		table = self.xPth('/html/body/div[6]/div/div/div/div[3]/ul/div') # Users table
		data = self.parse_user_table(table)

	def parse_user_table(self, table):
		# Scroll all the way down
		data = []
		parsed = set()
		looping = True
		all_parsed = 0
		while looping:
			for row in table.find_elements(By.TAG_NAME, 'li'):
				if row.id in parsed:
					continue
				try:
					self.driver.execute_script("arguments[0].scrollIntoView();", row )
				except:
					# The element disappeared
					pass

				all_parsed = 0

				try:
					username = row.find_element(By.XPATH, 'div/div/div//span/a/span').text
					try:
						nickname = row.find_element(By.XPATH, 'div/div[2]/div[2]/div').text
					except NoSuchElementException:
						nickname = None
					following = row.find_element(By.XPATH, 'div/div/button/div').text == "Abonné(e)"
					picture = row.find_element(By.XPATH, 'div/div[1]/div[1]/div/*/img').get_attribute('src')
				except Exception as e:
					pass
				else:
					parsed.add(row.id)

					user = {
						'username': username,
						'nickname': nickname,
						'following': following,
						'picture': picture
					}
					data.append(user)
			all_parsed += 1
			if all_parsed >= 2:
				break
			else:
				time.sleep(2)
			# # Attend la fin du chargement
			# xpth = 'div/svg'
			# try:
			# 	row.find_element(By.CLASS_NAME, 'svg')
			# 	row.find_element(By.XPATH, xpth)
			# except NoSuchElementException:
			# 	break
			# else:
			# WebDriverWait(self.driver, 10
			# ).until_not(lambda *args: print(args))#row.is_displayed)

		return data

	def xPth(self, xPth, wait=True):
		if wait:
			return self.wait(xPth)
		else:
			return self.driver.find_element(By.XPATH, xPth)

	def wait(self, xPth): 
		# Wait for element to be clickable
		return WebDriverWait(self.driver, 15).until(
			EC.element_to_be_clickable((By.XPATH, xPth))
		)

	def exists(self, xpath, wait=False):
		try:
			self.xPth(xpath, wait)
		except NoSuchElementException:
			return False
		except TimeoutException:
			return False
		return True

class Downloader:
	def __init__(self) -> None:
		self.que = queue.Queue()
		self._ddl_thread = threading.Thread(target=self.ddl_thread, args=(self.que,))
		self._ddl_thread.start()
	
	def ddl_thread(self, que):
		running = True
		while running:
			args = que.get()
			if args == 'STOP':
				running = False
				break
			
			url, file = args
			try:
				r = requests.get(url)
			except SSLError:
				que.put(args)
			except Exception as e:
				print('Error while downloading', file, '-', e)
			else:
				self.create_dir(os.path.dirname(file))
				i = 1
				base, ext = os.path.splitext(file)
				while os.path.exists(f'{base}{i}{ext}'):
					i += 1
				file = f'{base}{i}{ext}'	
					
				with open(file, 'wb') as f:
					f.write(r.content)
			que.task_done()
	
	def ddl(self, url, file):
		self.que.put((url, file))

	def create_dir(self, path):
		if not os.path.exists(path):
			root = os.path.dirname(path)
			if not os.path.exists(root):
				self.create_dir(root)
			os.mkdir(path)


if __name__ == "__main__":
	m = XpathFinder()
	# m.start()
	m.start_ui()
