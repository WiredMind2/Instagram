import json
# import multiprocessing.pool
import os
import queue
import random
import re
import sqlite3
import sys
import threading
import time
import traceback
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
import urllib3
from instagrapi import Client, config
from instagrapi.exceptions import (
	ClientError, ClientLoginRequired,
	ClientNotFoundError, DirectThreadNotFound,
	LoginRequired, PleaseWaitFewMinutes,
	UnknownError, UserNotFound)
from instagrapi.extractors import extract_media_v1, extract_user_short, extract_story_v1
from instagrapi.types import Location, Media, BaseModel
from instagrapi.utils import json_value

import secret
from google_api import GoogleAPI, ImageEditor, VideoEditor

# Classes


class Follower():
	FOLLOW = 1  # We followed the user
	UNFOLLOW = 2  # We unfollowed the user
	FOLLOWED = 3  # The user followed us
	UNFOLLOWED = 4  # The user unfollowed us

	KEEP = 5  # Don't unfollow
	UNKEEP = 6  # Can unfollow

	def __init__(self, *args, **data) -> None:
		# Construct an instance from sql request data
		if len(data) == 0:
			self.data = {}
			args = list(args)  # from tuple
			for key in ('pk', 'username', 'follow_since', 'keep', 'is_following', 'is_followed', 'profile_pic_url'):
				if args:
					val = args.pop(0)
				else:
					val = None
				self.data[key] = val
		else:
			self.data = data

	def __getitem__(self, *args, **kwargs):
		return self.data.__getitem__(*args, **kwargs)

	def __getattr__(self, *args, **kwargs):
		return self.data.__getitem__(*args, **kwargs)

	def __setitem__(self, *args, **kwargs):
		return self.data.__getitem__(*args, **kwargs)


class AttrDict(dict):
	def __init__(self, *args, **kwargs):
		super(AttrDict, self).__init__(*args, **kwargs)
		self.__dict__ = self


# Iterators

def user_followers_gql_chunk(self, user_id: str, max_amount: int = 0, end_cursor: str = None):
	user_id = str(user_id)
	users = []
	variables = {
		"id": user_id,
		"include_reel": True,
		"fetch_mutual": False,
		"first": 12
	}
	self.inject_sessionid_to_public()
	while True:
		if end_cursor:
			variables["after"] = end_cursor
		data = self.public_graphql_request(
			variables, query_hash="5aefa9893005572d237da5068082d8d5"
		)
		if not data["user"] and not users:
			raise UserNotFound(user_id=user_id, **data)
		page_info = json_value(
			data, "user", "edge_followed_by", "page_info", default={})
		edges = json_value(data, "user", "edge_followed_by",
						   "edges", default=[])
		for edge in edges:
			user = extract_user_short(edge["node"])
			users.append(user)
			yield user
		end_cursor = page_info.get("end_cursor")
		if not page_info.get("has_next_page") or not end_cursor:
			break
		if max_amount and len(users) >= max_amount:
			break


def user_followers_v1_chunk(self, user_id: str, max_amount: int = 0, max_id: str = ""):
	unique_set = set()
	users = []
	while True:
		result = self.private_request(f"friendships/{user_id}/followers/", params={
			"max_id": max_id,
			"count": 10000,
			"rank_token": self.rank_token,
			"search_surface": "follow_list_page",
			"query": "",
			"enable_groups": "true"
		})
		for user in result["users"]:
			user = extract_user_short(user)
			if user.pk in unique_set:
				continue
			unique_set.add(user.pk)
			users.append(user)
			yield user

		max_id = result.get("next_max_id")
		if not max_id or (max_amount and len(users) >= max_amount):
			break


def user_followers(self, user_id, use_cache=True, amount=0):
	user_id = str(user_id)
	users = self._users_followers.get(user_id, {})
	if not use_cache or not users or (amount and len(users) < amount):
		try:
			for user in user_followers_gql_chunk(self, user_id, amount):
				yield user
		except Exception as e:
			if not isinstance(e, ClientError):
				self.logger.exception(e)
			for user in user_followers_v1_chunk(self, user_id, amount):
				yield user
		self._users_followers[user_id] = {user.pk: user for user in users}


def user_medias_gql(self, user_id: int, sleep: int = 2):
	user_id = int(user_id)
	end_cursor = None
	variables = {
		"id": user_id,
		"first": 50,  # These are Instagram restrictions, you can only specify <= 50
	}
	while True:
		if end_cursor:
			variables["after"] = end_cursor
		medias_page, end_cursor = self.user_medias_paginated_gql(
			user_id, sleep=sleep, end_cursor=end_cursor
		)
		for media in medias_page:
			yield media
		if not end_cursor:
			break
		time.sleep(sleep)


def user_medias_v1(self, user_id: int):
	user_id = int(user_id)
	next_max_id = ""
	while True:
		try:
			medias_page, next_max_id = self.user_medias_paginated_v1(
				user_id,
				end_cursor=next_max_id
			)
		except Exception as e:
			self.logger.exception(e)
			break
		for media in medias_page:
			yield media
		if not self.last_json.get("more_available"):
			break
		next_max_id = self.last_json.get("next_max_id", "")


def user_medias(self, user_id: int):
	user_id = int(user_id)
	try:
		try:
			for media in user_medias_gql(self, user_id):
				yield media
		except ClientLoginRequired as e:
			if not self.inject_sessionid_to_public():
				raise e
			for media in user_medias_gql(self, user_id):
				yield media
	except Exception as e:
		if not isinstance(e, ClientError):
			self.logger.exception(e)
		try:
			for media in user_medias_v1(self, user_id):
				yield media
		except UnknownError as e:
			print(
				f'instagrapi.exceptions.UnknownError on user_medias({user_id}): {e}')

# TODO - Fetch all highlights with a single API call


def user_highlights(self, user_id: int, amount: int = 0):
	amount = int(amount)
	user_id = int(user_id)
	params = {
		"supported_capabilities_new": json.dumps(config.SUPPORTED_CAPABILITIES),
		"phone_id": self.phone_id,
		"battery_level": random.randint(25, 100),
		"is_charging": random.randint(0, 1),
		"will_sound_on": random.randint(0, 1),
	}
	result = self.private_request(f"feed/reels_tray/")
	result = self.private_request(
		f"highlights/{user_id}/highlights_tray/", params=params)
	return [
		self.extract_highlight_v1(highlight)
		for highlight in result.get("tray", [])
	]


def get_timeline(self):
	while True:
		headers = {
			"X-Ads-Opt-Out": "0",
			"X-DEVICE-ID": self.uuid,
			# str(random.randint(2000, 5000)),
			"X-CM-Bandwidth-KBPS": '-1.000',
			"X-CM-Latency": str(random.randint(1, 5)),
		}
		data = {
			'_uuid': self.uuid,
			'_csrftoken': self.token,
			'is_prefetch': '0',
			'is_pull_to_refresh': '0',
			'phone_id': self.phone_id,
			'timezone_offset': str(self.timezone_offset),
		}
		data = self.private_request(
			"feed/timeline/", json.dumps(data), with_signature=False, headers=headers
		)
		for item in data.get('feed_items', []):
			post = item.get('media_or_ad')
			# TODO - Do something with 'suggested_users'
			if post:
				if post.get('product_type') == 'ad':
					continue  # Ignore ads
				yield extract_media_v1(post)

		if data.get('more_available', False):
			next_max_id = data.get('next_max_id')

			data['max_id'] = next_max_id
		else:
			break


def get_reels_tray(self):
	REELS_COUNT = 3 # How many reels to fetch at once
	to_fetch = []
	data = {
		"supported_capabilities_new": config.SUPPORTED_CAPABILITIES,
		"timezone_offset": str(self.timezone_offset),
		"tray_session_id": self.tray_session_id,
		"request_id": self.request_id,
		"latest_preloaded_reel_ids": "[]",  # [{"reel_id":"6009504750","media_count":"15","timestamp":1628253494,"media_ids":"[\"2634301737009283814\",\"2634301789371018685\",\"2634301853921370532\",\"2634301920174570551\",\"2634301973895112725\",\"2634302037581608844\",\"2634302088273817272\",\"2634302822117736694\",\"2634303181452199341\",\"2634303245482345741\",\"2634303317473473894\",\"2634303382971517344\",\"2634303441062726263\",\"2634303502039423893\",\"2634303754729475501\"]"},{"reel_id":"4357392188","media_count":"4","timestamp":1628250613,"media_ids":"[\"2634142331579781054\",\"2634142839803515356\",\"2634150786575125861\",\"2634279566740346641\"]"},{"reel_id":"5931631205","media_count":"7","timestamp":1628253023,"media_ids":"[\"2633699694927154768\",\"2634153361241413763\",\"2634196788830183839\",\"2634219197377323622\",\"2634294221109889541\",\"2634299705648894876\",\"2634299760434939842\"]"}],
		"page_size": 50,
		"_csrftoken": self.token,
		"_uuid": self.uuid,
	}
	data = self.private_request("feed/reels_tray/", data)
	for story in data['tray']:
		user = extract_user_short(story['user'])
		if user.username is None:
			user = self.user_info(user.pk)

		if not 'items' in story:
			to_fetch.append(story['id'])

			if len(to_fetch) == REELS_COUNT:
				for reel in reel_info_v1(self, to_fetch):

					if reel.user.pk != user.pk:
						user = extract_user_short(story['user'])
					if story['user']['username'] is None and user.username is None:
						user = self.user_info(user.pk)

					reel.user = user
					yield reel
				to_fetch = []

			continue

		for item in story['items']:
			reel = extract_story_v1(item)
			
			if reel.user.pk != user.pk:
				user = extract_user_short(story['user'])
			if story['user']['username'] is None and user.username is None:
				user = self.user_info(user.pk)

			reel.user = user
			yield reel


	# for m_id in to_fetch:
	# 	yield self.story_info(m_id)

def reel_info_v1(self, pks):
	args = '&'.join(f'reel_ids={pk}' for pk in pks)
	result = self.private_request(f'feed/reels_media/?{args}',)
	for pk, reel in result.get('reels', {}).items():
		for media in reel.get('items', []):
			yield extract_story_v1(media)


def custom_direct_thread(self, thread_id: int, amount: int = 20, cursor=None):
	# Iterate over all messages from thread
	assert self.user_id, "Login required"
	params = {
		"visual_message_return_type": "unseen",
		"direction": "older",
		"seq_id": "40065",  # 59663
		"limit": "20",
	}
	items = []
	while True:
		if cursor:
			params["cursor"] = cursor
		try:
			result = self.private_request(
				f"direct_v2/threads/{thread_id}/", params=params
			)
		except ClientNotFoundError as e:
			raise DirectThreadNotFound(
				e, thread_id=thread_id, **self.last_json)
		thread = result["thread"]
		for item in thread["items"]:
			yield item
			# items.append(item)
		cursor = thread.get("oldest_cursor")
		if not cursor or (amount and len(items) >= amount):
			break


def ddl_raven_media(media, filename=''):
	imgs = media.get('image_versions2')
	if not imgs:
		return

	empty = True
	for candidate in imgs['candidates']:
		url = candidate['url']
		try:
			req = requests.get(url)
			req.raise_for_status()
		except Exception as e:
			print(e)
			continue
		else:
			empty = False
			break

	if empty:
		return

	return save_media(url, req, filename)


def ddl_thread_media(url, filename=''):
	try:
		req = requests.get(url)
		req.raise_for_status()
	except Exception as e:
		print(e)
		return

	return save_media(url, req, filename)


def save_media(url, req, filename=''):
	fname = urlparse(url).path.rsplit("/", 1)[1].strip()
	filename = "%s.%s" % (filename, fname.rsplit(".", 1)
						  [1]) if filename else fname

	with open(filename, "wb") as f:
		for chunk in req.iter_content(chunk_size=8192):
			f.write(chunk)

	return filename

# Utils


class InstaAPI(GoogleAPI):
	def __init__(self, files_root=None):
		self.files_root = os.path.abspath(files_root or '.')
		super().__init__(files_root=files_root)

		self.info = self.login()
		self.user_id = self.info.pk
		# self.api = GoogleAPI()

		self.get_db()

		print(f'Logged in as {self.info.username}')

	def get_db(self):
		self.con = sqlite3.connect(SETTINGS['DATABASE'], check_same_thread=False)
		self.con.row_factory = sqlite3.Row
		self.cur = self.con.cursor()
		self.db_lock = threading.RLock()

	def login(self, use_cache=True):
		self.cl = Client()
		if use_cache and os.path.exists(SETTINGS['AUTH_SETTINGS_FILE']):
			try:
				self.cl.load_settings(SETTINGS['AUTH_SETTINGS_FILE'])
			except Exception as e:
				print('Error while loading login settings:', e)

		self.cl.login(secret.USERNAME, secret.PASSWORD)
		self.cl.dump_settings(SETTINGS['AUTH_SETTINGS_FILE'])

		default = AttrDict({
			'pk': '39443737713',
			'username': 'yana23lb',
			'full_name': '✨Yana🌿'})

		try:
			return default
			return self.cl.account_info()  # Status 429: Too many requests
		except LoginRequired:
			if use_cache:
				print('Relogging without cache')
				time.sleep(10)
				return self.login(use_cache=False)
			else:
				raise
		except PleaseWaitFewMinutes:
			return default

	def list_threads(self):
		# List all threads
		threads = self.cl.direct_threads()
		for t in threads:
			yield t
			# print(t.id, t.thread_title)

	def get_conv_logs(self, thread_id):
		root = os.path.join('insta_data', 'threads')
		if not os.path.exists(root):
			os.mkdir(root)

		output = os.path.join(root, f'{str(thread_id)}.txt')
		open(output, 'w').close()  # Clear file

		thread = self.cl.direct_thread(thread_id)
		users = {int(u.pk): u.full_name for u in thread.users}
		users[self.user_id] = self.info.full_name
		c = 0
		with open(output, 'a', encoding='utf-8') as f:
			for item in custom_direct_thread(self.cl, thread_id):
				c += 1
				timestamp = item['timestamp']
				sender = users.get(item['user_id'], item['user_id'])
				if item['item_type'] == 'text':
					content = item['text']
				else:
					content = item['item_type']
				data = f'{timestamp}-{sender}: {content}\n'
				f.write(data)
				if c % 10 == 0:
					print(f'Saved {c} msgs')

		print('Done')

	def parse_dir(self, root):
		if not os.path.exists(root):
			return []
		for f in os.listdir(root):
			path = os.path.normpath(os.path.join(root, f))
			if os.path.isdir(path):
				for file in self.parse_dir(path):
					yield file
			else:
				yield path

	# Follows

	def get_follows(self, user_id, reload=False):
		if reload:
			print('Fetching followers')
			followers = self.cl.user_followers(user_id)
			print('Fetching followed')
			followed = self.cl.user_following(user_id)

			print('Saving data')
			print(f'{len(followers)} followers, {len(followed)} followed')

			def iter(followers, followed):
				follow_since = int(time.time())
				for pk, u in followers.items():
					is_followed = pk in followed
					data = {
						'follow_since': follow_since,
						'keep': Follower.UNKEEP,
						'is_following': True,
						'is_followed': is_followed,
					}
					data |= u.__dict__
					yield Follower(**data)
				for pk, u in followed.items():
					if pk not in followers:
						data = {
							'follow_since': follow_since,
							'keep': Follower.UNKEEP,
							'is_following': False,
							'is_followed': True,
						}
						data |= u.__dict__
						yield Follower(**data)

			exists_test = "SELECT EXISTS(SELECT 1 FROM users WHERE pk=?)"
			update = "UPDATE users SET username=:username, is_following=:is_following, is_followed=:is_followed, profile_pic_url=:profile_pic_url WHERE pk=:pk"
			insert = "INSERT INTO users (pk, username, follow_since, keep, is_following, is_followed, profile_pic_url) VALUES (:pk, :username, :follow_since, :keep, :is_following, :is_followed, :profile_pic_url)"

			try:
				self.db_lock.acquire(True)
				for follow in iter(followers, followed):
					self.cur.execute(exists_test, (follow['pk'],))
					exists = bool(self.cur.fetchone()[0])

					data = {
						'pk': follow['pk'],
						'username': follow['username'],
						'follow_since': follow['follow_since'],
						'keep': follow['keep'],
						'is_following': follow['is_following'],
						'is_followed': follow['is_followed'],
						'profile_pic_url': str(follow['profile_pic_url_hd'] or follow['profile_pic_url'])
					}
					sql = update if exists else insert

					self.cur.execute(sql, data)
			finally:
				self.con.commit()
				self.db_lock.release()
		else:
			with self.db_lock:
				self.cur.execute('SELECT pk FROM users WHERE is_followed=1')
				followers = [e[0] for e in self.cur.fetchall()]
				self.cur.execute('SELECT pk FROM users WHERE is_following=1')
				followed = [e[0] for e in self.cur.fetchall()]

		return followers, followed

	def unfollow_users(self):
		print('Unfollowing is disabled')
		return 0  # Disabled
		unfollow = "UPDATE users SET is_followed=0 WHERE pk=?"
		log_unfollow = "INSERT INTO actions_logs(pk, action, timestamp) VALUES (?, ?, ?)"
		delete_user = "DELETE FROM users WHERE pk=?"
		count = 0

		sql = f"SELECT * FROM users WHERE is_followed=1 AND is_following=0 AND keep={Follower.UNKEEP}"
		self.cur.execute(sql)
		users = self.cur.fetchall()

		if len(users) == 0:
			print('No user not following back')
			return
		print(f'{len(users)} users not following back')

		try:
			for user in users:
				pk, username, follow_since, keep, is_following, is_followed, *_ = user
				follow_time = int(time.time() - follow_since)
				if follow_time > SETTINGS['UNFOLLOW_DELAY']:
					print(f'Unfollowing {user["username"]}')
					continue
					try:
						out = self.cl.user_unfollow(pk)
					except requests.exceptions.JSONDecodeError:
						# User not found / deleted account?
						print(
							f'Account not found: {user["username"]}, deleting from db, pk: {pk}')
						if pk and len(str(pk)) > 0:
							self.cur.execute(delete_user, (pk,))
						continue

					# out = self.cl.public_a1_request(f"/web/friendships/{pk}/unfollow/")
					if out is not True:  # Could be None ?
						print('Potential error:', user)
						break

					count += 1
					self.cur.execute(unfollow, (pk,))
					self.cur.execute(
						log_unfollow, (pk, Follower.UNFOLLOW, int(time.time())))
					time.sleep(1)
					# a = self.cl.user_info(pk)
				# else:
				#     print(f'Still need to wait to unfollow {user["username"]}')
		except KeyboardInterrupt:
			print('Interrupted')
		finally:
			self.con.commit()
		print(f'Unfollowed {count} users')
		return count

	def update_users_info(self, limit=50):
		print('Updating users')

		def pk_iter():
			sql_reqs = [
				"SELECT follower FROM follows WHERE follower not in (SELECT pk from users);",
				"SELECT pk FROM users WHERE followers is null;"
			]  # Could use LIMIT 0,50; too
			for sql in sql_reqs:
				self.cur.execute(sql)
				for row in self.cur.fetchall():
					yield row

		count = 0
		# self.cur.execute(sql) -> WTF??
		try:
			self.db_lock.acquire(True)
			for row in pk_iter():
				try:
					time.sleep(1)
					pk = row[0]
					print(pk)
					info = self.cl.user_info(pk)
					sql = f"UPDATE users SET followers=:followers, following=:following, private=:private, profile_pic_url=:profile_pic_url WHERE pk=:pk"
					data = {
						'pk': pk,
						'followers': info.follower_count,
						'following': info.following_count,
						'private': int(info.is_private),
						'profile_pic_url': str(info.profile_pic_url_hd or info.profile_pic_url)
					}
					self.cur.execute(sql, data)

					count += 1
					if count >= limit:
						break
				except Exception as e:
					print(e)
					raise
		finally:
			self.con.commit()
			self.db_lock.release()

	# Media ddl

	def create_dir(self, path):
		if not os.path.exists(path):
			root = os.path.dirname(path)
			if not os.path.exists(root):
				self.create_dir(root)
			os.mkdir(path)

	def media_downloaded(self, m):
		# Check if a media has already been downloaded

		root, filename = self.get_media_filename(m)

		if m.media_type == 8:
			# Album
			for resource in m.resources:
				# filename_ress = f"{filename}_{resource['pk']}"
				filename_ress = filename.format(resource.pk)

				for f in os.listdir(root):
					f = f.rsplit('.', 1)[0]
					if f == filename_ress:
						# File already exists
						return True
		else:
			for f in os.listdir(root):
				if f.rsplit('.', 1)[0] == filename:
					# File already exists
					return True

		sql = "SELECT EXISTS(SELECT 1 FROM deleted_files WHERE media_pk=?)"
		with self.db_lock:
			self.cur.execute(sql, (m.pk,))
			exists = bool(self.cur.fetchone()[0])

		return exists

	def get_media_folder(self, m):
		if m.product_type == 'story':
			folder = 'stories'
		elif m.product_type == 'feed':
			folder = 'feed'
		elif m.product_type == 'igtv':
			folder = 'IGTV'
		elif m.product_type == 'clips':
			folder = 'reels'
		elif m.media_type == 1:
			# Post
			folder = 'posts'
		elif m.media_type == 2:
			# Video
			folder = 'videos'
		elif m.media_type == 8:
			# Album
			folder = 'posts'
		else:
			folder = 'others'

		return folder

	def get_media_filename(self, m):
		user = m.user.username
		if user is None:
			pk = m.user.pk
			user = self.cl.username_from_user_id(pk)
			if user is None:
				raise Exception(f'User with pk {pk} was not found!')
				return None, None
			m.user = user

		root = os.path.abspath(os.path.join(
			'./insta_data', self.get_media_folder(m), user))

		timestamp = m.taken_at
		if type(timestamp) is int:
			timestamp = str(timestamp)
		elif type(timestamp) is datetime:
			timestamp = str(int(timestamp.timestamp()))

		filename = f'{timestamp}-{m.pk.replace("_", "-")}'

		if m.media_type == 8:
			# Album
			# Option 1: create a folder
			# root = os.path.join(root, filename)
			# filename = ''

			# Option 2: rename the images
			filename += '-{}'

		self.create_dir(root)
		return root, filename

	def ddl_media(self, m, force=False):
		if isinstance(m, dict):
			m = AttrDict(**m)

		root, filename = self.get_media_filename(m)
		root = Path(root)

		if not force and self.media_downloaded(m):
			return []

		print(f"{m.user.username} - {m.pk}")

		try:
			paths = []
			if m.media_type == 1 and m.product_type == 'story':
				# Story - Photo
				paths.append(self.cl.story_download_by_url(
					m.thumbnail_url, filename=filename, folder=root))
			elif m.media_type == 1:
				# Photo
				paths.append(self.cl.photo_download_by_url(
					m.thumbnail_url, filename=filename, folder=root))
			elif m.media_type == 2 and m.product_type == 'feed':
				# Video
				if m.video_url is None:
					return
				paths.append(self.cl.video_download_by_url(
					m.video_url, filename=filename, folder=root))
			elif m.media_type == 2 and m.product_type == 'story':
				# Story - Video
				paths.append(self.cl.story_download_by_url(
					m.video_url, filename=filename, folder=root))
			elif m.media_type == 2 and m.product_type == 'igtv':
				# IGTV
				paths.append(self.cl.video_download_by_url(
					m.video_url, filename=filename, folder=root))
			elif m.media_type == 2 and m.product_type == 'clips':
				# Reels
				paths.append(self.cl.video_download_by_url(
					m.video_url, filename=filename, folder=root))
			elif m.media_type == 8:
				# Album
				for resource in m.resources:
					filename_ress = filename.format(resource.pk) # resource['pk']
					if resource.media_type == 1:
						paths.append(
							self.cl.photo_download_by_url(
								resource.thumbnail_url, filename=filename_ress, folder=root)
						)
					elif resource.media_type == 2:
						paths.append(
							self.cl.video_download_by_url(
								resource.video_url, filename=filename_ress, folder=root)
						)
			else:
				raise Exception(f'Unknown media: {m}')
		except AssertionError as e:
			print(e)
			pass
		return paths

	def ddl_stories(self, user_id, que):
		stories = self.cl.user_stories(user_id)
		count = 0
		# ids = set()

		for story in stories:
			# ids.add(story['pk'])

			if not self.media_downloaded(story):
				que.put(story)
				count += 1

		# self.cl.story_seen(ids)

		print(f'- {count} stories\n', end="")

	def ddl_posts(self, user_id, que):
		# TODO - Check in db if smth changed
		posts = user_medias(self.cl, user_id)
		post = None
		count = 0
		# ids, skipped = set(), set()

		for post in posts:
			if not self.media_downloaded(post):
				# ids.add(post.pk)
				que.put(post)
				count += 1
			# else:
			# 	skipped.add(post.pk)

		# self.cl.media_seen(ids, skipped)
		print(f'- {count} posts\n', end="")

	def ddl_highlights(self, user_id, que):
		story = None
		count = 0

		get_info = "SELECT * FROM medias WHERE pk=:pk AND user=:user LIMIT 0, 1;"
		insert_info = "INSERT INTO medias (pk, user, type, count, last) VALUES (:pk, :user, :type, :count, :last);"
		update_info = "UPDATE medias SET count=:count, last=:last WHERE pk=:pk AND user=:user;"

		highlights = self.cl.user_highlights(user_id)

		if len(highlights) > 0:
			try:
				self.db_lock.acquire(True)
				for highlight in highlights:
					data = {
						'pk': int(highlight.pk),
						'user': int(highlight.user.pk),
						'type': 'HIGHLIGHT',
						'count': highlight.media_count,
						'last': highlight.latest_reel_media
					}
					self.cur.execute(get_info, data)
					infos = self.cur.fetchone()

					if infos is not None and infos['count'] == highlight.media_count and infos['last'] == highlight.latest_reel_media:
						continue

					save_info = insert_info if infos is None else update_info
					self.cur.execute(save_info, data)

					time.sleep(1)
					loops = 0
					while loops < 3:
						try:
							highlight_info = self.cl.highlight_info(
								highlight.pk)
						except PleaseWaitFewMinutes:
							print('PleaseWaitFewMinutes error: waiting 1 minute')
							time.sleep(60)
							loops += 1
							if loops == 3:
								return
						else:
							break

					for story in highlight_info.items:
						if not self.media_downloaded(story):
							count += 1
							que.put(story)
							time.sleep(0.2)
			finally:
				self.con.commit()
				self.db_lock.release()

		print(f'- {count} highlights\n', end="")

	def que_handler(self, que, paths_queue):
		count = 0
		while True:
			data = que.get()
			if data == 'STOP':
				que.task_done()
				que.put(count)
				return
			try:
				paths = self.ddl_media(data)
				paths_queue.put(paths)
				count += len(paths)
			except urllib3.exceptions.SSLError as e:
				print('SSLError, ignoring')
			except Exception as e:
				print(
					f'Error on download, pk: {data["pk"]} - error: {type(e)}, {e}')
			else:
				time.sleep(1)
			finally:
				que.task_done()

	def get_tracked_medias(self):
		sql = f"SELECT * FROM users WHERE keep={Follower.KEEP}"
		with self.db_lock:
			self.cur.execute(sql)
			tracked = list(Follower(*e) for e in self.cur.fetchall())
		tracked_id = set(int(f['pk']) for f in tracked)

		paths_queue = queue.Queue()

		que = queue.Queue()
		handler = threading.Thread(
			target=self.que_handler, args=(que, paths_queue))
		handler.start()

		# Stories
		stories = get_reels_tray(self.cl)

		for story in stories:
			if int(story.user.pk) in tracked_id and not self.media_downloaded(story):
				que.put(story)			

		que.join()

		# Posts 
		posts = get_timeline(self.cl)

		with self.db_lock:
			self.cur.execute('SELECT value FROM cache WHERE key="last_seen_post"')
			last_seen = self.cur.fetchone()[0]

		last_seen = datetime.fromisoformat(last_seen)
		
		latest_seen = None

		for post in posts:
			if latest_seen is None:
				latest_seen = post.taken_at

			if post.taken_at < last_seen:
				break

			if int(post.user.pk) in tracked_id and not self.media_downloaded(post):
				que.put(post)

		que.join()

		with self.db_lock:
			self.cur.execute('UPDATE cache SET value=? WHERE key="last_seen_post"', (latest_seen.isoformat(),))
			self.con.commit()


		# for follower in tracked:
		# 	print(f"- {follower['username']}:")
		# 	self.ddl_stories(follower['pk'], que)
		# 	que.join()
		# 	# ddl_highlights(follower['pk'], que) - DISABLED
		# 	self.ddl_posts(follower['pk'], que)
		# 	que.join()

		que.put('STOP')
		handler.join()

		media_count = que.get()

		paths = []
		while not paths_queue.empty():
			paths.extend(
				self.convert_medias(paths_queue.get())
			)

		return media_count, paths

	def convert_medias(self, paths):
		for i, path in enumerate(paths):
			head, ext = os.path.splitext(path)
			if ext in ('.webp', '.png'):
				new_path = ImageEditor(path).convert_image()
				os.remove(path)
				paths[i] = new_path
		return paths

	# Target finder

	def parse_new_user_followers(self):
		sql = "SELECT pk FROM users WHERE pk not in (SELECT user FROM follows);"
		max_count = 20
		count = 0
		with self.db_lock:
			self.cur.execute(sql)
			pks = self.cur.fetchall()
		for pk in pks:
			pk = pk[0]
			count += self.get_followers(pk)
			if count >= max_count:
				break
		return count

	def get_followers(self, target_pk):
		if target_pk is None:
			return 0
		print(f'Looking for new users from pk {target_pk}')
		max_users = 20
		count = 0
		sql_exists = "SELECT EXISTS(SELECT 1 FROM users WHERE pk=?), EXISTS(SELECT 1 FROM follows WHERE user=? AND follower=?)"
		sql_user = f"INSERT INTO users (pk, username, follow_since, keep, is_following, is_followed, followers, following, private, profile_pic_url) VALUES (:pk, :username, {int(time.time())}, {Follower.UNKEEP}, 0, 0, :followers, :following, :private, :profile_pic_url)"
		sql_follow = "INSERT INTO follows (user, follower, last_check) VALUES (:user, :follower, :last_check)"
		sql_follow_update = "UPDATE follows last_check=:last_check WHERE user=:user AND follower=:follower"
		try:
			self.db_lock.acquire(True)
			for follower in user_followers(self.cl, target_pk):
				pk = int(follower.pk)
				self.cur.execute(sql_exists, (pk, int(target_pk), pk))

				user_exists, follow_exists = list(map(bool, self.cur.fetchone()))
				info = self.cl.user_info(pk)
				profile_pic_url = info.profile_pic_url_hd or info.profile_pic_url
				if not user_exists:
					data = {
						'pk': info.pk,
						'username': info.username,
						'followers': info.follower_count,
						'following': info.following_count,
						'private': int(info.is_private),
						'profile_pic_url': profile_pic_url
					}
					self.cur.execute(sql_user, data)

					count += 1
					print(
						f'Saved new user {count}/{max_users}: {data["username"]}, {data["followers"]}/{data["following"]}')
					if count == max_users:
						break

				data = {
					'user': target_pk,
					'follower': pk,
					'last_check': int(time.time())
				}
				req = sql_follow_update if follow_exists else sql_follow
				self.cur.execute(req, data)

		finally:
			self.con.commit()
			self.db_lock.release()

		if count == 0:
			print(f'All users already parsed for pk {target_pk}!')
		time.sleep(1)  # Make sure we don't spam too many requests
		return count

	def find_new_followers(self):
		sql = "SELECT * FROM follows JOIN users AS u on follower=u.pk WHERE is_followed=0;"

	# Repost

	def repost_url(self, url, desc):
		pk = self.cl.media_pk_from_url(url)
		media = self.cl.media_info(pk)

		paths = self.ddl_media(media, force=True)
		if len(paths) == 0:
			print(f'No media found for url: {url}')
			return

		if desc == '_copy':
			caption = media.caption
		else:
			caption = desc
		if media.location is None:
			location = None
		else:
			location = Location(**media.location)

		if media.media_type == 1:
			self.cl.photo_upload(
				path=paths[0],
				caption=caption,
				location=location
			)
		elif media.media_type == 2:
			self.cl.video_upload(
				path=paths[0],
				caption=caption,
				location=location
			)
		elif media.media_type == 8:
			self.cl.album_upload(
				paths=paths,
				caption=caption,
				location=location
			)
		else:
			print(f'Unknown media type: {media.media_type}')

	def repost_scheduled(self):
		sql = "SELECT url, desc FROM posts WHERE posted=0 AND timestamp < ?"
		sql_save = "UPDATE posts SET posted=1 WHERE url=?"

		with self.db_lock:
			self.cur.execute(sql, (int(time.time()),))
			posts = self.cur.fetchall()

			if len(posts) == 0:
				print('No post scheduled')
				return

			try:
				for post in posts:
					url, desc = post
					self.repost_url(url, desc)

					self.cur.execute(sql_save, (url,))
			finally:
				self.con.commit()

	def add_post(self, url, timestamp, desc_mode):
		sql_exists = "SELECT EXISTS(SELECT 1 FROM posts WHERE url=:url), EXISTS(SELECT 1 FROM posts WHERE url=:url AND posted=1)"
		sql_insert = "INSERT INTO posts (url, timestamp, desc, posted) VALUES (:url, :timestamp, :desc, 0)"
		sql_update = "UPDATE posts SET timestamp=:timestamp, desc=:desc WHERE url=:url"

		with self.db_lock:
			self.cur.execute(sql_exists, (url,))
			exists, post_exists = list(map(bool, self.cur.fetchone()))

		if exists:
			if post_exists:
				print('This image has already been posted!')
				return
			else:
				print('This image is already registered, updating')

		if isinstance(timestamp, datetime):
			timestamp = timestamp.timestamp()

		timestamp = int(timestamp)

		if desc_mode == 'none':
			desc = ''
		elif desc_mode == 'rnd_emoji':
			desc = random.choice(list(SETTINGS['EMOJIS']))
		else:
			desc = desc_mode

		data = {
			'url': url,
			'timestamp': timestamp,
			'desc': desc
		}
		print(data)
		sql = sql_update if exists else sql_insert

		with self.db_lock:
			self.cur.execute(sql, data)

			self.con.commit()

	# Web server sync

	def sync_db(self):
		with self.db_lock:
			sql = "SELECT * FROM users"
			self.cur.execute(sql)
			user_data = iter(self.cur)

			data = []  # WTF?
			count = 0
			empty = False
			while not empty:
				while count < 100:
					user = next(user_data, None)
					if user is None:
						empty = True
						break
					data.append(user)
					count += 1

				data = json.dumps(data)
				r = requests.post(
					BASE_URL + f'?unlock=will&sync_db={count}', data=data)
				r.raise_for_status()

				data = []
				count = 0

	def send_server(self, path):
		rel_path = os.path.normpath(os.path.relpath(
			path, './insta_data')).replace('\\', '/')
		with open(path, 'rb') as f:
			headers = {'content-type': 'application/x-www-form-urlencoded'}
			r = requests.post(
				BASE_URL + f'?unlock=will&upload_path={rel_path}', headers=headers, data=f)
			r.raise_for_status()
			if r.content == b'Ok':
				print(f'Sent {rel_path}\n', end="")
			else:
				rep = r.content.decode()
				raise Exception(rep)

	def get_server_medias(self):
		try:
			r = requests.get(BASE_URL + '?unlock=will&get_medias=ALL')
			r.raise_for_status()
			medias = r.json()
			return medias
		except Exception as e:
			print(e)
			return []

	def send_new_medias(self, paths):
		known = set(self.get_server_medias())
		for path in paths:
			if os.path.exists(path) and os.path.getsize(path) < SETTINGS['MAX_FILE_SIZE'] and path not in known:
				try:
					self.send_server(path)
				except Exception as e:
					print(e)
					raise

	# Logistic ig

	def purge_old_medias(self):
		root = 'insta_data'
		pat = r'\d+-\d+_\d+.\w+'
		for media_type in ('stories', 'posts', 'reels', 'IGTV', 'feed'):
			media_folder = os.path.join(root, media_type)
			for user in os.listdir(media_folder):
				user_folder = os.path.join(media_folder, user)
				for file in os.listdir(user_folder):
					path = os.path.join(user_folder, file)
					if os.path.isdir(path):
						print(f'Removing sub-folder {path}')
						while True:  # Loop needed for fake empty dir
							for sub in self.parse_dir(path):
								os.remove(sub)
							try:
								os.rmdir(path)
							except OSError as e:
								if e.errno == 2:
									break
								print(e)
								pass

					elif re.match(pat, file):
						print(f'Removing invalid file {path}')
						os.remove(path)

	def clean_files(self):
		for media in ('posts', 'stories', 'feed',  'IGTV'):
			root = os.path.join('insta_data', media)
			if os.path.isdir(root):
				for user in os.listdir(root):
					path = os.path.join(root, user)
					for file in os.listdir(path):
						filepath = os.path.join(path, file)

						if os.path.isdir(filepath):  # post subfolders
							pass
							for sub in self.parse_dir(filepath):
								os.remove(sub)
							os.rmdir(filepath)

						if '_' in file:  # incorrect _ in files
							target_path = os.path.join(
								path, file.replace('_', '-'))
							if os.path.exists(target_path):
								os.remove(filepath)
							else:
								os.rename(filepath, target_path)

						if file.count('-') == 2:  # Long filename when it's not needed
							head, tail = os.path.splitext(filepath)
							head = head.rsplit('-', 1)[0]
							short_name = head + tail

							if os.path.exists(short_name):
								os.remove(filepath)

						if os.path.splitext(file)[-1] == '.webp':
							editor = ImageEditor(filepath)
							editor.convert_image()
							print(f'Converted {filepath} to .jpg')
							os.remove(filepath)

	def delete_medias(self, paths=None):
		if paths is not None:
			for path in paths:
				path = os.path.abspath(path)
				if os.path.exists(path):
					if os.path.isdir(path):
						for sub in self.parse_dir(path):
							os.remove(sub)
						os.rmdir(path)
					else:
						os.remove(path)
			print(f'Deleted {len(paths)} files')
			return

		old_paths = set(self.parse_dir('insta_data'))
		input('Please delete the desired files...')

		new_paths = set(self.parse_dir('insta_data'))
		deleted = old_paths - new_paths

		pk_pat = r'\d+-(\d+)\d+?.\w+'
		sql = "INSERT INTO deleted_files(media_pk, path) VALUES (:media_pk, :path)"
		try:
			self.db_lock.acquire(True)
			for path in deleted:
				path = os.path.relpath(path, 'insta_data')
				pk = re.search(pk_pat, path)
				print(f'Deleted: {path}, pk {pk}')
				data = {
					'media_pk': pk,
					'path': path
				}
				self.cur.execute(sql, data)
		finally:
			self.con.commit()
			self.db_lock.release()

	def schedule(self):
		self.clean_files()

		media_count, paths = self.get_tracked_medias()

		followers, following = self.get_follows(self.user_id, True)
		unfollowed = self.unfollow_users()

		# Server sync
		# try:
		#     send_new_medias(paths)
		# except Exception as e:
		#     send_error(e)

		# Scheduled posts
		try:
			self.repost_scheduled()
		except Exception as e:
			self.send_error(e)

		# Search for new users - Blocks the account
		# try:
		#     parse_new_user_followers()
		# except Exception as e:
		#     send_error(e)

		self.send_status(self.info, media_count, len(
			followers), len(following), unfollowed, paths)

		# Google Drive stuff
		
		try:
			with self.db_lock:
				self.cur.execute('SELECT value FROM cache WHERE key="last_seen_comment"')
				last_comment = self.cur.fetchone()[0]

			new_comment = self.get_comments(last_comment)

			with self.db_lock:
				self.cur.execute('UPDATE cache SET value=? WHERE key="last_seen_comment"', (new_comment,))
				self.con.commit()
		except Exception as e:
			self.send_error(e)

		try:
			self.add_new_medias(3)
		except Exception as e:
			self.send_error(e)

		try:
			self.purge_deleted()
			self.purge_ghost_files()
		except Exception as e:
			self.send_error(e)

	def send_status(self, info, media_count, followers_count, following_count, unfollowed, paths):
		text = (
			'Schedule ran correctly\n'
			f'Account: {info.username}\n'
			f'{media_count} new medias\n'
			f'{followers_count} followers / {following_count} following\n'
			f'{unfollowed} unfollowed users'
		)
		data = {'content': text}

		if SEND_DC_ARCHIVES:
			archives = self.create_archive(paths)
			if len(archives) == 0:
				files = None
			else:
				files = {
					'file': (os.path.basename(archives[0]), open(archives[0], 'rb')),
				}
		else:
			files = None

		r = requests.post(secret.WEBHOOK_URL, data=data, files=files)
		r.raise_for_status()

		if SEND_DC_ARCHIVES:
			if len(archives) > 1:
				for archive in archives[1:]:
					files = {
						'file': (os.path.basename(archive), open(archive, 'rb')),
					}
					r = requests.post(secret.WEBHOOK_URL, files=files)
					r.raise_for_status()

			del files  # Close the file
			for archive in archives:
				try:
					os.remove(archive)
				except PermissionError as e:
					print(e)
					pass
		print('Webhook status sent!')

	def send_error(self, e):
		err_text = traceback.format_exception(
			type(e), value=e, tb=e.__traceback__)
		text = (
			'An error occured on insta_api.py!\n' if not SETTINGS[
				'DISCORD_PING'] else f'{SETTINGS["DISCORD_PING"]}, an error occured on insta_api.py!\n'
			f'Timestamp: {datetime.now().isoformat()}\n'
		) + ''.join(err_text)

		data = {'content': text}
		requests.post(secret.WEBHOOK_URL, data)

	def create_archive(self, paths):
		if len(paths) == 0:
			return []
		root = os.path.abspath('./insta_data')
		max_size = 8000000
		count = 0
		size = 0

		pat = r'new_medias_\d+.zip'
		for file in os.listdir(root):
			if re.match(pat, file):
				os.unlink(os.path.join(root, file))

		archive = os.path.join(root, f'new_medias_{count}.zip')
		archives = [archive]

		zipf = zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED)
		for path in paths:
			path = os.path.abspath(path)
			file_size = os.path.getsize(path)
			size += file_size
			if size >= max_size:
				zipf.close()
				count += 1
				size = file_size

				archive = os.path.join(root, f'new_medias_{count}.zip')
				archives.append(archive)
				zipf = zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED)

				if file_size > max_size:
					print(f'Skipping {path}: {file_size} bytes is too large')
					size = 0
					continue
			zipf.write(
				path,
				os.path.relpath(path, root)
			)
		zipf.close()
		return archives

	def parse_command(self, command, path):
		op, *args = command.split(' ')

		if not os.path.exists(path):
			print(f'Path not found in parse_command(): {path}')
			return

		update = True

		if op == "frame":
			editor = VideoEditor(path)
			if args:
				timestamp = int(args.pop(0))
			else:
				timestamp = None
			editor.save_frame(path, timestamp)
		elif op == "convert":
			if os.path.isdir(path):
				root = path
				for f in os.listdir(root):
					path = os.path.join(root, f)
					# Should deal with recursive folders
					self.parse_command(command, path)
				update = False
			else:
				editor = ImageEditor(path)
				editor.convert_image()
		# elif op == "get":
		#     get_type = args.pop(0)
		#     if get_type == 'highlights':

		else:
			update = False

		if update:
			self.force_update.add(path)


if __name__ == '__main__':
	SEND_DC_ARCHIVES = False
	LOCAL = False
	BASE_URL = "http://localhost/instagram" if LOCAL else "https://www.tetrazero.com/instagram"

	print(f'---{datetime.now().isoformat()}---')

	SETTINGS_FILE = './settings.json'
	if not os.path.exists(SETTINGS_FILE):
		open(SETTINGS_FILE, 'x').close()
		print('Please set new settings')
		exit()

	with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
		SETTINGS = json.load(f)

	if len(sys.argv) > 1:
		insta = InstaAPI()
		file, action, *args = sys.argv
		if action == 'schedule':
			try:
				insta.schedule()
			except Exception as e:
				print(f'Error on schedule: {type(e)}, {e}')
				insta.send_error(e)

		elif action == 'post':
			keys = ('url', 'timestamp', 'desc_mode')
			while len(args) >= len(keys):
				kwargs = {}
				for k in keys:
					kwargs[k] = args.pop(0)

				# Probably an error if the date is more than 1yr away
				if kwargs['timestamp'].isdigit() and int(kwargs['timestamp'])-time.time() < 365*24*60*60:
					kwargs['timestamp'] = int(kwargs['timestamp'])
				else:
					d_format = '%Y/%m/%d:%H:%M:%S'
					try:
						kwargs['timestamp'] = datetime.strptime(
							kwargs['timestamp'], d_format)
					except ValueError:
						print(
							f'Invalid timestamp! Must be UNIX timestamp or match the format "{d_format}"')
						continue

				insta.add_post(**kwargs)

		elif action == 'purge':
			insta.purge_old_medias()

		elif action == 'delete':
			insta.delete_medias()

		exit()

	insta = InstaAPI(files_root=os.path.abspath('insta_data'))
	# insta.get_tracked_medias()
	insta.schedule()

	# for e in get_reels_tray(insta.cl):
	# 	pass

	# threads = insta.list_threads()
	que = []
	t_id = '340282366841710300949128175644303772676'
	messages = insta.cl.direct_messages(t_id)
	for msg in messages:
		if msg.item_type == 'raven_media':
			ddl_raven_media(msg.visual_media['media'])
		elif msg.item_type == 'media':
			m = msg.media
			if m.media_type == 1:
				url = m.thumbnail_url
			elif m.media_type == 2:
				url = m.video_url
			else:
				print(f'Unknown media type: {m.media_type}')
				continue
			ddl_thread_media(url)

	pass
	# for thread in list_threads():
	#     print(thread.id, thread.thread_title)
	# data = cl.get_timeline_feed()
