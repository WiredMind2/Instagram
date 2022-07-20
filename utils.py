from fileinput import filename
import json
import os
import random
import time
from urllib.parse import urlparse

import requests
from instagrapi import config
from instagrapi.exceptions import (ClientError, ClientLoginRequired,
								   ClientNotFoundError, DirectThreadNotFound,
								   UnknownError, UserNotFound)
from instagrapi.extractors import (extract_media_v1, extract_story_v1,
								   extract_user_short)
from instagrapi.utils import json_value

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
	# None would match with post.get('pk') if 'pk' doesn't exists
	last_post_pk = -1

	while True:
		headers = {
			"X-Ads-Opt-Out": "0",
			"X-DEVICE-ID": self.uuid,
			# str(random.randint(2000, 5000)),
			"X-CM-Bandwidth-KBPS": '-1.000',
			"X-CM-Latency": str(random.randint(1, 5)),
		}
		params = {
			'_uuid': self.uuid,
			'_csrftoken': self.token,
			'is_prefetch': '0',
			'is_pull_to_refresh': '0',
			'phone_id': self.phone_id,
			'timezone_offset': str(self.timezone_offset),
		}
		data = self.private_request(
			"feed/timeline/", json.dumps(params), with_signature=False, headers=headers
		)
		for item in data.get('feed_items', []):
			post = item.get('media_or_ad')
			# TODO - Do something with 'suggested_users'
			if post:
				if post.get('product_type') == 'ad':
					continue  # Ignore ads
				yield extract_media_v1(post)

		if post.get('pk') == last_post_pk:
			break
		else:
			last_post_pk = post.get('pk')

		if data.get('more_available', False):
			next_max_id = data.get('next_max_id')

			params['max_id'] = next_max_id
		else:
			break


def get_reels_tray(self, tracked_ids=None):
	# Fetch recent stories
	# tracked_ids (Optionnal) - Filter stories based on user id
	REELS_COUNT = 3  # How many reels to fetch at once
	to_fetch = []
	user_cache = {}

	params = {
		"supported_capabilities_new": config.SUPPORTED_CAPABILITIES,
		"timezone_offset": str(self.timezone_offset),
		"tray_session_id": self.tray_session_id,
		"request_id": self.request_id,
		"latest_preloaded_reel_ids": "[]",  # [{"reel_id":"6009504750","media_count":"15","timestamp":1628253494,"media_ids":"[\"2634301737009283814\",\"2634301789371018685\",\"2634301853921370532\",\"2634301920174570551\",\"2634301973895112725\",\"2634302037581608844\",\"2634302088273817272\",\"2634302822117736694\",\"2634303181452199341\",\"2634303245482345741\",\"2634303317473473894\",\"2634303382971517344\",\"2634303441062726263\",\"2634303502039423893\",\"2634303754729475501\"]"},{"reel_id":"4357392188","media_count":"4","timestamp":1628250613,"media_ids":"[\"2634142331579781054\",\"2634142839803515356\",\"2634150786575125861\",\"2634279566740346641\"]"},{"reel_id":"5931631205","media_count":"7","timestamp":1628253023,"media_ids":"[\"2633699694927154768\",\"2634153361241413763\",\"2634196788830183839\",\"2634219197377323622\",\"2634294221109889541\",\"2634299705648894876\",\"2634299760434939842\"]"}],
		"page_size": 50,
		"_csrftoken": self.token,
		"_uuid": self.uuid,
	}
	data = self.private_request("feed/reels_tray/", params)

	for story in data['tray']:
		user = extract_user_short(story['user'])

		if tracked_ids and int(user.pk) not in tracked_ids:
			continue

		user_cache[user.pk] = user

		if user.username is None:
			user_cache[user.pk] = self.user_info(user.pk)

		if not 'items' in story:
			# TODO - Check if files already exists
			# for media_id in story['media_ids']: 
			# 	filename = f"{story['latest_reel_media']}-{media_id}"
			to_fetch.append(story['id'])

			if len(to_fetch) == REELS_COUNT:
				time.sleep(0.5)
				for reel in reel_info_v1(self, to_fetch):

					if reel.user.pk not in user_cache:
						user_cache[reel.user.pk] = self.user_info(reel.user.pk)

					reel.user = user_cache[reel.user.pk]
					yield reel
				to_fetch = []

		else:
			for item in story['items']:
				reel = extract_story_v1(item)

				if tracked_ids and int(reel.user.pk) not in tracked_ids:
					continue

				if reel.user.pk not in user_cache:
					user_cache[reel.user.pk] = self.user_info(reel.user.pk)

				reel.user = user_cache[reel.user.pk]
				yield reel


	pks = data['remaining_reel_ids_to_fetch']
	n = 3
	for chunk in [pks[i:i + n] for i in range(0, len(pks), n)]:
		for reel in reel_info_v1(self, chunk):
			if reel.user.pk in user_cache:
				reel.user = user_cache[reel.user.pk]
			# Most of the time, the user will not be in user cache
			# We must fetch data from database, outside of this function
			yield reel


def reel_info_v1(self, pks):
	try:
		result = self.private_request(f'feed/reels_media', params={'reel_ids': list(pks)})
	except Exception as e:
		print(f'Error on reel_info_v1({pks}): {e}')
		return

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


def ddl_raven_media(media, filename=None):
	if isinstance(media, str):
		urls = [media]
	else:

		imgs = media.get('image_versions2')

		if not imgs:
			return
		
		urls = [candidate['url'] for candidate in imgs['candidates']]

	empty = True
	for url in urls:
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

	if filename is None:
		root = os.path.abspath(os.path.join(
			'./insta_data',
			'threads',
			media["user"]["username"]))
	else:
		root = ''
	
	create_dir(root)

	return save_media(url, req, filename or '', root=root)


def save_media(url, req, filename='', root=''):
	fname = urlparse(url).path.rsplit("/", 1)[1].strip()
	filename = "%s.%s" % (filename, fname.rsplit(".", 1)
						  [1]) if filename else fname

	path = os.path.join(root, filename)

	with open(path, "wb") as f:
		for chunk in req.iter_content(chunk_size=8192):
			f.write(chunk)

	return path

# Utils

def get_media_folder(m):
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

def create_dir(path):
	if not os.path.exists(path):
		root = os.path.dirname(path)
		if not os.path.exists(root):
			create_dir(root)
		os.mkdir(path)