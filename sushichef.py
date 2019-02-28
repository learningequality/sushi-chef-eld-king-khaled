#!/usr/bin/env python

from bs4 import BeautifulSoup
import codecs
from collections import defaultdict, OrderedDict
import copy
import glob
from le_utils.constants import licenses, content_kinds, file_formats
import hashlib
import json
import logging
import ntpath
import os
from pathlib import Path
import re
import requests
from ricecooker.classes.licenses import get_license
from ricecooker.chefs import JsonTreeChef
from ricecooker.utils import downloader, html_writer
from ricecooker.utils.caching import CacheForeverHeuristic, FileCache, CacheControlAdapter
from ricecooker.utils.jsontrees import write_tree_to_json_tree, SUBTITLES_FILE
import time
from urllib.error import URLError
from urllib.parse import urljoin
from utils import if_dir_exists, get_name_from_url, clone_repo, build_path
from utils import if_file_exists, get_video_resolution_format, remove_links
from utils import get_name_from_url_no_ext, get_node_from_channel, get_level_map
from utils import remove_iframes, get_confirm_token, save_response_content
import youtube_dl


BASE_URL = "https://www.youtube.com/user/kkudl/playlists"

DATA_DIR = "chefdata"
COPYRIGHT_HOLDER = "King Khaled University in Abha, Saudi Arabia"
LICENSE = get_license(licenses.CC_BY, 
        copyright_holder=COPYRIGHT_HOLDER).as_dict()
AUTHOR = "King Khaled University in Abha, Saudi Arabia"

LOGGER = logging.getLogger()
__logging_handler = logging.StreamHandler()
LOGGER.addHandler(__logging_handler)
LOGGER.setLevel(logging.INFO)

DOWNLOAD_VIDEOS = True
LOAD_VIDEO_LIST = False

sess = requests.Session()
cache = FileCache('.webcache')
basic_adapter = CacheControlAdapter(cache=cache)
forever_adapter = CacheControlAdapter(heuristic=CacheForeverHeuristic(), cache=cache)
sess.mount('http://', basic_adapter)
sess.mount(BASE_URL, forever_adapter)

# Run constants
################################################################################
#CHANNEL_NAME = ""              # Name of channel
#CHANNEL_SOURCE_ID = ""    # Channel's unique id
CHANNEL_DOMAIN = "https://www.youtube.com/user/kkudl/playlists"          # Who is providing the content
CHANNEL_LANGUAGE = "ar"      # Language of channel
CHANNEL_DESCRIPTION = None                                  # Description of the channel (optional)
CHANNEL_THUMBNAIL = "https://yt3.ggpht.com/a-/AN66SAz9fwCzHEBXcCczoBEGfXr7xKzhooqj0yqVwQ=s288-mo-c-c0xffffffff-rj-k-no"                                    # Local path or url to image file (optional)

# Additional constants
################################################################################

def title_has_numeration(title):
    unit_name_ar = ["الوحده", "الوحدة"]
    for unit_name in unit_name_ar:
        if unit_name in title:
            index = title.find(unit_name)
            match = re.search("(?P<int>\d+)", title)
            if match:
                num = int(match.group("int"))
                return title[index: index+len(unit_name)] + " " + str(num), num
            else:
                return title[index: index+len(unit_name)], None 
    
    numbers = list(map(str, [1,2,3,4,5,6,7,8,9]))
    arab_nums = ["١", "٢", "٣", "٤", "٥"]
    title = title.replace("-", " ")
    for elem in title.split(" "):
        elem = elem.strip()
        for num in numbers:
            if elem == num:
                return title.replace(elem, "").strip(), int(num)
    
    for arab_num in title:
        index = title.find(arab_num)
        if index != -1 and index >= len(title) - 1:
            return title.replace(arab_num, "").strip(), 1
    
    return False, None


def title_patterns(title):
    title = re.sub(' +', ' ' , title)
    pattern01 = r"\d+\-\d+"
    match = re.search(pattern01, title)
    if match:
        index = match.span()
        numbers = title[index[0]:index[1]]
        number_unit = numbers.split("-")[0].strip()
        return "Unit {}".format(number_unit), int(number_unit)
    
    pattern02 = r"\d+\s+\d+"
    match = re.search(pattern02, title)
    if match:
        index = match.span()
        numbers = title[index[0]:index[1]]
        number_unit = int(title[index[1]:].strip())
        return "Unit {}".format(number), number_unit
    
    title_unit, unit_num = title_has_numeration(title)
    if title_unit is not False and unit_num is not None:
        return title_unit, unit_num
    elif title_unit is not False and unit_num is None:
        return title_unit, 1
    else:
        return title, 1


def remove_units_number(title):
    match = re.search(r'\|.*\|', title)
    if match:
        index = match.span()
        new_title = "{} | {}".format(title[:index[0]].strip(), title[index[1]:].strip())
        return new_title.strip()
    return title


def remove_special_case(title):
    title = title.replace("مهارات في علم الرياضيات", "")
    title = title.replace("-", "")
    return title.strip()


class Node(object):
    def __init__(self, title, source_id, lang="en"):
        self.title = title
        self.source_id = source_id
        self.tree_nodes = OrderedDict()
        self.lang = lang
        self.description = None

    def add_node(self, obj):
        node = obj.to_node()
        if node is not None:
            self.tree_nodes[node["source_id"]] = node

    def to_node(self):
        return dict(
            kind=content_kinds.TOPIC,
            source_id=self.source_id,
            title=self.title,
            description=self.description,
            language=self.lang,
            author=AUTHOR,
            license=LICENSE,
            children=list(self.tree_nodes.values())
        )
    

class Subject(Node):
    def __init__(self, *args, **kwargs):
        super(Subject, self).__init__(*args, **kwargs)
        self.topics = []

    def load(self, filename, auto_parse=False):
        with open(filename, "r") as f:
            topics = json.load(f)
            for topic in topics:
                topic_obj = Topic(topic["title"], topic["source_id"], lang=CHANNEL_LANGUAGE)
                for unit in topic["units"]:
                    units = Topic.auto_generate_units(unit["source_id"], 
                        title=unit["title"], lang=unit["lang"], 
                        auto_parse=auto_parse, only_folder_name=unit.get("only", None))
                    topic_obj.units.extend(units)
                self.topics.append(topic_obj)


class Topic(Node):
    def __init__(self, *args, **kwargs):
        super(Topic, self).__init__(*args, **kwargs)
        self.units = []

    @staticmethod
    def auto_generate_units(url, title=None, lang="en", auto_parse=False, only_folder_name=None):
        youtube = YouTubeResource(url)
        units = defaultdict(list)
        if title is not None:
            if only_folder_name is not None:
                for subtitle, url in youtube.playlist_name_links():
                    if subtitle.startswith(only_folder_name):
                        units[title].append((1, url))
            else:
                for _, url in youtube.playlist_name_links():
                    units[title].append((1, url))
        else:
            for name, url in youtube.playlist_name_links():
                unit_name_list = name.split("|")
                if len(unit_name_list) > 1 and auto_parse is False:
                    unit = unit_name_list[1]
                    unit_name = unit.strip().split(" ")[0]
                    number_unit = 1
                else:
                    unit_name, number_unit = title_patterns(name)
                units[unit_name].append((number_unit, url))

        units = sorted(units.items(), key=lambda x: x[1][0], reverse=False)
        for title, urls in units:
            unit = Unit(title, title, lang=lang)
            unit.urls = [url for _, url in urls]
            yield unit


class Unit(Node):
    def __init__(self, *args, **kwargs):
        super(Unit, self).__init__(*args, **kwargs)
        self.urls = []

    def download(self, download=True, base_path=None):
        for url in self.urls:
            youtube = YouTubeResource(url, lang=self.lang)
            youtube.download(download, base_path)
            youtube.title = remove_special_case(remove_units_number(youtube.title))
            self.add_node(youtube)

    def to_node(self):
        children = list(self.tree_nodes.values())
        if len(children) == 1:
            return children[0]
        else:
            return dict(
                kind=content_kinds.TOPIC,
                source_id=self.source_id,
                title=self.title,
                description=self.description,
                language=self.lang,
                author=AUTHOR,
                license=LICENSE,
                children=children
            )


class YouTubeResource(object):
    def __init__(self, source_id, name=None, type_name="Youtube", lang="ar", 
            embeded=False, section_title=None):
        LOGGER.info("    + Resource Type: {}".format(type_name))
        LOGGER.info("    - URL: {}".format(source_id))
        self.filename = None
        self.type_name = type_name
        self.filepath = None
        self.name = name
        self.section_title = section_title
        if embeded is True:
            self.source_id = YouTubeResource.transform_embed(source_id)
        else:
            self.source_id = self.clean_url(source_id)
        self.file_format = file_formats.MP4
        self.lang = lang
        self.is_valid = False

    def clean_url(self, url):
        if url[-1] == "/":
            url = url[:-1]
        return url.strip()

    @property
    def title(self):
        return self.name if self.name is not None else self.filename

    @title.setter
    def title(self, v):
        if self.name is not None:
            self.name = v
        else:
            self.filename = v

    @classmethod
    def is_youtube(self, url, get_channel=False):
        youtube = url.find("youtube") != -1 or url.find("youtu.be") != -1
        if get_channel is False:
            youtube = youtube and url.find("user") == -1 and url.find("/c/") == -1
        return youtube

    @classmethod
    def transform_embed(self, url):
        url = "".join(url.split("?")[:1])
        return url.replace("embed/", "watch?v=").strip()

    def playlist_links(self):
        ydl_options = {
                'no_warnings': True,
                'restrictfilenames':True,
                'continuedl': True,
                'quiet': False,
                'format': "bestvideo[height<={maxheight}][ext=mp4]+bestaudio[ext=m4a]/best[height<={maxheight}][ext=mp4]".format(maxheight='480'),
                'noplaylist': False
            }

        playlist_videos_url = []
        with youtube_dl.YoutubeDL(ydl_options) as ydl:
            try:
                ydl.add_default_info_extractors()
                info = ydl.extract_info(self.source_id, download=False)
                for entry in info["entries"]:
                    playlist_videos_url.append(entry["webpage_url"])
            except(youtube_dl.utils.DownloadError, youtube_dl.utils.ContentTooShortError,
                    youtube_dl.utils.ExtractorError) as e:
                LOGGER.info('An error occured ' + str(e))
                LOGGER.info(self.source_id)
            except KeyError as e:
                LOGGER.info(str(e))
        return playlist_videos_url

    def playlist_name_links(self):
        name_url = []
        source_id_hash = hashlib.sha1(self.source_id.encode("utf-8")).hexdigest()
        base_path = build_path([DATA_DIR, CHANNEL_SOURCE_ID])
        videos_url_path = os.path.join(base_path, "{}.json".format(source_id_hash))

        if if_file_exists(videos_url_path) and LOAD_VIDEO_LIST is True:
            with open(videos_url_path, "r") as f:
                name_url = json.load(f)
        else:
            for url in self.playlist_links():
                youtube = YouTubeResource(url)
                info = youtube.get_video_info(None, False)
                name_url.append((info["title"], url))
            with open(videos_url_path, "w") as f:
                json.dump(name_url, f)
        return name_url

    def get_video_info(self, download_to=None, subtitles=True):
        ydl_options = {
                'writesubtitles': subtitles,
                'allsubtitles': subtitles,
                'no_warnings': True,
                'restrictfilenames':True,
                'continuedl': True,
                'quiet': False,
                'format': "bestvideo[height<={maxheight}][ext=mp4]+bestaudio[ext=m4a]/best[height<={maxheight}][ext=mp4]".format(maxheight='480'),
                'outtmpl': '{}/%(id)s'.format(download_to),
                'noplaylist': True
            }

        with youtube_dl.YoutubeDL(ydl_options) as ydl:
            try:
                ydl.add_default_info_extractors()
                info = ydl.extract_info(self.source_id, download=(download_to is not None))
                return info
            except(youtube_dl.utils.DownloadError, youtube_dl.utils.ContentTooShortError,
                    youtube_dl.utils.ExtractorError) as e:
                LOGGER.info('An error occured ' + str(e))
                LOGGER.info(self.source_id)
            except KeyError as e:
                LOGGER.info(str(e))

    def subtitles_dict(self):
        subs = []
        video_info = self.get_video_info()
        if video_info is not None:
            video_id = video_info["id"]
            if 'subtitles' in video_info:
                subtitles_info = video_info["subtitles"]
                for language in subtitles_info.keys():
                    subs.append(dict(file_type=SUBTITLES_FILE, youtube_id=video_id, language=language))
        return subs

    #youtubedl has some troubles downloading videos in youtube,
    #sometimes raises connection error
    #for that I choose pafy for downloading
    def download(self, download=True, base_path=None):
        if not "watch?" in self.source_id or "/user/" in self.source_id or\
            download is False:
            return

        download_to = build_path([base_path, 'videos'])
        for i in range(4):
            try:
                info = self.get_video_info(download_to=download_to, subtitles=False)
                if info is not None:
                    LOGGER.info("    + Video resolution: {}x{}".format(info.get("width", ""), info.get("height", "")))
                    self.filepath = os.path.join(download_to, "{}.mp4".format(info["id"]))
                    self.filename = info["title"]
                    if self.filepath is not None and os.stat(self.filepath).st_size == 0:
                        LOGGER.info("    + Empty file")
                        self.filepath = None
            except (ValueError, IOError, OSError, URLError, ConnectionResetError) as e:
                LOGGER.info(e)
                LOGGER.info("Download retry")
                time.sleep(.8)
            except (youtube_dl.utils.DownloadError, youtube_dl.utils.ContentTooShortError,
                    youtube_dl.utils.ExtractorError, OSError) as e:
                LOGGER.info("     + An error ocurred, may be the video is not available.")
                return
            except OSError:
                return
            else:
                return

    def to_node(self):
        if self.filepath is not None:
            files = [dict(file_type=content_kinds.VIDEO, path=self.filepath)]
            files += self.subtitles_dict()
            node = dict(
                kind=content_kinds.VIDEO,
                source_id=self.source_id,
                title=self.title,
                description='',
                author=AUTHOR,
                files=files,
                language=self.lang,
                license=LICENSE
            )
            return node


# The chef subclass
################################################################################
class KingKhaledChef(JsonTreeChef):
    HOSTNAME = BASE_URL
    TREES_DATA_DIR = os.path.join(DATA_DIR, 'trees')

    def __init__(self):
        build_path([KingKhaledChef.TREES_DATA_DIR])
        super(KingKhaledChef, self).__init__()

    def pre_run(self, args, options):
        channel_tree = self.scrape(args, options)
        self.write_tree_to_json(channel_tree)

    def k12_lessons(self):
        global CHANNEL_SOURCE_ID
        self.RICECOOKER_JSON_TREE = 'ricecooker_json_tree_k12.json'
        CHANNEL_NAME = "ELD King Khaled University Learning (العربيّة)"
        CHANNEL_SOURCE_ID = "sushi-chef-eld-k12-ar"
        channel_tree = dict(
                source_domain=KingKhaledChef.HOSTNAME,
                source_id=CHANNEL_SOURCE_ID,
                title=CHANNEL_NAME,
                description="""تحتوي هذه القناة على مجموعة من الدروس في اللغة العربية والتجويد واللغة الإنجليزية والرياضيات الأساسية، وهي مجموعة من المقررات التي صممتها جامعة الملك خالد كجزء من مقرراتها الرقمية المفتوحة. وتناسب هذه الدروس طلاب المرحلة الثانوية ويمكن ملاءمتها مع بعض الصفوف للمرحلة الإعدادية أيضاً."""
[:400], #400 UPPER LIMIT characters allowed 
                thumbnail=CHANNEL_THUMBNAIL,
                author=AUTHOR,
                language=CHANNEL_LANGUAGE,
                children=[],
                license=LICENSE,
            )
        subject_en = Subject(title="English Language Skills اللغة الإنجليزية", 
                            source_id="English Language Skills اللغة الإنجليزية")
        subject_en.load("resources_en_lang_skills.json")

        subject_ar = Subject(title="Arabic Language Skills اللغة العربية", 
                            source_id="Arabic Language Skills اللغة الإنجليزية")
        subject_ar.load("resources_ar_lang_skills.json")

        subject_ar_st = Subject(title="Islamic Studies الثقافة الإسلامية", 
                            source_id="Islamic Studies الثقافة الإسلامية")
        subject_ar_st.load("resources_ar_islamic_studies.json")

        subject_ar_math = Subject(title="Math الرياضيات", 
                            source_id="Math الرياضيات")
        subject_ar_math.load("resources_ar_math.json")

        subjects = [subject_en, subject_ar, subject_ar_st, subject_ar_math]
        return channel_tree, subjects

    def intermediate_lessons(self):
        global CHANNEL_SOURCE_ID
        self.RICECOOKER_JSON_TREE = 'ricecooker_json_tree_professional.json'
        CHANNEL_NAME = "ELD Teacher Professional Development Cources (العربيّة)"
        CHANNEL_SOURCE_ID = "sushi-chef-eld-teacher-prof-dev-ar"
        channel_tree = dict(
                source_domain=KingKhaledChef.HOSTNAME,
                source_id=CHANNEL_SOURCE_ID,
                title=CHANNEL_NAME,
                description="""تحتوي هذه القناة على مجموعة من الدورات الملائمة للمعلمين  في مجالات التربية والتعليم ومهارات تدريس اللغة العربية والإدارة المدرسية والمناهج. وهي مجموعة من المقررات التي صممتها جامعة الملك خالد كجزء من مقرراتها الرقمية المفتوحة. وتناسب هذه المحاضرات جميع المعلمين بمختلف تخصصاتهم."""
[:400], #400 UPPER LIMIT characters allowed 
                thumbnail=CHANNEL_THUMBNAIL,
                author=AUTHOR,
                language=CHANNEL_LANGUAGE,
                children=[],
                license=LICENSE,
            )

        subject_sedu = Subject(title="التربية الخاصة Special Education", 
                            source_id="التربية الخاصة Special Education")
        subject_sedu.load("resources_ar_special_education.json", auto_parse=True)

        subject_about_edu = Subject(title="في التربية والتعليم About Education and Schooling",
                                source_id="في التربية والتعليم About Education and Schooling")
        subject_about_edu.load("resources_ar_about_education.json", auto_parse=True)

        subject_teaching = Subject(title="مناهج وتدريس Teaching and Curriculum",
                                source_id="مناهج وتدريس Teaching and Curriculum")
        subject_teaching.load("resources_ar_teaching.json", auto_parse=True)
        subjects = [subject_sedu, subject_about_edu, subject_teaching]
        return channel_tree, subjects

    def scrape(self, args, options):
        download_video = options.get('--download-video', "1")
        basic_lessons = int(options.get('--basic-lessons', "0"))
        intermedian_lessons = int(options.get('--intermedian-lessons', "0"))
        load_video_list = options.get('--load-video-list', "0")

        if int(download_video) == 0:
            global DOWNLOAD_VIDEOS
            DOWNLOAD_VIDEOS = False

        if int(load_video_list) == 1:
            global LOAD_VIDEO_LIST
            LOAD_VIDEO_LIST = True

        global channel_tree
        if basic_lessons == 1:
            channel_tree, subjects = self.k12_lessons()
        elif intermedian_lessons == 1:
            channel_tree, subjects = self.intermediate_lessons()

        base_path = [DATA_DIR] + ["King Khaled University in Abha"]
        base_path = build_path(base_path)

        for subject in subjects:
            for topic in subject.topics:
                for unit in topic.units:
                    unit.download(download=DOWNLOAD_VIDEOS, base_path=base_path)
                    topic.add_node(unit)
                subject.add_node(topic)
            channel_tree["children"].append(subject.to_node())
        
        return channel_tree

    def write_tree_to_json(self, channel_tree):
        scrape_stage = os.path.join(KingKhaledChef.TREES_DATA_DIR, 
                                self.RICECOOKER_JSON_TREE)
        write_tree_to_json_tree(scrape_stage, channel_tree)


# CLI
################################################################################
if __name__ == '__main__':
    chef = KingKhaledChef()
    chef.main()
