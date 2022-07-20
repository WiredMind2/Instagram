import json
import os

def init(local=None):
    global SEND_DC_ARCHIVES, LOCAL, BASE_URL, SETTINGS, SETTINGS_FILE
    const = ('SEND_DC_ARCHIVES', 'LOCAL', 'BASE_URL', 'SETTINGS', 'SETTINGS_FILE')

    LOCAL = local or False
    SEND_DC_ARCHIVES = False
    BASE_URL = "http://localhost/instagram" if LOCAL else "https://www.tetrazero.com/instagram"

    SETTINGS_FILE = './settings.json'
    if not os.path.exists(SETTINGS_FILE):
        open(SETTINGS_FILE, 'x').close()
        print('Please set new settings')
        exit()

    with open(SETTINGS_FILE, 'r', encoding='utf-8') as f:
        SETTINGS = json.load(f)

    # for key in const:
    #     globals()[key] = eval(key)