import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

class Config(object):
    # main variables (DO NOT CHANGE)
    output_dir = Path(os.getcwd(), 'output')

    # main parameters
    n_pagin = 500
    pagin = 'per_page={}'.format(n_pagin)
    baseurl = 'http://api.repo.nypl.org/api/v1/'
    trail_url = '/search?&publicDomainOnly=true'
    # token = 'xxxxx'
    token = os.getenv('token')
    auth = 'Token token=' + token
