import os
import json
import time
import math
import pickle
import requests
from tqdm import tqdm
import yaml

# Load configuration from config.yaml
with open('config.yaml', 'r') as config_file:
    config = yaml.safe_load(config_file)

# Extract credentials and settings from config
USER = config['user']
TOKEN = config['token']
CHECKPOINT_FILE = config['checkpoint_file']
OUTPUT_FILE = os.path.join(config['output_dir'], config['csv_file'])

REMAINING_REQUESTS = 30

def save_ckpt(lower_bound: int, upper_bound: int):
    global repo_list
    repo_list = list(set(repo_list))  # remove duplicates
    print(f"Saving checkpoint {lower_bound, upper_bound}...")
    with open(CHECKPOINT_FILE, 'wb') as f:
        pickle.dump((lower_bound, upper_bound, repo_list), f)

def get_request(lower_bound: int, upper_bound: int, page: int = 1):
    global REMAINING_REQUESTS, USER, TOKEN, repo_list
    r = requests.get(
        f'https://api.github.com/search/repositories?q=size:{lower_bound}..{upper_bound}+stars:>100&per_page=10&page={page}',
        auth=(USER, TOKEN)
    )

    if r.status_code == 403:
        print('API rate limit exceeded.')
        save_ckpt(lower_bound, upper_bound)
        print('Exiting program.')
        exit()
    elif r.status_code == 422:
        return False

    try:
        assert r.status_code == 200
    except:
        print(f'Unexpected status code. Status code returned is {r.status_code}')
        print(r.text)
        save_ckpt(lower_bound, upper_bound)
        print("Exiting program.")
        exit()
    
    REMAINING_REQUESTS -= 1

    if REMAINING_REQUESTS == 0:
        print("Sleeping 60 seconds to stay under GitHub API rate limit...")
        time.sleep(60)
        save_ckpt(lower_bound, upper_bound)
        REMAINING_REQUESTS = 30

    return r

def download_range(lower_bound, upper_bound):
    global repo_list
    for page in range(1, 3):
        r = get_request(lower_bound=lower_bound, upper_bound=upper_bound, page=page)
        
        if not r:
            return

        if page == 1:
            n_results = r.json()['total_count']
            n_query_pages = min(math.ceil(n_results/100), 10)

        for repository in r.json()['items']:
            name = repository['full_name']
            stars = repository['stargazers_count']
            lang = repository['language']
            repo_list.append((name, stars, lang))
            
            if len(repo_list) >= 10:
                return

        if page >= n_query_pages:
            return

if __name__ == '__main__':
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'rb') as f:
            lower_bound, upper_bound, repo_list = pickle.load(f)
        print(f"Loading from {lower_bound}..{upper_bound}")
    else:
        lower_bound = 0
        upper_bound = 5
        repo_list = []

    if lower_bound >= 100:
        print('''
Checkpoint is for an already completed download of GitHub repository information.
Please delete the checkpoint file to restart and try again.
            ''')
        exit()

    repo_count = 0
    while lower_bound < 100 and repo_count < 10:  # Changed upper limit to 1000 MB (roughly 1 GB)
        upper_bound = max(lower_bound + 1, min(100, lower_bound + 10))  # Simplified range adjustment
        
        print(f'Querying size {lower_bound}..{upper_bound}')
        r = get_request(lower_bound, upper_bound)
        if not r:
            break
        n_results = r.json()['total_count']
        
        if n_results > 0:
            print(f"Downloading repositories in size range {lower_bound}..{upper_bound}")
            download_range(lower_bound, upper_bound)
            repo_count = len(repo_list)
            print(f"Total repositories collected: {repo_count}")
        
        lower_bound = upper_bound + 1

    save_ckpt(lower_bound, upper_bound)

    with open(OUTPUT_FILE, 'w') as f:
        for repo in repo_list[:10]:  # Limit to 10 repositories in the output
            name, stars, lang = repo
            f.write(f'{name},{stars},{lang}\n')

    print(f"Collected information for {len(repo_list[:10])} repositories.")