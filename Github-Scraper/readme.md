

## Features

- Collects repository name, star count, and primary language
- Configurable via YAML file
- Implements rate limiting to respect GitHub API constraints
- Supports checkpointing to resume interrupted operations
- Outputs data to a CSV file

## Requirements

- Python 3.6+
- `requests` library
- `PyYAML` library
- `tqdm` library

## Installation

#### 1. Clone this repository:
git clone https://github.com/ola-silicon/data-scraper-all/new/main/Github-Scraper

#### 2. Install the required dependencies:
pip install -r requirements.txt

## Configuration

Create a `config.yaml` file in the project root with the following structure:


```csv_file: 'github_repositories.csv'
checkpoint_file: 'repo_ckpt.pkl'
output_dir: ''  # or your desired local output directory
num_jobs: 1  # Number of parallel jobs to execute
batch_size: # specify batch size according to VM configuration
user: 'your_github_username'
token: 'your_github_access_token'
aws_access_key: ''  # Optional: for future AWS integration
aws_secret_key: ''  # Optional: for future AWS integration
aws_region: ''  # Optional: for future AWS integration
```
### Replace your_github_username and your_github_access_token with your actual GitHub credentials.

### Usage

``` ScrapeRepoInfo.py```

#### The script : 

Load configuration from config.yaml
Search for repositories on GitHub based on size and star count
Collect information about up to 10 repositories
Save the data to the specified CSV file
Create a checkpoint file to allow resuming the operation if interrupted

``` downloadRepos.py ```

#### The script: 

Goes through out the .csv file. Scraps all the repositories and stores them on local for temporary, zips the folder with particular repo name, and then uploads to s3 and removes from it from local, provide proper num_jobs according to VM configuration
Zip them in a folder


### Output
The script ``` ScrapeRepoInfo.py``` generates a CSV file (default: github_repositories.csv) with the following columns:

#### Repository name (full name including owner)
#### Star count
#### Primary language

### Checkpointing
If the script is interrupted, it will save its progress in a checkpoint file (default: repo_ckpt.pkl). When restarted, it will automatically resume from the last saved checkpoint.

### Rate Limiting
The script respects GitHub's API rate limits by pausing when necessary. It will automatically resume after the rate limit resets.

### Error Handling
The script includes basic error handling for common issues such as API rate limit exceeded and unexpected HTTP status codes.


