
import argparse
import requests
import sys

url_template = 'https://hub.docker.com/v2/repositories/%s/tags?page_size=100'

def main(args):
    url = url_template % args.r
    tags_data = requests.get(url).json()
    tag = sorted(tags_data['results'], key=lambda r: r['last_updated'], reverse=True)
    if args.f:
        tag = [value for value in tag if args.f in value['name']]
    sys.stdout.write(tag[0]['name'])

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-r', required=True)
    parser.add_argument('-f')
    args = parser.parse_args()
    main(args)
