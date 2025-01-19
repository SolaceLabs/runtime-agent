import requests
import json
import os
import boto3
import textwrap

PRISMA_ROOT_API_URL = os.getenv("PRISMA_ROOT_API_URL")
PRISMA_AUTHENTICATE_URL = f"{PRISMA_ROOT_API_URL}/authenticate"
PRISMA_REGISTRY_SCAN_RESULT_URL = f"{PRISMA_ROOT_API_URL}/registry"
PRISMA_ACCESS_KEY = os.getenv('PRISMA_ACCESS_KEY')
PRISMA_ACCESS_KEY_SECRET = os.getenv('PRISMA_ACCESS_KEY_SECRET')
HTTP_JSON_HEADER = 'application/json'
# DOCKER_IMAGE_TO_CHECK is the repository name full and the tag that is used to scan it
DOCKER_IMAGE_TO_CHECK = os.getenv("DOCKER_IMAGE_TO_CHECK")
PRISMA_BLOCKING_VULNERABILITIES = "critical,high"


def get_excluded_packages():
    dynamodb_client = boto3.resource('dynamodb')

    whitesource_exclusion_table = dynamodb_client.Table('prisma-excluded-packages')
    whitesource_exclusion_entries = whitesource_exclusion_table.scan()['Items']

    exclusions = set()
    for exclusion in whitesource_exclusion_entries:
        exclusions.add(exclusion['packageName'])

    return exclusions


def block_print(long_string, each_line_length=75):
    print("\n\t".join(textwrap.wrap(long_string, each_line_length)))


def find_all_high_critical_vulnerabilities_to_resolve(excluded_libraries):
    # Authenticate
    authenticate_payload = {
        'username': PRISMA_ACCESS_KEY,
        'password': PRISMA_ACCESS_KEY_SECRET
    }
    authenticate_headers = {'Accept': HTTP_JSON_HEADER, 'Content-type': HTTP_JSON_HEADER}

    get_token_request = requests.post(
        PRISMA_AUTHENTICATE_URL,
        data=json.dumps(authenticate_payload),
        headers=authenticate_headers
    )
    token_response = json.loads(get_token_request.text)
    scan_result_token = token_response['token']

    # Get scan results
    scan_result_header = {
        'accept': HTTP_JSON_HEADER,
        'content-type': HTTP_JSON_HEADER,
        'Authorization': f'Bearer {scan_result_token}'
    }
    prisma_scan_result_request = requests.get(
        f'{PRISMA_REGISTRY_SCAN_RESULT_URL}?name={DOCKER_IMAGE_TO_CHECK}',
        data=json.dumps(authenticate_payload),
        headers=scan_result_header
    )

    prisma_project_vulnerabilities = prisma_scan_result_request.json()[0].get('vulnerabilities')
    vulnerabilities_to_resolve = dict()
    for vulnerability in prisma_project_vulnerabilities:
        if vulnerability['severity'] in PRISMA_BLOCKING_VULNERABILITIES:
            package_full_name = f"{vulnerability['packageName']}:{vulnerability['packageVersion']}"
            current_vulnerability_description = vulnerability['description']
            if package_full_name in excluded_libraries:
                print(f"ⓘ Package {package_full_name} has vulnerabilities but is in exclusion list.")
                print(f"vulnerability details: {vulnerability}")
            else:
                if package_full_name not in vulnerabilities_to_resolve:
                    vulnerabilities_to_resolve[package_full_name] = [current_vulnerability_description]
                else:
                    vulnerabilities_to_resolve[package_full_name].append(current_vulnerability_description)
    return vulnerabilities_to_resolve


prisma_exclusion_list = get_excluded_packages()
prisma_vulnerabilities_to_resolve = find_all_high_critical_vulnerabilities_to_resolve(prisma_exclusion_list)
if len(prisma_vulnerabilities_to_resolve) != 0:
    print(f"❌ Following {PRISMA_BLOCKING_VULNERABILITIES} Prisma vulnerabilities should get resolved before release: ")
    for vulnerability_name in prisma_vulnerabilities_to_resolve:
        print(f"🔴️ {vulnerability_name}")
        # print descriptions for this vulnerability
        for vulnerability_description in prisma_vulnerabilities_to_resolve[vulnerability_name]:
            block_print(f'\t ➡️ {vulnerability_description}\n')
    exit(1)
else:
    print(f"No {PRISMA_BLOCKING_VULNERABILITIES} Prisma vulnerabilities found! ✅")
