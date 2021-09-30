import json
import requests
import os

from config import Config

def app(config):

    url_auth = config['url_auth']
    headers = {'content-type': 'application/json'}
    data = {"username": config['username'], "password": config['password']}

    try:
        r = requests.post(url_auth, data=json.dumps(data), headers=headers)
        r.raise_for_status()
        token = "JWT " + r.json()['access_token']

    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("Error occured", err)


    prod_date = config['date']
    directory_path = os.path.join('.', str(prod_date))
    os.makedirs(directory_path, exist_ok=True)

    url_prod = config['url_prod']
    headers = {'content-type': 'application/json', 'Authorization': token}
    data = {"date": prod_date}

    try:
        r = requests.get(url_prod, data=json.dumps(data), headers=headers, timeout=config['timeout'])
        r.raise_for_status()
        data = r.json()

        with open(os.path.join(directory_path, prod_date+'.json'), 'w') as json_file:
            json.dump(data, json_file)

    except requests.exceptions.HTTPError as errh:
        print("Http Error:", errh)
    except requests.exceptions.ConnectionError as errc:
        print("Error Connecting:", errc)
    except requests.exceptions.Timeout as errt:
        print("Timeout Error:", errt)
    except requests.exceptions.RequestException as err:
        print("Error occured", err)


if __name__ == '__main__':
    config = Config('config.yaml')
    app(config.get_config('product_app'))
