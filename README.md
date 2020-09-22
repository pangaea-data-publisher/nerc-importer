# nerc-importer
Python client for importing terms from the BODC NERC (https://www.bodc.ac.uk/resources/products/web_services/vocab/) terminology server. Collection to be imported from the server can be specified in config/import.ini.
## Usage
To run the importer, please execute the following from the root directory. Please update config/import_template.ini with the pangaea database settings (username, password, host, port).
```
pip3 install -r requirements.txt
python3 harvester.py -c <path_to_config_file>
```
