#!/bin/bash 

expressvpn list > expresslist.txt
python3 updateactivetlds.py
python3 countryinfo.py
python3 mk_run_config.py 
