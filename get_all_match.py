import numpy as np
import pandas as pd
import json
import http.client
import time  


# API connection setup
conn = http.client.HTTPSConnection("cricbuzz-cricket.p.rapidapi.com")

headers = {
	"x-rapidapi-key": "d1f52dc134msh606acc9612e6217p15c440jsnb9910abbb802",
	"x-rapidapi-host": "cricbuzz-cricket.p.rapidapi.com"
}

conn.request("GET", f"/matches/v1/recent", headers=headers)
res = conn.getresponse()
data = res.read()

print(data)