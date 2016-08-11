
ENDPOINTS = [
    {
        "name": "samos",
        "url": "http://doms.coaps.fsu.edu:8890/ws/search/samos",
        "fetchParallel": True,
        "fetchThreads": 8,
        "itemsPerPage": 1000,
        "metadataUrl": "http://doms.jpl.nasa.gov/ws/metadata/dataset?shortName=SAMOS&format=umm-json"
    },
    {
        "name": "spurs",
        "url": "https://doms.jpl.nasa.gov/ws/search/spurs",
        "fetchParallel": True,
        "fetchThreads": 8,
        "itemsPerPage": 25000,
        "metadataUrl": "http://doms.jpl.nasa.gov/ws/metadata/dataset?shortName=SPURS-1&format=umm-json"
    },
    {
        "name": "icoads",
        "url": "http://rda-db-icoads.ucar.edu:8890/ws/search/icoads",
        "fetchParallel": True,
        "fetchThreads": 8,
        "itemsPerPage": 1000,
        "metadataUrl": "http://doms.jpl.nasa.gov/ws/metadata/dataset?shortName=ICOADS&format=umm-json"
    },
    {
        "name": "spurs2",
        "url": "https://doms.jpl.nasa.gov/ws/search/spurs2",
        "fetchParallel": True,
        "fetchThreads": 8,
        "itemsPerPage": 25000,
        "metadataUrl": "http://doms.jpl.nasa.gov/ws/metadata/dataset?shortName=SPURS-2&format=umm-json"
    }
]



def getEndpointByName(name):
    for endpoint in ENDPOINTS:
        if endpoint["name"].upper() == name.upper():
            return endpoint
    return None