import os
import json
import requests
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

LOGIN = os.getenv("LOGIN_BLUESKY")
PASS = os.getenv("PASS_BLUESKY")
# TARGET = [
#     # Politiques + médias mainstream
#     {"handle": "aoc.bsky.social", "name": "Alexandria Ocasio-Cortez"},
#     {"handle": "mcuban.bsky.social", "name": "Mark Cuban"},
#     {"handle": "georgetakei.bsky.social", "name": "George Takei"},
#     {"handle": "markhamillofficial.bsky.social", "name": "Mark Hamill"},
#     {"handle": "theonion.com", "name": "The Onion"},
#     {"handle": "nytimes.com", "name": "The New York Times"},
#     {"handle": "stephenking.bsky.social", "name": "Stephen King"},
#     {"handle": "maddow.msnbc.com", "name": "Rachel Maddow"},
#     {"handle": "meidastouch.com", "name": "MeidasTouch"},
#     {"handle": "npr.org", "name": "NPR"},
#     {"handle": "atrupar.com", "name": "Aaron Rupar"},
#     {"handle": "bryantylercohen.bsky.social", "name": "Bryan Tyler Cohen"},
#     {"handle": "hankgreen.bsky.social", "name": "Hank Green"},
#     {"handle": "gtconway.bsky.social", "name": "George Conway"},
#     {"handle": "washingtonpost.com", "name": "The Washington Post"},
#     {"handle": "mollyjongfast.bsky.social", "name": "Molly Jong-Fast"},
#     {"handle": "chrishayes.bsky.social", "name": "Chris Hayes"},
#     {"handle": "ronfilipkowski.bsky.social", "name": "Ron Filipkowski"},
#     {"handle": "neiltyson.bsky.social", "name": "Neil deGrasse Tyson"},
#     {"handle": "ava.bsky.social", "name": "Ava DuVernay"},
    
#     # Journalistes + fact-checkers
#     {"handle": "chrislhayes.bsky.social", "name": "Chris Hayes"},
#     {"handle": "jonathanvswan.bsky.social", "name": "Jonathan Swan"},
#     {"handle": "maggienyt.bsky.social", "name": "Maggie Haberman"},
#     {"handle": "peterbakernyt.bsky.social", "name": "Peter Baker"},
#     {"handle": "nancyaker.bsky.social", "name": "Nancy Aker"},
#     {"handle": "daveweigel.bsky.social", "name": "Dave Weigel"},
#     {"handle": "zkatz.bsky.social", "name": "Zeke Faux"},
#     {"handle": "paulfarhi.bsky.social", "name": "Paul Farhi"},
#     {"handle": "mediamatters.bsky.social", "name": "Media Matters"},
#     {"handle": "snopes.com", "name": "Snopes"},
    
#     # Scientifiques + vulgarisation
#     {"handle": "snarxiv.bsky.social", "name": "arXiv"},
#     {"handle": "carlsagan.bsky.social", "name": "Carl Sagan"},
#     {"handle": "scicomm.bsky.social", "name": "SciComm"},
#     {"handle": "naturemag.bsky.social", "name": "Nature"},
#     {"handle": "sciencemag.bsky.social", "name": "Science Magazine"},
#     {"handle": "natoile.bsky.social", "name": "National Geographic"},
#     {"handle": "mit.bsky.social", "name": "MIT"},
#     {"handle": "stanford.bsky.social", "name": "Stanford"},
#     {"handle": "nasa.bsky.social", "name": "NASA"},
#     {"handle": "cernatschool.bsky.social", "name": "CERN"},
    
#     # Tech + IA
#     {"handle": "openai.bsky.social", "name": "OpenAI"},
#     {"handle": "anthropic.bsky.social", "name": "Anthropic"},
#     {"handle": "elonmusk.bsky.social", "name": "Elon Musk"},
#     {"handle": "sama.bsky.social", "name": "Sam Altman"},
#     {"handle": "ylecun.bsky.social", "name": "Yann LeCun"},
#     {"handle": "demis_hassabis.bsky.social", "name": "Demis Hassabis"},
#     {"handle": "gwern.bsky.social", "name": "Gwern"},
#     {"handle": "karpathy.bsky.social", "name": "Andrej Karpathy"},
#     {"handle": "hardmaru.bsky.social", "name": "David Ha"},
#     {"handle": "jackclarkSF.bsky.social", "name": "Jack Clark"},
    
#     # Économie + finance
#     {"handle": "nouriel.bsky.social", "name": "Nouriel Roubini"},
#     {"handle": "paulkrugman.bsky.social", "name": "Paul Krugman"},
#     {"handle": "reuters.com", "name": "Reuters"},
#     {"handle": "bloomberg.com", "name": "Bloomberg"},
#     {"handle": "cnbc.com", "name": "CNBC"},
#     {"handle": "wsj.com", "name": "Wall Street Journal"},
#     {"handle": "ft.com", "name": "Financial Times"},
#     {"handle": "economist.com", "name": "The Economist"},
#     {"handle": "ft_markets.bsky.social", "name": "FT Markets"},
#     {"handle": "bankofengland.bsky.social", "name": "Bank of England"},
    
#     # Santé + COVID
#     {"handle": "drdorit.bsky.social", "name": "Dr. Dorit Reiss"},
#     {"handle": "ericding.bsky.social", "name": "Eric Ding"},
#     {"handle": "drfuhrmanmd.bsky.social", "name": "Dr. Joel Fuhrman"},
#     {"handle": "amanpour.bsky.social", "name": "Christiane Amanpour"},
#     {"handle": "who.bsky.social", "name": "World Health Organization"},
#     {"handle": "cdc.bsky.social", "name": "CDC"},
#     {"handle": "nih.bsky.social", "name": "National Institutes of Health"},
#     {"handle": "lancet.bsky.social", "name": "The Lancet"},
#     {"handle": "jama.bsky.social", "name": "JAMA"},
#     {"handle": "nejm.bsky.social", "name": "NEJM"},
    
#     # Climat + environnement
#     {"handle": "nasa_climate.bsky.social", "name": "NASA Climate"},
#     {"handle": "ipcc.bsky.social", "name": "IPCC"},
#     {"handle": "greenpeace.bsky.social", "name": "Greenpeace"},
#     {"handle": "wwf.bsky.social", "name": "World Wildlife Fund"},
#     {"handle": "climatehomeorg.bsky.social", "name": "Climate Home News"},
#     {"handle": "bloomberg_green.bsky.social", "name": "Bloomberg Green"},
#     {"handle": "david_attenborough.bsky.social", "name": "David Attenborough"},
#     {"handle": "paulhawken.bsky.social", "name": "Paul Hawken"},
#     {"handle": "billmckibben.bsky.social", "name": "Bill McKibben"},
#     {"handle": "tomhartley.bsky.social", "name": "Tom Hartley"},
    
#     # Election + politique
#     {"handle": "whitehouse.bsky.social", "name": "The White House"},
#     {"handle": "senate.bsky.social", "name": "U.S. Senate"},
#     {"handle": "housefloor.bsky.social", "name": "House Floor"},
#     {"handle": "politico.bsky.social", "name": "Politico"},
#     {"handle": "axios.bsky.social", "name": "Axios"},
#     {"handle": "nyt_politics.bsky.social", "name": "NYT Politics"},
#     {"handle": "270towin.bsky.social", "name": "270toWin"},
#     {"handle": "fivethirtyeight.bsky.social", "name": "FiveThirtyEight"},
#     {"handle": "varepublican.bsky.social", "name": "VA Republican"},
#     {"handle": "demsgov.bsky.social", "name": "Dems Gov"},
# ]

TARGET = [
    # Politiques + médias mainstream
    {"handle": "aoc.bsky.social", "name": "Alexandria Ocasio-Cortez"}
]

SEARCH_QUERIES = {
    "trending": ["breaking", "urgent", "live", "developing"],
    "politics": ["politics", "election", "vote", "campaign", "congress", "senate", "representative"],
    "health": ["covid", "vaccine", "health", "disease", "pandemic", "outbreak", "virus"],
    "science": ["science", "research", "study", "climate", "environment", "discovery"],
    "tech": ["ai", "tech", "startup", "innovation", "algorithm", "data", "blockchain"],
    "economy": ["inflation", "market", "recession", "jobs", "unemployment", "gdp", "currency"],
    "conspiracy": ["conspiracy", "hoax", "fake", "misinformation", "disinformation"],
    "social": ["social media", "twitter", "facebook", "instagram", "tiktok"],
}

API_BASE = "https://bsky.social/xrpc"

#SEARCH_QUERIES = {
    #"feed_trending": ["breaking", "urgent", "live"],
    #"feed_hot_topics": ["politics", "election", "covid", "crisis"],
    #"feed_discover": ["news", "world", "science", "tech"],
#}



BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TOKEN_FILE = os.path.join(BASE_DIR, "token.json")

MONGODB = os.getenv("MONGO_URI", "mongodb+srv://ThLeroy35:eTo8ZhKltmQIb0Ei@bluesky.sx2wonq.mongodb.net/?retryWrites=true&w=majority")
DATABASE = os.getenv("MONGO_DB", "stagging_bluesky")
COLLECTION_NAME = os.getenv("MONGO_COLLECTION", "raw_data")

if not MONGODB:
    raise EnvironmentError("MONGO_URI non défini dans les variables d'environnement")

CLIENT = MongoClient(MONGODB)
DB = CLIENT[DATABASE]
COLLECTION = DB[COLLECTION_NAME]

print(f"MongoDB connecté : {DATABASE}.{COLLECTION_NAME}")
    
def login(identifier: str, password: str, timeout: int = 10):
    
    #Récupération de l'URL de l'API et connexion
    url = f"{API_BASE}/com.atproto.server.createSession"
    payload = {"identifier": identifier, "password": password}

    #Tout ce bloc sert à la connexion utilisateur et à la récupération des différents tokens
    try:
        r = requests.post(url, json=payload, timeout=timeout)
        if r.status_code != 200:
            print("Erreur de login :", r.status_code, r.text)
            return None
        data = r.json()
        access = data.get("accessJwt")
        refresh = data.get("refreshJwt")
        if not access:
            print("Login échoué — aucun accessJwt reçu")
            return None

        #Si on a les token, on les écrits dans un fichier tokens.json
        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            json.dump(
                {"accessJwt": access, "refreshJwt": refresh},
                f, ensure_ascii=False, indent=2
            )
        print("Connexion OK — token enregistré dans token.json")
        return access
    except Exception as e:
        print("Erreur login :", e)
        return None
 
def load_token(identifier: str = None, password: str = None):
    if not os.path.exists(TOKEN_FILE):
        print(f"Aucun token trouvé à {TOKEN_FILE}")
        if identifier and password:
            print("Tentative de connexion automatique...")
            return login(identifier, password)
        raise FileNotFoundError("token.json introuvable")

    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("accessJwt")
    except json.JSONDecodeError as e:
        print("Token corrompu :", e)
        if identifier and password:
            return login(identifier, password)



def headers(self):
    return {"Authorization": f"Bearer {self.token}"}

#Généré par Chatgpt -> Gestion de l'expiration des tokens
def refresh_token():
    """Renouvelle le token d'accès à partir du refreshJwt."""
    if not os.path.exists(TOKEN_FILE):
        print("Aucun fichier de token trouvé pour le refresh.")
        return login(LOGIN, PASS)

    try:
        with open(TOKEN_FILE, "r", encoding="utf-8") as f:
            tokens = json.load(f)
            refresh = tokens.get("refreshJwt")
            if not refresh:
                print("Aucun refresh token trouvé, reconnexion...")
                return login(LOGIN, PASS)

        url = f"{API_BASE}/com.atproto.server.refreshSession"
        headers = {"Authorization": f"Bearer {refresh}"}
        r = requests.post(url, headers=headers, timeout=10)

        if r.status_code != 200:
            print("Échec du refresh :", r.status_code, r.text)
            # Si le refresh échoue, on relogue complètement
            return login(LOGIN, PASS)

        data = r.json()
        new_access = data.get("accessJwt")
        new_refresh = data.get("refreshJwt")

        if not new_access:
            print("Pas de nouveau token reçu, reconnexion complète...")
            return login(LOGIN, PASS)

        with open(TOKEN_FILE, "w", encoding="utf-8") as f:
            json.dump(
                {"accessJwt": new_access, "refreshJwt": new_refresh},
                f, ensure_ascii=False, indent=2
            )
        print("Token rafraîchi avec succès.")
        return new_access

    except Exception as e:
        print("Erreur lors du refresh :", e)
        return login(LOGIN, PASS)
