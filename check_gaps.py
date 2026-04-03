import sys,os,re,time
sys.stdout.reconfigure(encoding='utf-8',errors='replace')
from pathlib import Path
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)),'.env'))
from curl_cffi import requests as r
from bs4 import BeautifulSoup
s=r.Session();s.impersonate="chrome"
B="https://www.pakistanlawsite.com"
u=os.getenv("PLS_USER","");p=os.getenv("PLS_PASS","")
s.post(f"{B}/Login/ClearLoginHistory",data={"Login.UserName":u,"Login.Password":p},timeout=30)
time.sleep(2)

data=Path("data_v2")
for rep,year in [("PCrLJ",1978),("PLC",1985),("PLD",2026)]:
    time.sleep(2.5)
    resp=s.post(f"{B}/Login/CitationSearch",data={"year":year,"book":rep,"code":"","court":"","judge":"","lawyer":"","party":""},timeout=30)
    soup=BeautifulSoup(resp.text,"html.parser")
    pls_cits=set()
    for row in soup.find_all("tr",class_="caseType"):
        tds=row.find_all("td")
        if len(tds)>=2:
            c=tds[1].get_text(strip=True)
            if re.match(r"\d{4}\s+\w",c): pls_cits.add(c)
    our_dir=data/rep/str(year)
    ours=set(f.stem.replace("_"," ") for f in our_dir.glob("*.json")) if our_dir.exists() else set()
    missing=pls_cits-ours
    print(f"{rep} {year}: PLS={len(pls_cits)} | Ours={len(ours)} | Missing={len(missing)}")
    if missing and len(missing)<=5:
        for m in sorted(missing): print(f"  {m}")
