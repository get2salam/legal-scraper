"""Save PTD 1961 metadata (144 cases) - content not available on PLS."""
import sys,os,time,json,re
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

resp=s.post(f"{B}/Login/CitationSearch",data={"year":1961,"book":"PTD","code":"","court":"","judge":"","lawyer":"","party":""},timeout=30)
soup=BeautifulSoup(resp.text,"html.parser")

out=Path("data_v2/PTD/1961")
out.mkdir(parents=True,exist_ok=True)
saved=0

for row in soup.find_all("tr",class_="caseType"):
    tds=row.find_all("td")
    if len(tds)<4:continue
    cit=tds[1].get_text(strip=True)
    if not re.match(r"\d{4}\s+\w+\s+\d+",cit):continue
    title=tds[2].get_text(strip=True)
    court=tds[3].get_text(strip=True)
    btn=row.find("input",attrs={"casetypeid":True})
    cid=btn.get("casetypeid","") if btn else ""
    judge_elem=row.find("span",style=lambda st:st and "darkred" in st)
    judge=judge_elem.get_text(strip=True) if judge_elem else ""
    
    fn=cit.replace(" ","_")
    data={
        "citation":cit,
        "reporter":"PTD",
        "year":1961,
        "case_name":cid,
        "court":court,
        "judges":judge,
        "parties":title.split("Versus")[0].strip() if "Versus" in title else title,
        "judgment":"",
        "metadata_only":True,
        "pls_note":"Content not available on PLS (1961 PTD volume not digitised). Metadata scraped from search results.",
    }
    with open(out/f"{fn}.json","w",encoding="utf-8") as f:
        json.dump(data,f,ensure_ascii=False,indent=2)
    saved+=1

print(f"Saved {saved} PTD 1961 metadata files to {out}")
