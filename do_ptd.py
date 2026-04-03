import sys,os,re,time,json
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
time.sleep(2);print("logged in")
resp=s.post(f"{B}/Login/CitationSearch",data={"year":1961,"book":"PTD","code":"","court":"","judge":"","lawyer":"","party":""},timeout=30)
soup=BeautifulSoup(resp.text,"html.parser")
rows=soup.find_all("tr",class_="caseType")
print(f"rows: {len(rows)}")
cases=[]
for row in rows:
    tds=row.find_all("td")
    c=tds[1].get_text(strip=True) if len(tds)>=2 else ""
    if not re.match(r"\d{4}\s+\w+\s+\d+",c):continue
    btn=row.find("input",attrs={"casetypeid":True})
    cid=btn.get("casetypeid","") if btn else ""
    cases.append((c,cid))
print(f"cases: {len(cases)}, first: {cases[0] if cases else 'none'}")
out=Path(os.path.dirname(os.path.abspath(__file__)))/"data_v2"/"PTD"/"1961"
out.mkdir(parents=True,exist_ok=True)
(out/"original_html").mkdir(exist_ok=True)
n=0;e=0
for c,cid in cases:
    fn=c.replace(" ","_")
    if(out/f"{fn}.json").exists():continue
    if not cid:e+=1;continue
    time.sleep(3)
    try:
        r2=s.post(f"{B}/Login/GetCaseFile",data={"caseName":cid,"headNotes":0},timeout=60)
        if r2.status_code==200 and len(r2.text)>100:
            ct=r2.text
            try:ct=json.loads(ct)
            except:pass
            d={"citation":c,"reporter":"PTD","year":1961,"case_name":cid,"judgment":ct if isinstance(ct,str) else json.dumps(ct)}
            with open(out/f"{fn}.json","w",encoding="utf-8") as f:json.dump(d,f,ensure_ascii=False,indent=2)
            with open(out/"original_html"/f"{fn}.html","w",encoding="utf-8") as f:f.write(ct if isinstance(ct,str) else json.dumps(ct))
            n+=1
            if n%10==0:print(f"  scraped {n}")
    except Exception as ex:print(f"  err: {ex}");e+=1
print(f"DONE: scraped={n} errors={e} total={len(list(out.glob('*.json')))}")
