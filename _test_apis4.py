import sys
sys.stdout.reconfigure(encoding='utf-8')
from curl_cffi import requests
import json

s = requests.Session(impersonate='chrome')

# SST - Get all judgments count
print('=== SST Total ===')
r = s.get('https://sstsindh.gov.pk/admin/api/judgements.php', params={
    'action': 'getJ', 'start': 0, 'noOfRecords': 5, 'keyword': ''
}, timeout=15)
data = r.json()
total = data["totalrecords"]
print(f"Total records: {total}")

# Test PDF download
first = data['data'][0]
name = first["name"]
pdf_url = f"https://sstsindh.gov.pk/admin/upload/judgements/{name}"
print(f"Testing PDF: {pdf_url}")
r2 = s.get(pdf_url, timeout=15)
print(f"PDF Status: {r2.status_code}, Size: {len(r2.content)} bytes")
print(f"Is PDF: {r2.content[:4] == b'%PDF'}")

# FSC - try alternative URLs
print('\n=== FSC ===')
for url in [
    'https://federalshariatcourt.gov.pk/en/leading-judgements/',
    'http://www.federalshariatcourt.gov.pk/en/leading-judgements/',
    'https://federalshariatcourt.gov.pk/',
]:
    try:
        r = s.get(url, timeout=10)
        print(f"{url}: Status={r.status_code}, Len={len(r.text)}")
        break
    except Exception as e:
        print(f"{url}: FAILED - {str(e)[:80]}")

# IHC - Also test the Case Law Management System (frmSrchOrdr.aspx)
print('\n=== IHC Case Law ===')
r3 = s.get('https://mis.ihc.gov.pk/frmSrchOrdr.aspx', timeout=15)
print(f"Status: {r3.status_code}, Len: {len(r3.text)}")
# Save for analysis
with open('_ihc_caselaw.html', 'w', encoding='utf-8') as f:
    f.write(r3.text)
print("Saved _ihc_caselaw.html")

# IHC - test the read judgment page
print('\n=== IHC Read Judgment ===')
r4 = s.get('https://mis.ihc.gov.pk/frmRdJgmnt.aspx', timeout=15)
print(f"Status: {r4.status_code}, Len: {len(r4.text)}")
