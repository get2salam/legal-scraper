import json, sys, urllib.request, time
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

# Step 3: Check various domains on Wayback CDX
tests = [
    ('pakistancode.gov.pk/english/*', 'PakistanCode'),
    ('pakistancode.gov.pk/*', 'PakistanCode_root'),
    ('na.gov.pk/uploads/legislation/*', 'NA_uploads'),
    ('molaw.gov.pk/*', 'MoLaw'),
    ('punjablaws.gov.pk/*', 'PunjabLaws'),
    ('khyberpakhtunkhwa.gov.pk/*', 'KPK_laws'),
    ('sindhlaws.gov.pk/*', 'SindhLaws'),
]

for url_pat, label in tests:
    try:
        api = f'http://web.archive.org/cdx/search/cdx?url={url_pat}&output=json&limit=5&fl=timestamp,original,statuscode&filter=statuscode:200'
        resp = urllib.request.urlopen(api, timeout=30)
        data = json.loads(resp.read())
        count = len(data) - 1
        print(f'{label}: {count} snapshots')
        for row in data[1:3]:
            print(f'  {row[0]}: {row[1][:90]}')
    except Exception as e:
        print(f'{label}: ERROR - {e}')
    time.sleep(1)
