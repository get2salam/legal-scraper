import json, os, glob

# Find a case with substantial judgment text to understand citation patterns
base = r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2"
reporters = ["SCMR", "PLD", "PCrLJ", "CLC"]

for rep in reporters:
    files = glob.glob(os.path.join(base, rep, "2023", "*.json"))[:20]
    for fpath in files:
        try:
            with open(fpath, 'r', encoding='utf-8') as f:
                d = json.load(f)
            jtext = d.get('judgment_clean', '') or d.get('judgment', '')
            if len(jtext) > 5000:
                print(f"\n=== {d.get('citation')} | Court: {d.get('court')} | Text: {len(jtext)} chars ===")
                print(f"Keys: {list(d.keys())}")
                # Look for citation patterns
                import re
                # PLD pattern
                pld = re.findall(r'PLD\s+\d{4}\s+\w+\s+\d+', jtext)
                scmr = re.findall(r'\d{4}\s+SCMR\s+\d+', jtext)
                pcrlj = re.findall(r'\d{4}\s+PCr\.?LJ\s+\d+', jtext)
                clc = re.findall(r'\d{4}\s+CLC\s+\d+', jtext)
                ylr = re.findall(r'\d{4}\s+YLR\s+\d+', jtext)
                mld = re.findall(r'\d{4}\s+MLD\s+\d+', jtext)
                print(f"PLD: {pld[:5]}")
                print(f"SCMR: {scmr[:5]}")
                print(f"PCrLJ: {pcrlj[:5]}")
                print(f"CLC: {clc[:5]}")
                print(f"YLR: {ylr[:5]}")
                print(f"MLD: {mld[:5]}")
                
                # Print a snippet with citations
                for m in re.finditer(r'(PLD\s+\d{4}\s+\w+\s+\d+|\d{4}\s+SCMR\s+\d+)', jtext):
                    start = max(0, m.start() - 100)
                    end = min(len(jtext), m.end() + 100)
                    print(f"\n  CONTEXT: ...{jtext[start:end]}...")
                    break
                    
                if pld or scmr or pcrlj or clc:
                    break
        except Exception as e:
            continue
    else:
        continue
    break
