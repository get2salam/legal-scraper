import json, os, glob, re

base = r"C:\Users\gempo\.openclaw\workspace\projects\pakistan-legislation-scraper\data_v2"
signal_words = ['overruled', 'reversed', 'set aside', 'distinguished', 'followed', 'affirmed', 'approved', 'applied', 'relied upon']

count = 0
for rep in ["SCMR", "PLD"]:
    for year_dir in sorted(glob.glob(os.path.join(base, rep, "*")), reverse=True):
        if not os.path.isdir(year_dir):
            continue
        for fpath in glob.glob(os.path.join(year_dir, "*.json"))[:50]:
            try:
                with open(fpath, 'r', encoding='utf-8') as f:
                    d = json.load(f)
                jtext = d.get('judgment_clean', '') or d.get('judgment', '')
                if not jtext:
                    continue
                # Normalize whitespace for searching
                jnorm = re.sub(r'\s+', ' ', jtext)
                for sw in signal_words:
                    if sw in jnorm.lower():
                        # Find citation near signal word
                        for m in re.finditer(sw, jnorm, re.IGNORECASE):
                            start = max(0, m.start() - 200)
                            end = min(len(jnorm), m.end() + 200)
                            context = jnorm[start:end]
                            # Check if there's a citation nearby
                            if re.search(r'(PLD\s+\d{4}\s+\w+\s+\d+|\d{4}\s+SCMR\s+\d+|\d{4}\s+CLC\s+\d+|\d{4}\s+MLD\s+\d+|\d{4}\s+PCr\.?LJ\s+\d+)', context):
                                print(f"\n=== {d.get('citation')} | Signal: {sw} ===")
                                print(f"  ...{context}...")
                                count += 1
                                if count >= 10:
                                    raise StopIteration
                            break
            except StopIteration:
                raise
            except:
                continue
        if count >= 10:
            break
    if count >= 10:
        break
