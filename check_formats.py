import os

DATA = 'data_v2'
os.chdir(os.path.join(os.path.dirname(__file__), DATA))

# Check a recently scraped case
for rep in ['SCMR', 'PLD', 'MLD']:
    yp = os.path.join(rep, '2002')
    if not os.path.isdir(yp):
        continue
    jsons = sorted([f for f in os.listdir(yp) if f.endswith('.json')])
    if not jsons:
        continue
    
    citation = jsons[-1].replace('.json', '')
    print(f"Sample: {citation}")
    print()
    
    # 1. JSON
    p1 = os.path.join(rep, '2002', f'{citation}.json')
    print(f"  1. JSON:          {'YES' if os.path.exists(p1) else 'NO'}")
    
    # 2. Original HTML
    p2 = os.path.join(rep, '2002', 'original', f'{citation}.html')
    print(f"  2. Original HTML: {'YES' if os.path.exists(p2) else 'NO'}")
    
    # 3. Readable HTML  
    p3 = os.path.join('html', rep, '2002', f'{citation}.html')
    print(f"  3. Readable HTML: {'YES' if os.path.exists(p3) else 'NO'}")
    
    # 4. JSONL
    p4 = f'{rep}_2002.jsonl'
    print(f"  4. JSONL:         {'YES' if os.path.exists(p4) else 'NO'}")
    
    # Counts
    json_count = len(jsons)
    orig_dir = os.path.join(rep, '2002', 'original')
    orig_count = len([f for f in os.listdir(orig_dir) if f.endswith('.html')]) if os.path.isdir(orig_dir) else 0
    read_dir = os.path.join('html', rep, '2002')
    read_count = len([f for f in os.listdir(read_dir) if f.endswith('.html')]) if os.path.isdir(read_dir) else 0
    
    print(f"\n  Counts for {rep}/2002:")
    print(f"    JSON:          {json_count}")
    print(f"    Original HTML: {orig_count}")
    print(f"    Readable HTML: {read_count}")
    print(f"    JSONL:         {'exists' if os.path.exists(p4) else 'MISSING'}")
    break
