import json
p = json.load(open('data_v2/progress.json'))
searches = set(p.get('completed_searches', []))
reporters = ['SCMR','PLD','MLD','CLC','PCrLJ','PTD','PLC','YLR','CLD','GBLR']
for y in range(2025, 1946, -1):
    todo = [r for r in reporters if f"{y}-{r}" not in searches]
    done = [r for r in reporters if f"{y}-{r}" in searches]
    if todo and done:
        print(f"{y}: PARTIAL - done={len(done)}, todo={todo}")
    elif todo:
        print(f"{y}: NOT STARTED")
