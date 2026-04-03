"""Find genuinely truncated legislation files for re-fetch."""
import os
import json

base = "data_v2/legislation"
truncated = []
total = 0

for letter in sorted(os.listdir(base)):
    letter_path = os.path.join(base, letter)
    if not os.path.isdir(letter_path) or len(letter) != 1 or not letter.isalpha():
        continue
    for fname in sorted(os.listdir(letter_path)):
        if not fname.endswith(".json"):
            continue
        total += 1
        fpath = os.path.join(letter_path, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            text = data.get("full_text", "") or ""
            sections = data.get("sections", {})
            file_size = os.path.getsize(fpath)
            
            # Genuinely truncated: has sections but content is suspiciously short
            # or has text that ends abruptly
            if sections:
                section_vals = list(sections.values())
                non_empty = [v for v in section_vals if v and str(v).strip() not in ("-1", '"-1"', "")]
                if non_empty:
                    avg_len = sum(len(str(v)) for v in non_empty) / len(non_empty)
                    # If average section length < 50 chars, likely truncated
                    if avg_len < 50 and len(non_empty) > 3:
                        truncated.append({
                            "path": fpath,
                            "letter": letter,
                            "title": data.get("title", ""),
                            "sections_count": len(sections),
                            "avg_section_len": round(avg_len, 1),
                            "file_size": file_size,
                        })
            elif text and 100 < len(text) < 500:
                # Very short full_text might be truncated
                truncated.append({
                    "path": fpath,
                    "letter": letter,
                    "title": data.get("title", ""),
                    "text_len": len(text),
                    "file_size": file_size,
                })
        except Exception as e:
            print(f"  Error reading {fpath}: {e}")

print(f"Scanned {total} files")
print(f"Found {len(truncated)} potentially truncated files")
print()
for t in truncated[:20]:
    print(f"  {t['letter']}/{os.path.basename(t['path'])} — {t.get('title', '?')[:60]}")

# Save for re-fetch
with open("data_v2/legislation/truncated_refetch.json", "w", encoding="utf-8") as f:
    json.dump(truncated, f, indent=2, ensure_ascii=False)
print(f"\nSaved to data_v2/legislation/truncated_refetch.json")
