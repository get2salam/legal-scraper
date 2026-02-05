#!/usr/bin/env python3
"""
Judgment Summarizer
===================
Generate summaries of legal judgments using LLM APIs.
Supports OpenAI, Anthropic, and Google AI.
"""

import json
import os
from pathlib import Path
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path("data/pakistanlawsite")
JSONL_DIR = DATA_DIR / "jsonl"
CASES_DIR = DATA_DIR / "cases"
SUMMARIES_DIR = DATA_DIR / "summaries"

# Summary prompt template
SUMMARY_PROMPT = """You are a legal research assistant. Summarize the following court judgment concisely.

JUDGMENT:
{judgment}

Provide a summary with these sections:
1. **Case Citation**: [citation]
2. **Court**: [court name]
3. **Key Issue**: What legal question was being decided?
4. **Facts**: Brief facts of the case (2-3 sentences)
5. **Held**: What did the court decide?
6. **Ratio Decidendi**: The legal principle established
7. **Key Legislation**: Acts/articles cited

Keep the summary under 300 words. Be precise and use legal terminology appropriately.

SUMMARY:"""


def load_case(case_id: str) -> Optional[dict]:
    """Load a case by ID from JSON file."""
    json_path = CASES_DIR / f"{case_id}.json"
    if json_path.exists():
        with open(json_path, "r", encoding="utf-8") as f:
            return json.load(f)
    
    # Try finding in JSONL files
    for jsonl_file in JSONL_DIR.glob("cases_*.jsonl"):
        with open(jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    try:
                        case = json.loads(line)
                        if case.get("id") == case_id:
                            return case
                    except json.JSONDecodeError:
                        pass
    return None


def summarize_with_openai(text: str, model: str = "gpt-4o-mini") -> str:
    """Summarize using OpenAI API."""
    try:
        from openai import OpenAI
        client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        
        response = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": SUMMARY_PROMPT.format(judgment=text[:15000])}],
            max_tokens=500,
            temperature=0.3,
        )
        return response.choices[0].message.content
    except ImportError:
        return "Error: openai package not installed. Run: pip install openai"
    except Exception as e:
        return f"Error: {str(e)}"


def summarize_with_anthropic(text: str, model: str = "claude-3-haiku-20240307") -> str:
    """Summarize using Anthropic API."""
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        
        response = client.messages.create(
            model=model,
            max_tokens=500,
            messages=[{"role": "user", "content": SUMMARY_PROMPT.format(judgment=text[:15000])}],
        )
        return response.content[0].text
    except ImportError:
        return "Error: anthropic package not installed. Run: pip install anthropic"
    except Exception as e:
        return f"Error: {str(e)}"


def summarize_with_google(text: str, model: str = "gemini-1.5-flash") -> str:
    """Summarize using Google AI API."""
    try:
        import google.generativeai as genai
        genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
        
        model_obj = genai.GenerativeModel(model)
        response = model_obj.generate_content(SUMMARY_PROMPT.format(judgment=text[:15000]))
        return response.text
    except ImportError:
        return "Error: google-generativeai package not installed. Run: pip install google-generativeai"
    except Exception as e:
        return f"Error: {str(e)}"


def summarize_case(case_id: str, provider: str = "openai") -> dict:
    """Summarize a case using specified LLM provider."""
    case = load_case(case_id)
    if not case:
        return {"error": f"Case {case_id} not found"}
    
    judgment = case.get("judgment", case.get("text", ""))
    if not judgment:
        return {"error": "No judgment text found"}
    
    # Get summary based on provider
    if provider == "openai":
        summary = summarize_with_openai(judgment)
    elif provider == "anthropic":
        summary = summarize_with_anthropic(judgment)
    elif provider == "google":
        summary = summarize_with_google(judgment)
    else:
        return {"error": f"Unknown provider: {provider}"}
    
    result = {
        "case_id": case_id,
        "title": case.get("title", ""),
        "provider": provider,
        "summary": summary,
        "original_length": len(judgment),
        "summary_length": len(summary),
    }
    
    # Save summary
    SUMMARIES_DIR.mkdir(parents=True, exist_ok=True)
    summary_path = SUMMARIES_DIR / f"{case_id}_summary.json"
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)
    
    return result


def batch_summarize(limit: int = 10, provider: str = "openai"):
    """Summarize multiple cases."""
    cases = []
    for jsonl_file in JSONL_DIR.glob("cases_*.jsonl"):
        with open(jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    try:
                        case = json.loads(line)
                        if case.get("judgment") or case.get("text"):
                            cases.append(case.get("id", ""))
                    except json.JSONDecodeError:
                        pass
    
    # Filter out already summarized
    existing = {f.stem.replace("_summary", "") for f in SUMMARIES_DIR.glob("*_summary.json")} if SUMMARIES_DIR.exists() else set()
    to_summarize = [c for c in cases if c not in existing][:limit]
    
    print(f"Summarizing {len(to_summarize)} cases with {provider}...")
    
    for i, case_id in enumerate(to_summarize, 1):
        print(f"  [{i}/{len(to_summarize)}] {case_id}...", end=" ")
        result = summarize_case(case_id, provider)
        if "error" in result:
            print(f"ERROR: {result['error']}")
        else:
            print(f"OK ({result['summary_length']} chars)")
    
    print(f"\nDone! Summaries saved to {SUMMARIES_DIR}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Summarize legal judgments")
    parser.add_argument("--case", "-c", help="Case ID to summarize")
    parser.add_argument("--batch", "-b", type=int, help="Batch summarize N cases")
    parser.add_argument("--provider", "-p", default="openai", 
                       choices=["openai", "anthropic", "google"],
                       help="LLM provider")
    
    args = parser.parse_args()
    
    if args.case:
        result = summarize_case(args.case, args.provider)
        print(json.dumps(result, indent=2))
    elif args.batch:
        batch_summarize(args.batch, args.provider)
    else:
        print("Usage:")
        print("  python summarizer.py --case 2025PLD1 --provider openai")
        print("  python summarizer.py --batch 10 --provider google")
        print("\nAvailable providers: openai, anthropic, google")
        print("Set API keys in .env: OPENAI_API_KEY, ANTHROPIC_API_KEY, GOOGLE_API_KEY")


if __name__ == "__main__":
    main()
