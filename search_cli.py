#!/usr/bin/env python3
"""
Legal Search CLI
================
Simple command-line search for case law data.
No external dependencies required.
"""

import json
import re
import sys
from pathlib import Path
from collections import Counter

DATA_DIR = Path("data/pakistanlawsite")
JSONL_DIR = DATA_DIR / "jsonl"


def load_all_cases():
    """Load all cases from JSONL files."""
    cases = []
    for jsonl_file in JSONL_DIR.glob("cases_*.jsonl"):
        with open(jsonl_file, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    try:
                        cases.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    return cases


def search(cases, query, book=None, year=None, court=None, limit=10):
    """Search cases by keyword with optional filters."""
    query_lower = query.lower()
    results = []
    
    for case in cases:
        # Apply filters
        if book and case.get("book", "").upper() != book.upper():
            continue
        if year and case.get("year") != year:
            continue
        if court and court.lower() not in case.get("court", "").lower():
            continue
        
        # Search in text
        text = " ".join([
            str(case.get("title", "")),
            str(case.get("headnotes", "")),
            str(case.get("judgment", case.get("text", ""))),
        ]).lower()
        
        if query_lower in text:
            # Simple relevance: count occurrences
            score = text.count(query_lower)
            results.append((score, case))
    
    # Sort by relevance
    results.sort(key=lambda x: x[0], reverse=True)
    return [(case, score) for score, case in results[:limit]]


def show_case(case, show_full=False):
    """Display a case summary."""
    print(f"\n{'='*60}")
    print(f"  {case.get('title', 'Unknown')}")
    print(f"{'='*60}")
    print(f"  ID:     {case.get('id', '')}")
    print(f"  Book:   {case.get('book', '')} {case.get('year', '')}")
    print(f"  Court:  {case.get('court', '')}")
    print(f"  Judges: {case.get('judges', '')}")
    
    headnotes = case.get("headnotes", "")
    if headnotes:
        print(f"\n  HEADNOTES:")
        print(f"  {headnotes[:500]}{'...' if len(headnotes) > 500 else ''}")
    
    if show_full:
        judgment = case.get("judgment", case.get("text", ""))
        if judgment:
            print(f"\n  JUDGMENT:")
            print(f"  {judgment[:2000]}{'...' if len(judgment) > 2000 else ''}")


def stats(cases):
    """Show statistics about the case database."""
    print(f"\n{'='*60}")
    print(f"  CASE LAW DATABASE STATISTICS")
    print(f"{'='*60}")
    
    print(f"\n  Total Cases: {len(cases)}")
    
    by_book = Counter(c.get("book", "Unknown") for c in cases)
    print(f"\n  By Law Report:")
    for book, count in by_book.most_common():
        print(f"    {book:12} {count:4}")
    
    by_year = Counter(c.get("year", "Unknown") for c in cases)
    print(f"\n  By Year:")
    for year, count in sorted(by_year.items(), reverse=True):
        print(f"    {year}: {count}")


def interactive_mode(cases):
    """Interactive search mode."""
    print(f"\n{'='*60}")
    print(f"  LEGAL SEARCH CLI")
    print(f"  {len(cases)} cases loaded")
    print(f"{'='*60}")
    print("\nCommands:")
    print("  search <query>     - Search for cases")
    print("  book:<code>        - Filter by book (PLD, SCMR, etc.)")
    print("  year:<year>        - Filter by year")
    print("  show <case_id>     - Show case details")
    print("  stats              - Show database statistics")
    print("  quit               - Exit")
    
    current_filters = {}
    
    while True:
        try:
            cmd = input("\n> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break
        
        if not cmd:
            continue
        
        if cmd.lower() in ("quit", "exit", "q"):
            print("Goodbye!")
            break
        
        if cmd.lower() == "stats":
            stats(cases)
            continue
        
        if cmd.lower().startswith("book:"):
            current_filters["book"] = cmd.split(":", 1)[1].strip()
            print(f"Filter set: book={current_filters['book']}")
            continue
        
        if cmd.lower().startswith("year:"):
            current_filters["year"] = int(cmd.split(":", 1)[1].strip())
            print(f"Filter set: year={current_filters['year']}")
            continue
        
        if cmd.lower().startswith("show "):
            case_id = cmd.split(" ", 1)[1].strip()
            for case in cases:
                if case.get("id") == case_id:
                    show_case(case, show_full=True)
                    break
            else:
                print(f"Case {case_id} not found")
            continue
        
        if cmd.lower().startswith("search "):
            query = cmd.split(" ", 1)[1].strip()
        else:
            query = cmd
        
        # Search
        results = search(
            cases, query,
            book=current_filters.get("book"),
            year=current_filters.get("year"),
            limit=10
        )
        
        if results:
            print(f"\nFound {len(results)} results for '{query}':")
            for case, score in results:
                print(f"\n  [{score}] {case.get('title', 'Unknown')}")
                print(f"      ID: {case.get('id', '')} | {case.get('book', '')} {case.get('year', '')}")
                headnotes = case.get("headnotes", "")
                if headnotes:
                    print(f"      {headnotes[:150]}...")
        else:
            print(f"No results for '{query}'")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Search legal case law")
    parser.add_argument("query", nargs="?", help="Search query")
    parser.add_argument("--book", "-b", help="Filter by law report")
    parser.add_argument("--year", "-y", type=int, help="Filter by year")
    parser.add_argument("--limit", "-l", type=int, default=10, help="Max results")
    parser.add_argument("--interactive", "-i", action="store_true", help="Interactive mode")
    parser.add_argument("--stats", "-s", action="store_true", help="Show statistics")
    
    args = parser.parse_args()
    
    print("Loading cases...")
    cases = load_all_cases()
    print(f"Loaded {len(cases)} cases")
    
    if args.stats:
        stats(cases)
    elif args.interactive or not args.query:
        interactive_mode(cases)
    else:
        results = search(cases, args.query, book=args.book, year=args.year, limit=args.limit)
        if results:
            print(f"\nFound {len(results)} results:")
            for case, score in results:
                show_case(case)
        else:
            print("No results found")


if __name__ == "__main__":
    main()
