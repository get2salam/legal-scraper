#!/usr/bin/env python3
"""
Legal Scraper CLI

Command-line interface for the legal scraper framework.
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from dotenv import load_dotenv

# Load environment
load_dotenv()

from legal_scraper import Scraper
from legal_scraper.analytics import extract_citations, generate_stats

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)


def cmd_status(args):
    """Show scraper status."""
    with Scraper(adapter=args.adapter) as scraper:
        status = scraper.status()
        print("\n" + "=" * 50)
        print("  LEGAL SCRAPER STATUS")
        print("=" * 50)
        for key, value in status.items():
            print(f"  {key}: {value}")
        print("=" * 50 + "\n")


def cmd_search(args):
    """Search for cases."""
    with Scraper(adapter=args.adapter) as scraper:
        if not scraper.authenticate():
            print("Authentication failed")
            return 1
        
        results = scraper.search(args.query, limit=args.limit)
        
        print(f"\nFound {len(results)} cases:\n")
        for case in results[:args.limit]:
            print(f"  [{case.get('id')}] {case.get('title', 'No title')}")
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(results, f, indent=2)
            print(f"\nSaved to {args.output}")


def cmd_enumerate(args):
    """Enumerate cases for a year."""
    with Scraper(adapter=args.adapter) as scraper:
        if not scraper.authenticate():
            print("Authentication failed")
            return 1
        
        case_ids = scraper.enumerate(year=args.year)
        
        print(f"\nFound {len(case_ids)} cases for {args.year}:\n")
        for case_id in case_ids[:10]:
            print(f"  {case_id}")
        if len(case_ids) > 10:
            print(f"  ... and {len(case_ids) - 10} more")


def cmd_fetch(args):
    """Fetch cases."""
    with Scraper(adapter=args.adapter) as scraper:
        if not scraper.authenticate():
            print("Authentication failed")
            return 1
        
        if args.id:
            # Fetch single case
            case = scraper.fetch(args.id)
            if case:
                print(json.dumps(case, indent=2))
            else:
                print(f"Case not found: {args.id}")
        
        elif args.year:
            # Fetch all cases for year
            case_ids = scraper.enumerate(year=args.year)
            fetched = scraper.batch_fetch(case_ids, limit=args.limit)
            print(f"\nFetched {fetched} cases")


def cmd_analyze(args):
    """Run analytics."""
    if args.type == "stats":
        stats = generate_stats(args.data_dir)
        print(json.dumps(stats, indent=2))
    
    elif args.type == "citations":
        cases_dir = Path(args.data_dir) / "cases"
        cases = []
        for f in cases_dir.glob("*.json"):
            with open(f) as fp:
                cases.append(json.load(fp))
        
        from legal_scraper.analytics.citations import analyze_citations
        results = analyze_citations(cases)
        print(json.dumps(results, indent=2, default=str))


def main():
    parser = argparse.ArgumentParser(
        description="Legal Scraper CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python cli.py status
  python cli.py search --query "constitutional petition" --limit 10
  python cli.py enumerate --year 2024
  python cli.py fetch --id case_001
  python cli.py analyze --type stats
        """
    )
    parser.add_argument(
        "--adapter", "-a",
        default="example",
        help="Adapter to use (default: example)"
    )
    parser.add_argument(
        "--data-dir", "-d",
        default="data",
        help="Data directory"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command")
    
    # Status
    sub = subparsers.add_parser("status", help="Show scraper status")
    sub.set_defaults(func=cmd_status)
    
    # Search
    sub = subparsers.add_parser("search", help="Search for cases")
    sub.add_argument("--query", "-q", required=True, help="Search query")
    sub.add_argument("--limit", "-l", type=int, default=10, help="Max results")
    sub.add_argument("--output", "-o", help="Output file")
    sub.set_defaults(func=cmd_search)
    
    # Enumerate
    sub = subparsers.add_parser("enumerate", help="List cases for a year")
    sub.add_argument("--year", "-y", type=int, required=True, help="Year")
    sub.set_defaults(func=cmd_enumerate)
    
    # Fetch
    sub = subparsers.add_parser("fetch", help="Fetch case(s)")
    sub.add_argument("--id", help="Single case ID")
    sub.add_argument("--year", "-y", type=int, help="Fetch all for year")
    sub.add_argument("--limit", "-l", type=int, default=100, help="Max to fetch")
    sub.set_defaults(func=cmd_fetch)
    
    # Analyze
    sub = subparsers.add_parser("analyze", help="Run analytics")
    sub.add_argument(
        "--type", "-t",
        choices=["stats", "citations"],
        required=True,
        help="Analysis type"
    )
    sub.set_defaults(func=cmd_analyze)
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return 1
    
    return args.func(args) or 0


if __name__ == "__main__":
    sys.exit(main())
