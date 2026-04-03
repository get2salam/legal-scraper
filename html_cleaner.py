#!/usr/bin/env python3
"""
HTML Cleaner Module
===================
Utilities for cleaning MS Word HTML and extracting clean text from PLS content.
Handles the messy HTML exported from MS Word that PLS uses for statute content.
"""

import re
import html
from typing import Optional
from bs4 import BeautifulSoup


def strip_html_to_text(html_content: str) -> str:
    """
    Convert HTML content to clean plain text.
    Removes all HTML tags, MS Word artifacts, and normalizes whitespace.
    
    Args:
        html_content: Raw HTML string (possibly with escaped unicode)
        
    Returns:
        Clean plain text
    """
    if not html_content:
        return ""
    
    # Handle double-escaped JSON strings
    if html_content.startswith('"') and html_content.endswith('"'):
        try:
            # Remove outer quotes and decode unicode escapes
            html_content = html_content[1:-1]
            html_content = html_content.encode('utf-8').decode('unicode_escape')
        except:
            pass
    
    # Decode unicode escape sequences
    try:
        if '\\u' in html_content:
            html_content = html_content.encode('utf-8').decode('unicode_escape')
    except:
        pass
    
    # Unescape HTML entities
    html_content = html.unescape(html_content)
    
    # Remove MS Office conditional comments
    html_content = re.sub(r'<!\[if[^\]]*\]>.*?<!\[endif\]>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<!--\[if.*?\]>.*?<!\[endif\]-->', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<!--.*?-->', '', html_content, flags=re.DOTALL)
    
    # Remove MS Office XML tags
    html_content = re.sub(r'<o:p>.*?</o:p>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'</?o:[^>]+>', '', html_content, flags=re.IGNORECASE)
    html_content = re.sub(r'</?w:[^>]+>', '', html_content, flags=re.IGNORECASE)
    html_content = re.sub(r'</?m:[^>]+>', '', html_content, flags=re.IGNORECASE)
    
    # Remove style and script blocks
    html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    
    # Remove head section entirely
    html_content = re.sub(r'<head[^>]*>.*?</head>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    
    # Parse with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Get text, preserving some structure
    text = soup.get_text(separator='\n', strip=True)
    
    # Clean up the text
    # Replace multiple whitespace with single space (except newlines)
    text = re.sub(r'[^\S\n]+', ' ', text)
    
    # Replace multiple newlines with double newline
    text = re.sub(r'\n\s*\n+', '\n\n', text)
    
    # Remove leading/trailing whitespace from each line
    lines = [line.strip() for line in text.split('\n')]
    text = '\n'.join(lines)
    
    # Remove empty lines at start/end
    text = text.strip()
    
    # Replace non-breaking spaces
    text = text.replace('\xa0', ' ')
    
    return text


def clean_statute_html(html_content: str) -> str:
    """
    Clean statute HTML for display, removing MS Word artifacts but keeping structure.
    Returns cleaned HTML suitable for display.
    
    Args:
        html_content: Raw HTML from PLS
        
    Returns:
        Clean HTML with proper structure
    """
    if not html_content:
        return ""
    
    # Handle double-escaped JSON strings
    if html_content.startswith('"') and html_content.endswith('"'):
        try:
            html_content = html_content[1:-1]
            html_content = html_content.encode('utf-8').decode('unicode_escape')
        except:
            pass
    
    # Decode unicode escape sequences
    try:
        if '\\u' in html_content:
            html_content = html_content.encode('utf-8').decode('unicode_escape')
    except:
        pass
    
    # Unescape HTML entities (do this after unicode decoding)
    html_content = html.unescape(html_content)
    
    # Remove MS Office conditional comments
    html_content = re.sub(r'<!\[if[^\]]*\]>.*?<!\[endif\]>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<!--\[if.*?\]>.*?<!\[endif\]-->', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<!--.*?-->', '', html_content, flags=re.DOTALL)
    
    # Remove MS Office XML tags
    html_content = re.sub(r'<o:p>.*?</o:p>', ' ', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'</?o:[^>]+/?>', '', html_content, flags=re.IGNORECASE)
    html_content = re.sub(r'</?w:[^>]+/?>', '', html_content, flags=re.IGNORECASE)
    html_content = re.sub(r'</?m:[^>]+/?>', '', html_content, flags=re.IGNORECASE)
    
    # Remove style and script blocks
    html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    
    # Remove head section
    html_content = re.sub(r'<head[^>]*>.*?</head>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
    
    # Remove html and body tags but keep content
    html_content = re.sub(r'</?html[^>]*>', '', html_content, flags=re.IGNORECASE)
    html_content = re.sub(r'</?body[^>]*>', '', html_content, flags=re.IGNORECASE)
    
    # Remove xmlns declarations from remaining tags
    html_content = re.sub(r'\s+xmlns[^=]*="[^"]*"', '', html_content)
    
    # Remove MSO-specific style attributes
    html_content = re.sub(r'mso-[^;":]+:[^;"]+;?', '', html_content)
    
    # Remove class attributes (mostly MSO classes)
    html_content = re.sub(r'\s+class="[^"]*"', '', html_content)
    
    # Remove inline styles that are now empty
    html_content = re.sub(r'\s+style="\s*"', '', html_content)
    
    # Clean up remaining style attributes (keep simple ones)
    def clean_style(match):
        style = match.group(1)
        # Keep only text-align and font-weight
        kept = []
        for prop in style.split(';'):
            prop = prop.strip()
            if prop.startswith('text-align:') or prop.startswith('font-weight:'):
                kept.append(prop)
        if kept:
            return f' style="{"; ".join(kept)}"'
        return ''
    
    html_content = re.sub(r'\s+style="([^"]*)"', clean_style, html_content)
    
    # Remove empty divs/spans
    html_content = re.sub(r'<div[^>]*>\s*</div>', '', html_content, flags=re.IGNORECASE)
    html_content = re.sub(r'<span[^>]*>\s*</span>', '', html_content, flags=re.IGNORECASE)
    
    # Remove empty paragraphs (with only &nbsp;)
    html_content = re.sub(r'<p[^>]*>\s*(&nbsp;)?\s*</p>', '', html_content, flags=re.IGNORECASE)
    
    # Clean up multiple whitespace
    html_content = re.sub(r'\s+', ' ', html_content)
    
    # But restore newlines around block elements
    html_content = re.sub(r'\s*(</?(?:p|div|h[1-6]|ul|ol|li|table|tr|td|th|br)[^>]*>)\s*', r'\n\1\n', html_content, flags=re.IGNORECASE)
    
    # Clean up excessive newlines
    html_content = re.sub(r'\n\s*\n+', '\n\n', html_content)
    
    return html_content.strip()


def extract_preamble(sections: list) -> Optional[str]:
    """
    Extract preamble from sections list.
    
    Args:
        sections: List of section dicts with 'number' and 'text' fields
        
    Returns:
        Clean text of preamble, or None if not found
    """
    for section in sections:
        number = section.get('number', '').upper()
        if number == 'PREAMBLE' or 'preamble' in number.lower():
            text = section.get('text', '') or section.get('text_raw', '')
            return strip_html_to_text(text)
    return None


def normalize_citation(citation: str) -> str:
    """
    Normalize a case citation for consistent formatting.
    
    Args:
        citation: Raw citation string like "1986  PLD  29" or "1986 PLD 29,"
        
    Returns:
        Normalized citation like "1986 PLD 29"
    """
    if not citation:
        return ""
    
    # Remove trailing punctuation
    citation = citation.strip().rstrip(',').rstrip('.')
    
    # Normalize whitespace
    citation = ' '.join(citation.split())
    
    return citation


def extract_case_citations(text: str) -> list:
    """
    Extract all case citations from text.
    
    Args:
        text: Text containing citations
        
    Returns:
        List of normalized citation strings
    """
    # Pattern for Pakistani case citations
    pattern = r'(\d{4})\s+(PLD|SCMR|CLC|PCrLJ|MLD|YLR|PTD|PLC|CLD|GBLR)\s+(\d+)'
    
    matches = re.findall(pattern, text, re.IGNORECASE)
    
    citations = []
    seen = set()
    
    for year, reporter, page in matches:
        citation = f"{year} {reporter.upper()} {page}"
        if citation not in seen:
            seen.add(citation)
            citations.append(citation)
    
    return citations


def extract_statute_citations(text: str) -> list:
    """
    Extract statute citations from text.
    Looks for patterns like "Act 1975", "Ordinance 2000", "Rules 1977", etc.
    
    Args:
        text: Text containing statute references
        
    Returns:
        List of statute reference strings
    """
    # Pattern for common statute formats
    patterns = [
        r'(?:the\s+)?([A-Z][A-Za-z\s]+(?:Act|Ordinance|Rules?|Regulations?|Order|Code)(?:\s*,?\s*\d{4})?)',
        r'Section\s+\d+\s+of\s+(?:the\s+)?([A-Z][A-Za-z\s]+)',
    ]
    
    statutes = []
    seen = set()
    
    for pattern in patterns:
        matches = re.findall(pattern, text)
        for match in matches:
            # Clean up the match
            statute = match.strip()
            statute = re.sub(r'\s+', ' ', statute)
            
            # Skip if too short or generic
            if len(statute) < 5:
                continue
            if statute.lower() in ['the act', 'this act', 'the rules', 'the ordinance']:
                continue
            
            if statute not in seen:
                seen.add(statute)
                statutes.append(statute)
    
    return statutes


def generate_statute_slug(title: str) -> str:
    """
    Generate a URL-safe slug from statute title.
    
    Args:
        title: Full statute title
        
    Returns:
        Safe filename/slug like "abandoned_properties_act_1975"
    """
    if not title:
        return "unknown"
    
    # Convert to lowercase
    slug = title.lower()
    
    # Remove common words
    slug = re.sub(r'\bthe\b', '', slug)
    
    # Replace non-alphanumeric with underscore
    slug = re.sub(r'[^a-z0-9]+', '_', slug)
    
    # Remove leading/trailing underscores
    slug = slug.strip('_')
    
    # Collapse multiple underscores
    slug = re.sub(r'_+', '_', slug)
    
    # Limit length
    if len(slug) > 100:
        slug = slug[:100].rsplit('_', 1)[0]
    
    return slug


if __name__ == "__main__":
    # Test with sample HTML
    sample_html = r'''"\u003chtml xmlns:o=\"urn:schemas-microsoft-com:office:office\"\r\n\u003chead\u003e\u003cmeta http-equiv=Content-Type content=\"text/html; charset=windows-1252\"\u003e\u003c/head\u003e\r\n\u003cbody\u003e\u003cp class=MsoNormal\u003e\u003cb\u003e1. Short title.-\u003c/b\u003eThis Act may be called the Test Act, 1975.\u003c/p\u003e\u003c/body\u003e\u003c/html\u003e"'''
    
    print("=== Strip HTML to Text ===")
    print(strip_html_to_text(sample_html))
    print()
    
    print("=== Clean HTML ===")
    print(clean_statute_html(sample_html))
    print()
    
    print("=== Extract Citations ===")
    text = "See 1986 PLD 29, 1983 PLD 176, and 2007 SCMR 1872 for reference"
    print(extract_case_citations(text))
