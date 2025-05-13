#!/usr/bin/env python3

# /// script
# dependencies = [
#   "tavily-python>=0.2.0",
#   "rich>=13.7.0",
# ]
# ///

"""
/// Example Usage

# Run scraper agent with default settings
uv run sfa_tavily_scraper.py -p "Municipal Waste Management" -k "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"

# Run with custom max results
uv run sfa_tavily_scraper.py -p "Find recycling data" -k "tvly-YOUR_API_KEY" -m 10

///
"""

import os
import sys
import json
import argparse
import shutil
from typing import Dict, List, Any, Tuple
from datetime import datetime
from rich.console import Console
from rich.panel import Panel
from rich.theme import Theme
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from tavily import TavilyClient

# Custom theme for better visual output
custom_theme = Theme({
    "info": "bold cyan",
    "success": "bold green",
    "warning": "bold yellow",
    "error": "bold red",
    "highlight": "bold magenta",
    "agent_step": "bold blue on white",
    "search_call": "bold blue",
    "search_result": "dim blue",
    "extract_call": "bold cyan",
    "extract_result": "dim cyan",
    "final_result": "bold green"
})

# Initialize console with custom theme
console = Console(theme=custom_theme)

# Terminal detection helper
def get_terminal_info() -> Tuple[int, int, bool]:
    """
    Detects terminal width, height, and if it supports colors.
    
    Returns:
        Tuple containing width, height, and color support
    """
    try:
        width, height = shutil.get_terminal_size()
    except:
        width, height = 80, 24
    
    color_support = True
    if os.environ.get('TERM') == 'dumb':
        color_support = False
    if sys.platform == 'win32' and 'ANSICON' not in os.environ:
        if os.environ.get('TERM_PROGRAM') not in ['vscode', 'hyper']:
            color_support = False
    
    return width, height, color_support

def search_for_reports(client: TavilyClient, query: str, max_results: int = 5) -> List[Dict[str, Any]]:
    """
    Searches for reports using Tavily's search API.
    
    Args:
        client: Tavily client
        query: Search query
        max_results: Maximum number of results to return
        
    Returns:
        List of report dictionaries
    """
    # Use terminal-aware output
    term_width, _, _ = get_terminal_info()
    header = f"[search_call]Tavily Search Call[/search_call]"
    console.print(Panel(
        f"{header}\n[info]Query:[/info] {query}\n[info]Max Results:[/info] {max_results}",
        width=min(term_width-4, 100),
        expand=False
    ))
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("[cyan]Searching the web...", total=None)
        
        # Search with Tavily
        response = client.search(
            query=query,
            search_depth="advanced",
            include_answer=False,
            include_raw_content=True,
            max_results=max_results
        )
        
        progress.update(task, completed=1.0)
    
    # Format the results into reports
    reports = []
    for result in response['results']:
        # Extract date if available
        date = 'Unknown'
        # Try to find a date in the content
        if 'published_date' in result:
            date = result['published_date']
            
        # Create report object
        report = {
            'title': result['title'],
            'date': date,
            'url': result['url'],
            'source': extract_source_from_url(result['url']),
            'format': 'HTML',
            'content': result.get('content', ''),
            'raw_content': result.get('raw_content', '')
        }
        reports.append(report)
        
    # Display summary
    console.print(f"[success]Found {len(reports)} results[/success]")
    
    # Show preview of found reports in a table
    if reports:
        preview_count = min(3, len(reports))
        preview_table = Table(title=f"Preview of Found Results", show_header=True, header_style="bold cyan")
        preview_table.add_column("Title", style="cyan")
        preview_table.add_column("Source", style="highlight")
        preview_table.add_column("URL", style="dim")
        
        for i in range(preview_count):
            preview_table.add_row(
                reports[i]['title'][:50] + ('...' if len(reports[i]['title']) > 50 else ''),
                reports[i]['source'],
                reports[i]['url'][:30] + '...'
            )
        
        console.print(preview_table)
    
    return reports

def extract_content(client: TavilyClient, reports: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Extracts full content from report URLs using Tavily's extract API.
    
    Args:
        client: Tavily client
        reports: List of reports to extract content from
        
    Returns:
        Updated list of reports with extracted content
    """
    # Use terminal-aware output
    term_width, _, _ = get_terminal_info()
    header = f"[extract_call]Tavily Extract Call[/extract_call]"
    console.print(Panel(
        f"{header}\n[info]Extracting content from {len(reports)} URLs[/info]",
        width=min(term_width-4, 100),
        expand=False
    ))
    
    # Collect URLs
    urls = [report['url'] for report in reports]
    
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task("[cyan]Extracting content...", total=None)
        
        # Batch extract content
        if urls:
            try:
                extraction_results = client.extract(urls=urls, extract_depth="advanced")
                progress.update(task, completed=1.0)
                
                # Update reports with extracted content
                for i, result in enumerate(extraction_results.get('results', [])):
                    if i < len(reports):
                        reports[i]['raw_content'] = result.get('raw_content', '')
                        reports[i]['format'] = detect_format(result.get('raw_content', ''))
            except Exception as e:
                console.print(f"[error]Error extracting content: {str(e)}[/error]")
        else:
            progress.update(task, completed=1.0)
    
    return reports

def save_reports(reports: List[Dict[str, Any]], output_file: str = "waste_management_reports.json") -> bool:
    """
    Saves reports to a JSON file with metadata.
    
    Args:
        reports: List of reports to save
        output_file: Output file path
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Use terminal-aware output
        term_width, _, has_color = get_terminal_info()
        
        if has_color:
            header = f"[final_result]✓ Saving Reports[/final_result]"
        else:
            header = "Saving Reports"
            
        console.print(Panel(
            f"{header}\n[info]Saving {len(reports)} reports to {output_file}[/info]",
            width=min(term_width-4, 100),
            expand=False
        ))
        
        # Create output structure with metadata
        output_data = {
            "metadata": {
                "agent_name": "Tavily Waste Management Scraper Agent",
                "version": "1.0.0",
                "collection_date": datetime.utcnow().isoformat(),
                "report_count": len(reports),
                "sources_count": len(set(report['source'] for report in reports))
            },
            "reports": reports
        }
        
        # Write to file
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False)
            
        # Display summary
        console.print(f"[success]Successfully saved {len(reports)} reports to {output_file}[/success]")
        return True
        
    except Exception as e:
        console.print(f"[error]Error saving reports: {str(e)}[/error]")
        return False

def extract_source_from_url(url: str) -> str:
    """Extract a readable source name from the URL."""
    try:
        domain = url.split("//")[-1].split("/")[0]
        
        # Remove www. if present
        if domain.startswith('www.'):
            domain = domain[4:]
            
        # Remove subdomain if present (keep only last two parts)
        parts = domain.split('.')
        if len(parts) > 2:
            domain = '.'.join(parts[-2:])
            
        # Capitalize domain parts
        domain_parts = domain.split('.')
        capitalized = '.'.join([part.capitalize() for part in domain_parts[:-1]] + [domain_parts[-1]])
        
        return capitalized
    except:
        return "Unknown Source"

def detect_format(content: str) -> str:
    """
    Detect the format of content based on patterns.
    
    Args:
        content: Raw content to analyze
        
    Returns:
        Detected format (HTML, PDF, etc.)
    """
    content_lower = content.lower()
    
    if '<html' in content_lower or '<!doctype html' in content_lower:
        return 'HTML'
    elif '%pdf-' in content_lower:
        return 'PDF'
    elif content.startswith('PK'):  # ZIP file header
        return 'ZIP'
    else:
        return 'Text'

def generate_search_queries(topic: str) -> List[str]:
    """
    Generates specific search queries based on the general topic.
    
    Args:
        topic: General search topic
        
    Returns:
        List of specific search queries
    """
    if 'waste management' in topic.lower():
        return [
            f"{topic} latest reports",
            f"{topic} government publications",
            f"{topic} statistics data",
            f"{topic} research papers",
            f"{topic} industry analysis"
        ]
    elif 'recycling' in topic.lower():
        return [
            f"{topic} efficiency reports",
            f"{topic} rate statistics",
            f"{topic} technology innovations",
            f"{topic} government guidelines",
            f"{topic} best practices"
        ]
    else:
        return [
            f"{topic} latest reports",
            f"{topic} data analysis",
            f"{topic} research publications"
        ]

def main():
    # Terminal info for splash screen
    term_width, term_height, has_color = get_terminal_info()
    
    # Display splash screen
    if has_color:
        splash_width = min(term_width-4, 80)
        splash = Panel(
            "[highlight]Tavily Waste Management Scraper v1.0[/highlight]\n"
            "[info]A specialized scraper to collect waste management reports using Tavily API[/info]",
            width=splash_width,
            style="bold blue",
            expand=False
        )
        console.print(splash)
    else:
        console.print("Tavily Waste Management Scraper v1.0")
        console.print("A specialized scraper to collect waste management reports using Tavily API")
    
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Tavily Waste Management Scraper Agent")
    parser.add_argument("-p", "--prompt", required=True, help="The search prompt")
    parser.add_argument("-k", "--api-key", help="Tavily API key (or set TAVILY_API_KEY env var)")
    parser.add_argument("-m", "--max-results", type=int, default=5, help="Maximum results per query (default: 5)")
    parser.add_argument("-o", "--output", default="waste_management_reports.json", help="Output file path")
    args = parser.parse_args()

    # Get API key from args or environment
    api_key = args.api_key or os.environ.get("TAVILY_API_KEY")
    if not api_key:
        console.print("[error]Error: No Tavily API key provided. Use -k flag or set TAVILY_API_KEY environment variable.[/error]")
        sys.exit(1)
        
    # Initialize Tavily client
    try:
        client = TavilyClient(api_key=api_key)
    except Exception as e:
        console.print(f"[error]Error initializing Tavily client: {str(e)}[/error]")
        sys.exit(1)
    
    # Generate search queries based on prompt
    search_queries = generate_search_queries(args.prompt)
    
    # Track all reports
    all_reports = []
    
    # Execute searches
    for i, query in enumerate(search_queries):
        # Create a stylized header for this step
        if has_color:
            console.rule(f"[agent_step]Search Query {i+1}/{len(search_queries)}[/agent_step]")
        else:
            console.rule(f"Search Query {i+1}/{len(search_queries)}")
            
        # Search for reports
        reports = search_for_reports(client, query, max_results=args.max_results)
        
        # Add to collection
        all_reports.extend(reports)
    
    # Remove duplicates based on URL
    unique_reports = []
    seen_urls = set()
    for report in all_reports:
        if report['url'] not in seen_urls:
            seen_urls.add(report['url'])
            unique_reports.append(report)
    
    # Extract full content from reports if needed
    if unique_reports:
        if has_color:
            console.rule(f"[agent_step]Extracting Full Content[/agent_step]")
        else:
            console.rule("Extracting Full Content")
            
        # Only process reports that don't already have raw content
        reports_to_extract = [r for r in unique_reports if not r.get('raw_content')]
        if reports_to_extract:
            updated_reports = extract_content(client, reports_to_extract)
            
            # Update reports in the main collection
            url_to_content = {r['url']: r.get('raw_content', '') for r in updated_reports}
            for report in unique_reports:
                if report['url'] in url_to_content:
                    report['raw_content'] = url_to_content[report['url']]
    
    # Save reports
    if unique_reports:
        if has_color:
            console.rule(f"[agent_step]Saving Reports[/agent_step]")
        else:
            console.rule("Saving Reports")
            
        save_reports(unique_reports, output_file=args.output)
        
        # Display final summary
        console.print(Panel(
            f"[final_result]Scraper Completed Successfully[/final_result]\n"
            f"Total reports found: {len(unique_reports)}\n"
            f"Unique sources: {len(set(r['source'] for r in unique_reports))}\n"
            f"Saved to: {args.output}",
            width=min(term_width-4, 80),
            expand=False
        ))
    else:
        console.print("[warning]No reports found matching your query[/warning]")

if __name__ == "__main__":
    main()