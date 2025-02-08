import argparse
import asyncio
import os
import sys
from xml.etree import ElementTree

import requests

__location__ = os.path.dirname(os.path.abspath(__file__))
__output__ = os.path.join(__location__, "output")

# Append parent directory to system path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from typing import List

from crawl4ai import (AsyncWebCrawler, BrowserConfig, CacheMode,
                      CrawlerRunConfig, CrawlResult)


async def crawl_parallel(urls: List[str], max_concurrent: int = 3):
    print("\n=== Parallel Crawling with Browser Reuse ===")

    # Minimal browser config
    browser_config = BrowserConfig(
        headless=True,
        verbose=True,
        extra_args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"],
    )
    crawl_config = CrawlerRunConfig(cache_mode=CacheMode.BYPASS)

    # Create the crawler instance
    crawler = AsyncWebCrawler(config=browser_config)
    await crawler.start()

    try:
        # We'll chunk the URLs in batches of 'max_concurrent'
        success_count = 0
        fail_count = 0
        for i in range(0, len(urls), max_concurrent):
            batch = urls[i : i + max_concurrent]
            tasks = []

            for j, url in enumerate(batch):
                # Unique session_id per concurrent sub-task
                session_id = f"parallel_session_{i + j}"
                task = crawler.arun(url=url, config=crawl_config, session_id=session_id)
                tasks.append(task)

            # Gather results
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Evaluate results
            for url, result in zip(batch, results):
                assert isinstance(result, (Exception, CrawlResult))
                if isinstance(result, Exception):
                    print(f"Error crawling {url}: {result}")
                    fail_count += 1
                elif result.success:
                    success_count += 1
                    save_markdown_to_result_dir(result)
                else:
                    fail_count += 1

        print(f"\nSummary:")
        print(f"  - Successfully crawled: {success_count}")
        print(f"  - Failed: {fail_count}")

    finally:
        print("\nClosing crawler...")
        await crawler.close()


def save_markdown_to_result_dir(result: CrawlResult):
    """
    Saves the markdown content to a file in the output directory.

    Args:
        result (CrawlResult): The crawl result
    """
    if result.markdown:
        # Convert result.url to a filename
        filename = (
            result.url.replace("https://", "").replace("/", "_").replace(".", "_")
        )

        # Save the markdown content to a file
        output_file = os.path.join(__output__, f"{filename}.md")
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(result.markdown)


def get_urls_to_crawl(sitemap_url: str) -> list[str]:
    """
    Fetches all URLs from the base URL's sitemap.

    Returns:
        List[str]: List of URLs
    """
    try:
        response = requests.get(sitemap_url)
        response.raise_for_status()

        # Parse the XML
        root = ElementTree.fromstring(response.content)

        # Extract all URLs from the sitemap
        # The namespace is usually defined in the root element
        namespace = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        urls = [loc.text for loc in root.findall(".//ns:loc", namespace) if loc.text]

        return urls
    except Exception as e:
        print(f"Error fetching sitemap: {e}")
        return []


def get_sitemap_url() -> str:
    """
    Parses the command line arguments to get the sitemap URL.

    Returns:
        str: The sitemap URL
    """
    parser = argparse.ArgumentParser(description="Crawl URLs from a sitemap")
    parser.add_argument("sitemap_url", type=str, help="URL of the sitemap")
    args = parser.parse_args()
    return args.sitemap_url


async def main():
    sitemap_url = get_sitemap_url()
    urls = get_urls_to_crawl(sitemap_url)
    if urls:
        # Create the output directory if it doesn't exist
        if not os.path.exists(__output__):
            os.makedirs(__output__)

        print(f"Found {len(urls)} URLs to crawl")
        await crawl_parallel(urls, max_concurrent=10)
    else:
        print("No URLs found to crawl")


if __name__ == "__main__":
    asyncio.run(main())
