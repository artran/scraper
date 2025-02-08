import argparse
import asyncio
import logging
import os
import sys
from xml.etree import ElementTree

import requests

__location__ = os.path.dirname(os.path.abspath(__file__))
__output__ = os.path.join(__location__, "output")
logger = logging.getLogger(__name__)

# Append parent directory to system path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from typing import List

from crawl4ai import (AsyncWebCrawler, BrowserConfig, CacheMode,
                      CrawlerRunConfig, CrawlResult)


async def _crawl_parallel(urls: List[str], html_output: bool, max_concurrent: int = 3):
    logger.info("\n=== Parallel Crawling with Browser Reuse ===")

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
                    logger.critical("Error crawling %s: %s", url, result)
                    fail_count += 1
                elif result.success:
                    success_count += 1
                    _save_markdown_to_result_dir(result, html_output)
                else:
                    fail_count += 1

        logger.info("\nSummary:")
        logger.info("  - Successfully crawled: %s", success_count)
        logger.info("  - Failed: %s", fail_count)

    finally:
        logger.info("\nClosing crawler...")
        await crawler.close()


def _save_markdown_to_result_dir(result: CrawlResult, html_output: bool):
    """
    Saves the markdown content to a file in the output directory.

    Args:
        result (CrawlResult): The crawl result
    """
    # Convert result.url to a filename
    filename = result.url.replace("https://", "").replace("/", "_").replace(".", "_")

    if html_output:
        if result.html:
            # Save the html content to a file
            output_file = os.path.join(__output__, f"{filename}.html")
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(result.html)  # pyright: ignore [reportArgumentType]
    else:
        if result.markdown:
            # Save the markdown content to a file
            output_file = os.path.join(__output__, f"{filename}.md")
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(result.markdown)  # pyright: ignore [reportArgumentType]


def _get_urls_to_crawl(sitemap_url: str) -> list[str]:
    """
    Fetches all URLs from the sitemap.

    Returns:
        List[str]: List of URLs
    """
    namespace = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    try:
        response = requests.get(sitemap_url)
        response.raise_for_status()

        # Parse the XML
        root = ElementTree.fromstring(response.content)

        # Extract all URLs from the sitemap
        urls = [loc.text for loc in root.findall(".//ns:loc", namespace) if loc.text]
        logger.info(f"Found %s URLs in sitemap", len(urls))

        return urls
    except Exception as e:
        logger.exception("Error fetching sitemap")
        return []


def _get_child_sitemaps(base_sitemap_url: str) -> list[str]:
    """
    Fetches all child sitemaps from the base URL's sitemap.

    Returns:
        List[str]: List of URLs for all child sitemaps
    """
    namespace = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}

    try:
        response = requests.get(base_sitemap_url)
        response.raise_for_status()

        # Parse the XML
        root = ElementTree.fromstring(response.content)

        # Check if the sitemap has child sitemaps
        if not root.tag.endswith("sitemapindex"):
            return [base_sitemap_url]

        urls = [loc.text for loc in root.findall(".//ns:loc", namespace) if loc.text]
        logger.info("Found %s child sitemaps", len(urls))
        return urls
    except Exception as e:
        logger.exception("Error fetching child sitemap")
        return []


def _parse_command_line() -> argparse.Namespace:
    """
    Parses the command line arguments to get the sitemap URL and any other options.

    Returns:
        Namespace: The parsed arguments
    """
    parser = argparse.ArgumentParser(description="Crawl URLs from a sitemap")
    parser.add_argument("sitemap_url", type=str, help="URL of the sitemap")
    parser.add_argument(
        "--html-output",
        action="store_true",
        help="Save HTML instead of markdown",
    )

    args = parser.parse_args()
    return args


async def main():
    args = _parse_command_line()
    child_sitemaps = _get_child_sitemaps(args.sitemap_url)

    urls: list[str] = []
    for child_sitemap in child_sitemaps:
        urls.extend(_get_urls_to_crawl(child_sitemap))

    if urls:
        # Create the output directory if it doesn't exist
        if not os.path.exists(__output__):
            os.makedirs(__output__)

        logger.info("Found %s URLs to crawl", len(urls))
        await _crawl_parallel(urls, args.html_output, max_concurrent=10)
    else:
        logger.warning("No URLs found to crawl")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    asyncio.run(main())
