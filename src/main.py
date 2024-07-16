import asyncio
import csv
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from typing import List, Optional

import aiohttp
from bs4 import BeautifulSoup
from git import GitCommandError, Repo


@dataclass
class CountEntry:
    id: int
    timestamp: int
    count: int
    change: int


@dataclass
class ScraperConfig:
    url: str
    selector: str
    csv_filename: str
    refresh_time: int


class GitHandler:
    def __init__(self, repo_path):
        self.repo_path = repo_path
        self.repo = None
        self.github_token = "git_token"  # Hardcoded token (not recommended for production)
        self.setup_logging()

    def init_repo(self):
        if not os.path.isdir(os.path.join(self.repo_path, ".git")):
            self.repo = Repo.init(self.repo_path)
            self.repo.git.checkout("-b", "main")  # Create and switch to 'main' branch

            # Create .gitignore file
            gitignore_path = os.path.join(self.repo_path, ".gitignore")
            with open(gitignore_path, "w") as f:
                f.write("logs/\n")

            # Add .gitignore to the repo
            self.repo.index.add([".gitignore"])
            self.repo.index.commit("Add .gitignore file")
        else:
            self.repo = Repo(self.repo_path)

    def setup_logging(self):
        os.makedirs("logs", exist_ok=True)

        # Main logger (INFO and above)
        main_log_path = os.path.join("logs", "git_handler.log")
        main_handler = RotatingFileHandler(main_log_path, maxBytes=50 * 1024 * 1024, backupCount=1)
        main_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        main_handler.setFormatter(main_formatter)
        main_handler.setLevel(logging.INFO)

        # Debug logger
        debug_log_path = os.path.join("logs", "git_handler_debug.log")
        debug_handler = RotatingFileHandler(debug_log_path, maxBytes=50 * 1024 * 1024, backupCount=1)
        debug_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        debug_handler.setFormatter(debug_formatter)
        debug_handler.setLevel(logging.DEBUG)

        # Console handler (INFO and above only)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        console_handler.setFormatter(console_formatter)

        # Set up the logger
        self.logger = logging.getLogger("GitHandler")
        self.logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture all levels in file logs
        self.logger.addHandler(main_handler)
        self.logger.addHandler(debug_handler)
        self.logger.addHandler(console_handler)

        # Prevent log messages from propagating to the root logger
        self.logger.propagate = False

    def commit_and_push(self, remote_url):
        if not self.repo:
            raise ValueError("Repository not initialized")

        try:
            # Add all changes (this will respect .gitignore)
            self.repo.git.add(A=True)

            # Only commit if there are changes
            if self.repo.is_dirty(untracked_files=True):
                commit_message = f"Update data {datetime.now().isoformat()}"
                self.repo.index.commit(commit_message)

                # Get the current branch name
                current_branch = self.repo.active_branch.name

                # Check if remote exists, add if it doesn't
                try:
                    origin = self.repo.remote("origin")
                except ValueError:
                    # Use token in the URL
                    token_url = remote_url.replace("https://", f"https://{self.github_token}@")
                    origin = self.repo.create_remote("origin", token_url)

                # Fetch from remote
                try:
                    fetch_info = origin.fetch()
                    for info in fetch_info:
                        if info.flags & info.ERROR:
                            self.logger.error(f"Error fetching: {info.note}")
                except GitCommandError as e:
                    self.logger.error(f"Error fetching from remote: {e}")

                # Check if the branch exists on the remote
                remote_branches = [ref.name for ref in origin.refs]
                if f"origin/{current_branch}" not in remote_branches:
                    # If the branch doesn't exist on the remote, push and set upstream
                    try:
                        self.repo.git.push("--set-upstream", "origin", current_branch)
                    except GitCommandError as e:
                        self.logger.error(f"Error pushing new branch to remote: {e}")
                        return
                else:
                    # If the branch exists, just push
                    try:
                        push_info = origin.push(current_branch)
                        for info in push_info:
                            if info.flags & info.ERROR:
                                self.logger.error(f"Error pushing: {info.summary}")
                            else:
                                self.logger.info(f"Push successful: {info.summary}")
                    except GitCommandError as e:
                        self.logger.error(f"Error pushing to remote: {e}")
                        return

                self.logger.info(f"Successfully pushed changes to GitHub on branch {current_branch}")
            else:
                self.logger.info("No changes to commit")
        except GitCommandError as e:
            self.logger.error(f"Git operation failed: {e}")


class GenericScraper:
    CSV_HEADERS = ["ID", "Timestamp", "Count", "Change"]

    def __init__(self, config: ScraperConfig, data_dir: str, git_handler: GitHandler):
        self.config = config
        self.data_dir = data_dir
        self.git_handler = git_handler
        self.setup_logging()
        self.last_entry = self.get_last_entry()

    def setup_logging(self):
        os.makedirs("logs", exist_ok=True)

        # Main logger (INFO and above)
        main_log_path = os.path.join("logs", "scraper.log")
        main_handler = RotatingFileHandler(main_log_path, maxBytes=50 * 1024 * 1024, backupCount=1)
        main_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        main_handler.setFormatter(main_formatter)
        main_handler.setLevel(logging.INFO)

        # Debug logger
        debug_log_path = os.path.join("logs", "scraper_debug.log")
        debug_handler = RotatingFileHandler(debug_log_path, maxBytes=50 * 1024 * 1024, backupCount=1)
        debug_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        debug_handler.setFormatter(debug_formatter)
        debug_handler.setLevel(logging.DEBUG)

        # Console handler (INFO and above only)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        console_handler.setFormatter(console_formatter)

        # Set up the logger
        self.logger = logging.getLogger(self.config.url)
        self.logger.setLevel(logging.DEBUG)  # Set to DEBUG to capture all levels in file logs
        self.logger.addHandler(main_handler)
        self.logger.addHandler(debug_handler)
        self.logger.addHandler(console_handler)

        # Prevent log messages from propagating to the root logger
        self.logger.propagate = False

    async def scrape_count(self, session: aiohttp.ClientSession) -> Optional[int]:
        try:
            async with session.get(self.config.url, timeout=10) as response:
                response.raise_for_status()
                text = await response.text()
                soup = BeautifulSoup(text, "html.parser")
                count_element = soup.select_one(self.config.selector)
                if count_element:
                    count_str = "".join(filter(str.isdigit, count_element.text.strip()))
                    return int(count_str) if count_str else None
                else:
                    return None
        except (aiohttp.ClientError, ValueError) as e:
            self.logger.error(f"Error scraping count: {e}")
            return None

    def get_last_entry(self) -> Optional[CountEntry]:
        csv_path = os.path.join(self.data_dir, self.config.csv_filename)
        if not os.path.exists(csv_path):
            return None

        try:
            with open(csv_path, "r") as file:
                lines = file.readlines()
                if len(lines) > 1:  # Check if there's more than just the header
                    last_line = lines[-1].strip().split(",")
                    return CountEntry(int(last_line[0]), int(last_line[1]), int(last_line[2]), int(last_line[3]))
        except (IOError, ValueError) as e:
            self.logger.error(f"Error reading last entry: {e}")
        return None

    def append_to_csv(self, entry: CountEntry):
        csv_path = os.path.join(self.data_dir, self.config.csv_filename)
        file_exists = os.path.isfile(csv_path)
        try:
            with open(csv_path, "a", newline="") as file:
                writer = csv.writer(file)
                if not file_exists:
                    writer.writerow(self.CSV_HEADERS)
                    self.logger.info(f"Created new CSV file with headers: {csv_path}")
                writer.writerow([entry.id, entry.timestamp, entry.count, entry.change])
            self.logger.info(f"Appended entry: ID={entry.id}, Count={entry.count}, Change={entry.change}")

            # Update GitHub after new data is collected
            self.git_handler.commit_and_push("https://github.com/Active5850/tarkov_event_arms_race.git")
        except IOError as e:
            self.logger.error(f"Error appending to CSV: {e}")

    async def run(self):
        current_id = self.last_entry.id + 1 if self.last_entry else 1
        async with aiohttp.ClientSession() as session:
            while True:
                timestamp = int(time.time())
                count = await self.scrape_count(session)

                if count is not None:
                    change = count - self.last_entry.count if self.last_entry else 0
                    if not self.last_entry or change != 0:
                        new_entry = CountEntry(current_id, timestamp, count, change)
                        self.append_to_csv(new_entry)
                        self.last_entry = new_entry
                        current_id += 1
                    else:
                        utc_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                        self.logger.debug(f"No change at {utc_time}: Count={count}")
                else:
                    self.logger.error("Failed to retrieve count")

                self.logger.debug(f"[Waiting {self.config.refresh_time}s]")
                await asyncio.sleep(self.config.refresh_time)


async def run_scraper(config: ScraperConfig, data_dir: str, git_handler: GitHandler):
    scraper = GenericScraper(config, data_dir, git_handler)
    try:
        await scraper.run()
    except asyncio.CancelledError:
        scraper.logger.info("Scraper task cancelled")
    except Exception as e:
        scraper.logger.exception(f"Unexpected error: {e}")


async def main():
    data_dir = "data"

    print("Starting Bot...")

    # Initialize Git repo
    git_handler = GitHandler(data_dir)
    git_handler.init_repo()

    # Update GitHub after new data is collected
    git_handler.commit_and_push("https://github.com/Active5850/tarkov_event_arms_race.git")

    configs = [
        ScraperConfig(
            url="https://www.escapefromtarkov.com/arms_race_peacekeeper",
            selector=".peacekeeper .container .content .block_count .count",
            csv_filename="peacekeeper_count.csv",
            refresh_time=60,
        ),
        ScraperConfig(
            url="https://www.escapefromtarkov.com/arms_race_prapor",
            selector=".prapor .container .content .block_count .count",
            csv_filename="prapor_count.csv",
            refresh_time=60,
        ),
    ]

    tasks: List[asyncio.Task] = []
    for config in configs:
        task = asyncio.create_task(run_scraper(config, data_dir, git_handler))
        tasks.append(task)

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logging.info("\nStopping all tasks. Please wait...")
    finally:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        logging.info(f"All tasks stopped. Total tasks created: {len(tasks)}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Program interrupted by user.")
