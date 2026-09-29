import os

DATE = "2026-08-30"
BASE_URL = "https://data.gharchive.org"

PROJECT_DIR = "/Users/harikadam/gharchive_project"
RAW_DIR = os.path.join(PROJECT_DIR, "raw")
LATE_DIR = os.path.join(PROJECT_DIR, "late_arrivals")
DB_DIR = PROJECT_DIR
DB_PATH = os.path.join(DB_DIR, "pipeline_state.db")

LATE_WINDOW_HOURS = 2
BACKFILL_MODE = False
FAKE_NOW = None

MAX_RETRIES = 2

EXPECTED_FIELDS = {"id", "type", "actor", "repo", "created_at"}

EXPECTED_SCHEMA = {
    "id": "string",
    "type": "string",
    "created_at": "string",
    "actor.id": "bigint",
    "actor.login": "string",
    "repo.id": "bigint",
    "repo.name": "string",
}

KNOWN_EVENT_TYPES = {
    "CommitCommentEvent", "CreateEvent", "DeleteEvent", "ForkEvent",
    "GollumEvent", "IssueCommentEvent", "IssuesEvent", "MemberEvent",
    "PublicEvent", "PullRequestEvent", "PullRequestReviewEvent",
    "PullRequestReviewCommentEvent", "PullRequestReviewThreadEvent",
    "PushEvent", "ReleaseEvent", "SponsorshipEvent", "WatchEvent",
    "DiscussionEvent", "DiscussionCommentEvent",
}