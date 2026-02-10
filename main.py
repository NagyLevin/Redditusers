import os
import time
import argparse
from datetime import datetime, timezone
from typing import Optional, Iterable

import praw
from prawcore import NotFound, Forbidden, Redirect
from dotenv import load_dotenv
from tqdm import tqdm
import pathlib

# ======= DEFAULT CONFIG =======
DEFAULT_USERS = ["Levin"]  # just a test user so that the program doesn't crash
DEFAULT_OUTDIR = "output"  # base output directory

DEFAULT_USERFILE = "./users.txt"  # optional: default username list file
VISITED_FILE = pathlib.Path("./visited_users.txt") # file to store the usernames that have been visited
TIMEOUTS_FILE = pathlib.Path("./timeouts_users.txt") # file to store the usernames that have timed out so that we can download them again
# ==============================


# ---------- visited / timeouts ----------
def _norm_user(name: str) -> str:
    name = (name or "").strip()
    if name.lower().startswith("u/"):
        name = name[2:]
    return name.lower()

def add_to_visited(username: str) -> None:
    VISITED_FILE.touch(exist_ok=True)
    key = _norm_user(username)
    cur = set(x.strip() for x in VISITED_FILE.read_text(encoding="utf-8", errors="ignore").splitlines() if x.strip())
    if key not in cur:
        with VISITED_FILE.open("a", encoding="utf-8") as f:
            f.write(key + "\n")

def is_visited(username: str) -> bool:
    VISITED_FILE.touch(exist_ok=True)
    key = _norm_user(username)
    return key in {x.strip() for x in VISITED_FILE.read_text(encoding="utf-8", errors="ignore").splitlines() if x.strip()}

def add_to_timeouts(username: str) -> None:
    TIMEOUTS_FILE.touch(exist_ok=True)
    key = _norm_user(username)
    cur = set(x.strip() for x in TIMEOUTS_FILE.read_text(encoding="utf-8", errors="ignore").splitlines() if x.strip())
    if key not in cur:
        with TIMEOUTS_FILE.open("a", encoding="utf-8") as f:
            f.write(key + "\n")


# ---------- helpers ----------
def ensure_dir(p: str) -> None:
    if p:
        os.makedirs(p, exist_ok=True)

def to_epoch(dt: Optional[str]) -> Optional[int]:
    """
    dt can be:
      - None
      - '2025-08-01' (UTC 00:00:00)
      - '2025-08-01T14:30:00' (UTC)
      - epoch string (e.g. '1722575400')
    """
    if dt is None:
        return None
    try:
        return int(float(dt))  # already epoch
    except ValueError:
        pass
    if "T" in dt:
        return int(datetime.fromisoformat(dt).replace(tzinfo=timezone.utc).timestamp())
    return int(datetime.fromisoformat(dt + "T00:00:00").replace(tzinfo=timezone.utc).timestamp())

def _safe_text(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.replace("\r", "")
    lines = s.split("\n")
    return ("\n      ").join(lines)

def _fmt_utc(ts: int) -> str:
    try:
        return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
    except Exception:
        return str(ts)

def load_users_from_file(path: str) -> list[str]:
    """
    Read usernames from a text file, one per line.
    - Ignores empty lines and lines starting with '#'
    - Accepts optional leading 'u/' and removes it
    - De-duplicates while preserving order (case-insensitive)
    """
    seen = set()
    users: list[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            if line.lower().startswith("u/"):
                line = line[2:]
            line = line.split()[0]
            key = _norm_user(line)
            if key and key not in seen:
                seen.add(key)
                users.append(line)
    if not users:
        raise RuntimeError(f"No usernames found in file: {path}")
    return users


# ---------- auth ----------
def init_reddit() -> praw.Reddit:
    load_dotenv()
    cid  = os.getenv("REDDIT_CLIENT_ID", "").strip()
    csec = os.getenv("REDDIT_CLIENT_SECRET", "").strip()
    ua   = os.getenv("REDDIT_USER_AGENT", "").strip()

    if not ua:
        raise RuntimeError("Missing REDDIT_USER_AGENT in .env")

    def smoke_test(r: praw.Reddit):
        next(iter(r.subreddit("popular").hot(limit=1)))

    if cid and csec:
        try:
            r = praw.Reddit(
                client_id=cid,
                client_secret=csec,
                user_agent=ua,
                ratelimit_seconds=5,
            )
            r.read_only = True
            smoke_test(r)
            print("[auth] OK: app-only (client_credentials)")
            return r
        except Exception as e:
            print("[auth] FAIL app-only:", repr(e))

    raise RuntimeError("Authentication error (check .env: REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT)")


# ---------- resolve user ----------
def resolve_user(reddit: praw.Reddit, username: str):
    """
    Validate user exists & accessible.
    Returns a PRAW Redditor object or None.
    """
    name = (username or "").strip()
    if not name:
        return None
    if name.lower().startswith("u/"):
        name = name[2:]

    u = reddit.redditor(name)
    try:
        # Force fetch
        _ = u.id
        return u
    except NotFound:
        print(f"[skip] u/{name} not found / deleted / suspended.")
    except Forbidden:
        print(f"[skip] u/{name} forbidden (cannot access).")
    except Redirect:
        print(f"[skip] u/{name} redirected (invalid user).")
    except Exception as e:
        print(f"[skip] u/{name} unknown error: {e!r}")
    return None


# ---------- iterators (descending by new) ----------
def iter_user_posts(user, before: Optional[int], after: Optional[int], hard_limit: Optional[int]) -> Iterable:
    count = 0
    for s in user.submissions.new(limit=None):
        cu = int(getattr(s, "created_utc", 0))
        if before is not None and cu > before:
            continue
        if after is not None and cu < after:
            break
        yield s
        count += 1
        if hard_limit and count >= hard_limit:
            break

def iter_user_comments(user, before: Optional[int], after: Optional[int], hard_limit: Optional[int]) -> Iterable:
    count = 0
    for c in user.comments.new(limit=None):
        cu = int(getattr(c, "created_utc", 0))
        if before is not None and cu > before:
            continue
        if after is not None and cu < after:
            break
        yield c
        count += 1
        if hard_limit and count >= hard_limit:
            break


# ---------- writing ----------
def write_post_block(f, s) -> None:
    created = int(getattr(s, "created_utc", 0))
    title = getattr(s, "title", "") or ""
    subreddit = str(getattr(s, "subreddit", "")) or ""
    url = getattr(s, "url", "") or ""
    permalink = getattr(s, "permalink", "") or ""
    selftext = _safe_text(getattr(s, "selftext", None))

    f.write("Post:\n")
    #f.write(f"  created_utc: {created} ({_fmt_utc(created)})\n") #nem kell mikor csinálták minden postnál
    f.write(f"  subreddit: r/{subreddit}\n")
    f.write(f"  title: {title}\n")
    #if url:
    #    f.write(f"  url: {url}\n") nem kell url
    #if permalink:
    #    f.write(f"  permalink: {permalink}\n")
    if selftext:
        f.write("  body:\n")
        f.write(f"    {selftext}\n")
    f.write("\n")

def write_comment_block(f, c) -> None:
    created = int(getattr(c, "created_utc", 0))
    subreddit = str(getattr(c, "subreddit", "")) or ""
    body = _safe_text(getattr(c, "body", None))
    permalink = getattr(c, "permalink", "") or ""
    link_id = getattr(c, "link_id", "") or ""
    parent_id = getattr(c, "parent_id", "") or ""

    f.write("Comment:\n")
    #f.write(f"  created_utc: {created} ({_fmt_utc(created)})\n") nem kell idő
    f.write(f"  subreddit: r/{subreddit}\n")
    #if link_id:
    #    f.write(f"  link_id: {link_id}\n")
    #if parent_id:
    #    f.write(f"  parent_id: {parent_id}\n")
    #if permalink:
    #    f.write(f"  permalink: {permalink}\n")
    if body:
        f.write("  body:\n")
        f.write(f"    {body}\n")
    f.write("\n")


# ---------- main download ----------
def download_user_activity(
    reddit: praw.Reddit,
    username: str,
    out_dir: str,
    after: Optional[int],
    before: Optional[int],
    limit_posts: Optional[int],
    limit_comments: Optional[int],
    sleep_s: float = 0.5,
    include_posts: bool = True,
    include_comments: bool = True,
) -> None:
    user = resolve_user(reddit, username)
    if user is None:
        return

    ensure_dir(out_dir)
    uname_key = _norm_user(username)  # for stable filenames
    posts_path = os.path.join(out_dir, f"{uname_key}_posts.txt")
    chats_path = os.path.join(out_dir, f"{uname_key}_chats.txt")

    posts_saved = 0
    cmts_saved = 0

    posts_file = open(posts_path, "w", encoding="utf-8") if include_posts else None
    cmts_file = open(chats_path, "w", encoding="utf-8") if include_comments else None

    try:
        if posts_file:
            posts_file.write(f"=== u/{uname_key} POSTS ===\n\n")
        if cmts_file:
            cmts_file.write(f"=== u/{uname_key} COMMENTS ===\n\n")

        if include_posts and posts_file:
            pbar = tqdm(desc=f"Posts u/{uname_key}", unit="post")
            for s in iter_user_posts(user, before=before, after=after, hard_limit=limit_posts):
                write_post_block(posts_file, s)
                posts_saved += 1
                pbar.update(1)
                time.sleep(sleep_s)
            pbar.close()

        if include_comments and cmts_file:
            pbar = tqdm(desc=f"Comments u/{uname_key}", unit="comment")
            for c in iter_user_comments(user, before=before, after=after, hard_limit=limit_comments):
                write_comment_block(cmts_file, c)
                cmts_saved += 1
                pbar.update(1)
                time.sleep(sleep_s)
            pbar.close()

    finally:
        if posts_file:
            posts_file.flush()
            posts_file.close()
        if cmts_file:
            cmts_file.flush()
            cmts_file.close()

    if include_posts:
        print(f" Saved: {posts_saved} posts    -> {posts_path}")
    if include_comments:
        print(f" Saved: {cmts_saved} comments -> {chats_path}")


def main():
    ap = argparse.ArgumentParser(
        description="Reddit user downloader (all posts + comments via PRAW)")  # Parancssori argumentumok kezelése (CLI), a program leírásával

    ap.add_argument(
        "username",
        nargs="*",
        help="e.g. spez (you can pass multiple separated by space)")  # Opcionális pozicionális lista: ha megadsz userneveket (többet is), ezeket dolgozza fel.
   # Ha üresen hagyod, akkor a program az alapértelmezett listát / input fájlt használja.

    ap.add_argument(
        "--out",
        default=None,
        help="output directory (default: DEFAULT_OUTDIR)")  # Kimeneti mappa megadása. Ha nem adod meg, akkor a DEFAULT_OUTDIR lesz használva.

    ap.add_argument(
        "--after",
        help="lower time bound (epoch or ISO e.g., 2024-01-01)",
        default=None)  # Alsó időhatár (mettől): csak az ennél újabb bejegyzéseket/kommenteket gyűjti.
   # Elfogad epoch másodpercet (pl. 1722575400) vagy ISO dátumot (pl. 2024-01-01 vagy 2024-01-01T12:30:00).

    ap.add_argument(
        "--before",
        help="upper time bound (epoch or ISO)",
        default=None)  # Felső időhatár (meddig): csak az ennél régebbi (vagy nem frissebb) elemeket gyűjti.
   # Ugyanúgy epoch vagy ISO formátum.

    ap.add_argument(
        "--limit-posts",
        type=int,
        default=None,
        help="max number of posts per user (None = unlimited)")  # Maximum ennyi posztot gyűjt 1 usertől. Ha nincs megadva, akkor nincs limit (amennyit az API enged).

    ap.add_argument(
        "--limit-comments",
        type=int,
        default=None,
        help="max number of comments per user (None = unlimited)")  # Maximum ennyi kommentet gyűjt 1 usertől. Ha nincs megadva, akkor nincs limit.

    ap.add_argument(
        "--no-posts",
        action="store_true",
        help="skip posts")  # Ha ezt megadod, akkor a program NEM gyűjti a posztokat (csak kommenteket, ha az nincs tiltva).

    ap.add_argument(
        "--no-comments",
        action="store_true",
        help="skip comments")  # Ha ezt megadod, akkor a program NEM gyűjti a kommenteket (csak posztokat, ha az nincs tiltva).

    ap.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="sleep between items (seconds)")  # Két lekérés/elem feldolgozása között ennyi másodpercet vár.
   # Ez segít elkerülni a rate limitet ne terheljük tul az APIt

    ap.add_argument(
        "--auth-test",
        action="store_true",
        help="only test authentication and exit")  # Csak ellenőrzi, hogy működik-e az authentikáció (.env beállítások jók-e), majd kilép.
   # Hasznos gyors teszthez letöltés nélkül.

    ap.add_argument(
        "--inputfile",
        default=None,
        help="path to a text file listing usernames (one per line)")  # Ha megadod, akkor innen olvassa a userneveket (1 sor = 1 username).
   # Ilyenkor a CLI-ben megadott "username" lista helyett ezt használja (a kódod logikájától függően).

    ap.add_argument(
        "--reset-visited",
        action="store_true",
        help="ignore visited_users.txt and start fresh (this run)")  # Ha megadod, akkor a program figyelmen kívül hagyja a visited_users.txt-t,
   # tehát újra feldolgozza azokat a usereket is, akiket korábban már feldolgozott (azért hogy idő elteltével újra lehessen gyűjteni az adatokat).


    args = ap.parse_args()
    after = to_epoch(args.after)
    before = to_epoch(args.before)

    reddit = init_reddit()
    if args.auth_test:
        print("[auth] smoke test successful – exiting (--auth-test)")
        return

    # Load usernames
    if args.inputfile:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        inpath = args.inputfile
        if not os.path.isabs(inpath):
            inpath = os.path.join(base_dir, inpath)
        users = load_users_from_file(inpath)
        print(f"[info] Loaded {len(users)} usernames from {inpath}")
    else:
        users = args.username if args.username else DEFAULT_USERS

    outdir = args.out if args.out else DEFAULT_OUTDIR

    # reset visited for this run
    if args.reset_visited and VISITED_FILE.exists():
        # do NOT delete file, just ignore it by renaming (safer)
        # if you prefer hard delete: VISITED_FILE.unlink()
        print("[info] --reset-visited enabled: ignoring visited_users.txt for this run")
        visited_override = True
    else:
        visited_override = False

    for u in users:
        if not u.strip():
            continue

        if (not visited_override) and is_visited(u):
            print(f"Already processed u/{_norm_user(u)}")
            continue

        try:
            download_user_activity(
                reddit=reddit,
                username=u,
                out_dir=outdir,
                after=after,
                before=before,
                limit_posts=args.limit_posts,
                limit_comments=args.limit_comments,
                sleep_s=args.sleep,
                include_posts=(not args.no_posts),
                include_comments=(not args.no_comments),
            )
            add_to_visited(u)

        except Exception as e:
            print(f"[ABORT USER] u/{_norm_user(u)} due to failure: {e}")
            add_to_timeouts(u)
            continue


if __name__ == "__main__":
    main()
