import os
import re
import time
import argparse
from datetime import datetime, timezone
from typing import Optional, Iterable, Tuple

import praw
from prawcore import NotFound, Forbidden, Redirect
from dotenv import load_dotenv
from tqdm import tqdm
import pathlib

# ======= DEFAULT CONFIG =======
DEFAULT_USERS = ["Levin"]  # just a test user so that the program doesn't crash
DEFAULT_OUTDIR = "output"  # base output directory

DEFAULT_USERFILE = "./users.txt"  # optional: default username list file
VISITED_FILE = pathlib.Path("./visited_users.txt")     # file to store the usernames that have been visited
TIMEOUTS_FILE = pathlib.Path("./timeouts_users.txt")   # file to store the usernames that have timed out so that we can download them again

# visited / new subs
VISITED_SUBS_FILE = pathlib.Path("./visited_subs.txt")  # subredditek amiket skipelünk (post+comment)
NEW_SUBS_FILE = pathlib.Path("./new_subs.txt")          # itt gyűjtjük az új subokat futás közben
# ==============================


# ---------- console logging ----------
def log(msg: str) -> None:
    print(msg, flush=True)


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


# ---------- subs helpers ----------
def _norm_sub(name: str) -> str:
    name = (name or "").strip()
    if name.lower().startswith("r/"):
        name = name[2:]
    return name.lower()


def load_visited_subs() -> set[str]:
    """
    visited_subs.txt format:
      - egy subreddit soronként: pl. 'askreddit' vagy 'r/askreddit'
      - üres sorok és # kommentek ignorálva
    """
    VISITED_SUBS_FILE.touch(exist_ok=True)
    subs: set[str] = set()
    for raw in VISITED_SUBS_FILE.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        subs.add(_norm_sub(line))
    return subs


def load_existing_new_subs() -> set[str]:
    NEW_SUBS_FILE.touch(exist_ok=True)
    return {
        _norm_sub(x.strip())
        for x in NEW_SUBS_FILE.read_text(encoding="utf-8", errors="ignore").splitlines()
        if x.strip() and (not x.strip().startswith("#"))
    }


def append_new_sub(sub_key: str, new_subs_seen: set[str]) -> None:
    """
    sub_key: normalizált (lower) subreddit név 'r/' nélkül
    new_subs_seen: már kiírt new_sub-ok setje, hogy ne duplikáljunk
    """
    sub_key = _norm_sub(sub_key)
    if not sub_key:
        return
    if sub_key in new_subs_seen:
        return
    NEW_SUBS_FILE.touch(exist_ok=True)
    with NEW_SUBS_FILE.open("a", encoding="utf-8") as f:
        f.write(sub_key + "\n")
    new_subs_seen.add(sub_key)


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
    cid = os.getenv("REDDIT_CLIENT_ID", "").strip()
    csec = os.getenv("REDDIT_CLIENT_SECRET", "").strip()
    ua = os.getenv("REDDIT_USER_AGENT", "").strip()

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
            log("[auth] OK: app-only (client_credentials)")
            return r
        except Exception as e:
            log("[auth] FAIL app-only: " + repr(e))

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
        _ = u.id  # Force fetch
        return u
    except NotFound:
        log(f"[skip] u/{name} not found / deleted / suspended.")
    except Forbidden:
        log(f"[skip] u/{name} forbidden (cannot access).")
    except Redirect:
        log(f"[skip] u/{name} redirected (invalid user).")
    except Exception as e:
        log(f"[skip] u/{name} unknown error: {e!r}")
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
    title = getattr(s, "title", "") or ""
    subreddit = str(getattr(s, "subreddit", "")) or ""
    selftext = _safe_text(getattr(s, "selftext", None))

    f.write("Post:\n")
    f.write(f"  subreddit: r/{subreddit}\n")
    f.write(f"  title: {title}\n")
    if selftext:
        f.write("  body:\n")
        f.write(f"    {selftext}\n")
    f.write("\n")


def write_comment_block(f, c) -> None:
    subreddit = str(getattr(c, "subreddit", "")) or ""
    body = _safe_text(getattr(c, "body", None))

    f.write("Comment:\n")
    f.write(f"  subreddit: r/{subreddit}\n")
    if body:
        f.write("  body:\n")
        f.write(f"    {body}\n")
    f.write("\n")


# ---------- HU filter (langdetect + phunspell) ----------
_WORD_RE = re.compile(r"[A-Za-zÁÉÍÓÖŐÚÜŰáéíóöőúüű]+")


def init_langdetect():
    """
    Returns (detect_langs_func, ok_bool).
    """
    try:
        from langdetect import detect_langs, DetectorFactory
        DetectorFactory.seed = 0
        return detect_langs, True
    except Exception as e:
        log(f"[warn] langdetect not available: {e!r}")
        return None, False


def init_hunspell_hu():
    """
    Returns (hunspell_obj, ok_bool).
    Needs Hungarian dictionary (.aff/.dic). We try:
      - env: HUNSPELL_AFF + HUNSPELL_DIC
      - common linux paths
    """
    try:
        from phunspell import HunSpell
    except Exception as e:
        log(f"[warn] phunspell not available: {e!r}")
        return None, False

    env_aff = os.getenv("HUNSPELL_AFF", "").strip()
    env_dic = os.getenv("HUNSPELL_DIC", "").strip()
    candidates = []

    if env_aff and env_dic:
        candidates.append((env_aff, env_dic))

    candidates += [
        ("/usr/share/hunspell/hu_HU.aff", "/usr/share/hunspell/hu_HU.dic"),
        ("/usr/share/myspell/dicts/hu_HU.aff", "/usr/share/myspell/dicts/hu_HU.dic"),
        ("/usr/share/myspell/hu_HU.aff", "/usr/share/myspell/hu_HU.dic"),
    ]

    for aff, dic in candidates:
        try:
            if os.path.exists(aff) and os.path.exists(dic):
                hs = HunSpell(dic, aff)  # phunspell: HunSpell(dic_path, aff_path)
                log(f"[lang] phunspell OK: {dic} + {aff}")
                return hs, True
        except Exception as e:
            log(f"[warn] phunspell init failed for {dic} / {aff}: {e!r}")

    log("[warn] phunspell available, but Hungarian dictionary not found. "
        "Set HUNSPELL_AFF and HUNSPELL_DIC env vars, or install hu_HU hunspell dict.")
    return None, False


def langdetect_hu_score(text: str, detect_langs_func) -> Optional[float]:
    if not detect_langs_func:
        return None
    t = (text or "").strip()
    if len(t) < 15:
        return 0.0
    try:
        langs = detect_langs_func(t)
        # langs: [hu:0.87, en:0.13] ...
        for lp in langs:
            if getattr(lp, "lang", None) == "hu":
                return float(getattr(lp, "prob", 0.0))
        return 0.0
    except Exception:
        return 0.0


def hunspell_hu_score(text: str, hunspell_obj) -> Optional[float]:
    if hunspell_obj is None:
        return None
    t = (text or "").strip()
    if not t:
        return 0.0

    words = _WORD_RE.findall(t)
    words = [w.lower() for w in words if len(w) >= 2]
    if len(words) < 5:
        return 0.0

    ok = 0
    total = 0
    for w in words[:400]:  # safety cap
        total += 1
        try:
            if hunspell_obj.spell(w):
                ok += 1
        except Exception:
            # if spell fails, ignore that word from ratio
            total -= 1

    if total <= 0:
        return 0.0
    return ok / total


def is_hungarian(text: str, threshold: float, detect_langs_func, hunspell_obj) -> Tuple[bool, float, float]:
    """
    Returns: (is_hu, langdetect_score, hunspell_score)
    If a detector is unavailable -> score = -1.0
    Rule: keep if (langdetect_score >= threshold) OR (hunspell_score >= threshold)
    """
    ld = langdetect_hu_score(text, detect_langs_func)
    hs = hunspell_hu_score(text, hunspell_obj)

    ld_score = float(ld) if ld is not None else -1.0
    hs_score = float(hs) if hs is not None else -1.0

    keep = False
    if ld is not None and ld >= threshold:
        keep = True
    if hs is not None and hs >= threshold:
        keep = True

    return keep, ld_score, hs_score


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
    visited_subs: Optional[set[str]] = None,
    new_subs_seen: Optional[set[str]] = None,
    hu_threshold: Optional[float] = None,
    detect_langs_func=None,
    hunspell_obj=None,
) -> None:
    uname_key = _norm_user(username)
    log(f"[start] Processing u/{uname_key}")

    user = resolve_user(reddit, username)
    if user is None:
        log(f"[done]  Skipped u/{uname_key}")
        return

    visited_subs = visited_subs or set()
    new_subs_seen = new_subs_seen or set()

    # csak egyszer írjuk ki userenként ugyanazt az üzenetet
    logged_visited_subs: set[str] = set()
    logged_new_subs: set[str] = set()
    logged_filter_skips: int = 0  # ne spameljen végtelenül

    ensure_dir(out_dir)
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
            log(f"[dl]   Downloading u/{uname_key} posts ...")
            pbar = tqdm(desc=f"Posts u/{uname_key}", unit="post")
            for s in iter_user_posts(user, before=before, after=after, hard_limit=limit_posts):
                sub = str(getattr(s, "subreddit", "")) or ""
                sub_key = _norm_sub(sub)

                # visited subs -> skip
                if sub_key in visited_subs:
                    if sub_key not in logged_visited_subs:
                        log(f"[skip] r/{sub_key} in visited_subs.txt, skipped (posts)")
                        logged_visited_subs.add(sub_key)
                    pbar.update(1)
                    time.sleep(sleep_s)
                    continue

                # HU filter
                if hu_threshold is not None:
                    title = getattr(s, "title", "") or ""
                    selftext = getattr(s, "selftext", "") or ""
                    text = (title + "\n" + selftext).strip()

                    keep, ld_score, hs_score = is_hungarian(text, hu_threshold, detect_langs_func, hunspell_obj)
                    if not keep:
                        if logged_filter_skips < 5:
                            log(f"[skip] r/{sub_key} not HU enough (posts) ld={ld_score:.3f} hs={hs_score:.3f}")
                            logged_filter_skips += 1
                        pbar.update(1)
                        time.sleep(sleep_s)
                        continue

                # new sub only if we actually keep an item from it
                if sub_key and (sub_key not in logged_new_subs):
                    log(f"[new]  r/{sub_key}")
                    logged_new_subs.add(sub_key)
                    append_new_sub(sub_key, new_subs_seen)

                write_post_block(posts_file, s)
                posts_saved += 1
                pbar.update(1)
                time.sleep(sleep_s)

            pbar.close()
            log(f"[dl]   Finished u/{uname_key} posts. Saved: {posts_saved} -> {posts_path}")

        if include_comments and cmts_file:
            log(f"[dl]   Downloading u/{uname_key} comments ...")
            pbar = tqdm(desc=f"Comments u/{uname_key}", unit="comment")
            for c in iter_user_comments(user, before=before, after=after, hard_limit=limit_comments):
                sub = str(getattr(c, "subreddit", "")) or ""
                sub_key = _norm_sub(sub)

                # visited subs -> skip
                if sub_key in visited_subs:
                    if sub_key not in logged_visited_subs:
                        log(f"[skip] r/{sub_key} in visited_subs.txt, skipped (comments)")
                        logged_visited_subs.add(sub_key)
                    pbar.update(1)
                    time.sleep(sleep_s)
                    continue

                # HU filter
                if hu_threshold is not None:
                    body = getattr(c, "body", "") or ""
                    text = body.strip()

                    keep, ld_score, hs_score = is_hungarian(text, hu_threshold, detect_langs_func, hunspell_obj)
                    if not keep:
                        if logged_filter_skips < 5:
                            log(f"[skip] r/{sub_key} not HU enough (comments) ld={ld_score:.3f} hs={hs_score:.3f}")
                            logged_filter_skips += 1
                        pbar.update(1)
                        time.sleep(sleep_s)
                        continue

                # new sub only if we actually keep an item from it
                if sub_key and (sub_key not in logged_new_subs):
                    log(f"[new]  r/{sub_key}")
                    logged_new_subs.add(sub_key)
                    append_new_sub(sub_key, new_subs_seen)

                write_comment_block(cmts_file, c)
                cmts_saved += 1
                pbar.update(1)
                time.sleep(sleep_s)

            pbar.close()
            log(f"[dl]   Finished u/{uname_key} comments. Saved: {cmts_saved} -> {chats_path}")

    finally:
        if posts_file:
            posts_file.flush()
            posts_file.close()
        if cmts_file:
            cmts_file.flush()
            cmts_file.close()

    log(f"[done] Completed u/{uname_key}")


def main():
    ap = argparse.ArgumentParser(
        description="Reddit user downloader (all posts + comments via PRAW)")

    ap.add_argument(
        "username",
        nargs="*",
        help="e.g. spez (you can pass multiple separated by space)")

    ap.add_argument(
        "--out",
        default=None,
        help="output directory (default: DEFAULT_OUTDIR)")

    ap.add_argument(
        "--after",
        help="lower time bound (epoch or ISO e.g., 2024-01-01)",
        default=None)

    ap.add_argument(
        "--before",
        help="upper time bound (epoch or ISO)",
        default=None)

    ap.add_argument(
        "--limit-posts",
        type=int,
        default=None,
        help="max number of posts per user (None = unlimited)")

    ap.add_argument(
        "--limit-comments",
        type=int,
        default=None,
        help="max number of comments per user (None = unlimited)")

    ap.add_argument(
        "--no-posts",
        action="store_true",
        help="skip posts")

    ap.add_argument(
        "--no-comments",
        action="store_true",
        help="skip comments")

    ap.add_argument(
        "--sleep",
        type=float,
        default=0.5,
        help="sleep between items (seconds)")

    ap.add_argument(
        "--auth-test",
        action="store_true",
        help="only test authentication and exit")

    ap.add_argument(
        "--inputfile",
        default=None,
        help="path to a text file listing usernames (one per line)")

    ap.add_argument(
        "--reset-visited",
        action="store_true",
        help="ignore visited_users.txt and start fresh (this run)")

    ap.add_argument(
        "--filterhu",
        type=float,
        default=None,
        help="Hungarian filter threshold (0..1). Keep item if langdetect OR phunspell >= threshold. Example: --filterhu 0.4",
    )

    args = ap.parse_args()
    after = to_epoch(args.after)
    before = to_epoch(args.before)

    reddit = init_reddit()
    if args.auth_test:
        log("[auth] smoke test successful – exiting (--auth-test)")
        return

    # Load visited subs + new_subs cache
    visited_subs = load_visited_subs()
    new_subs_seen = load_existing_new_subs()
    log(f"[info] Loaded {len(visited_subs)} visited sub(s) from {VISITED_SUBS_FILE}")
    log(f"[info] Loaded {len(new_subs_seen)} already-known new sub(s) from {NEW_SUBS_FILE}")

    # HU filter init (only if requested)
    hu_threshold: Optional[float] = args.filterhu
    detect_langs_func = None
    hunspell_obj = None

    if hu_threshold is not None:
        if hu_threshold < 0.0 or hu_threshold > 1.0:
            raise RuntimeError("--filterhu must be between 0.0 and 1.0")

        detect_langs_func, ld_ok = init_langdetect()
        hunspell_obj, hs_ok = init_hunspell_hu()

        if not ld_ok and not hs_ok:
            raise RuntimeError(
                "HU filter requested but neither langdetect nor phunspell is usable.\n"
                "Install: pip install langdetect phunspell\n"
                "For phunspell you also need Hungarian hunspell dictionaries (hu_HU.aff + hu_HU.dic),\n"
                "or set env vars: HUNSPELL_AFF and HUNSPELL_DIC."
            )

        log(f"[lang] HU filter enabled: threshold={hu_threshold} (langdetect={'OK' if ld_ok else 'NO'}, phunspell={'OK' if hs_ok else 'NO'})")

    # Load usernames
    if args.inputfile:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        inpath = args.inputfile
        if not os.path.isabs(inpath):
            inpath = os.path.join(base_dir, inpath)
        users = load_users_from_file(inpath)
        log(f"[info] Loaded {len(users)} usernames from {inpath}")
    else:
        users = args.username if args.username else DEFAULT_USERS

    outdir = args.out if args.out else DEFAULT_OUTDIR

    # reset visited for this run
    if args.reset_visited and VISITED_FILE.exists():
        log("[info] --reset-visited enabled: ignoring visited_users.txt for this run")
        visited_override = True
    else:
        visited_override = False

    total = len(users)
    for i, u in enumerate(users, start=1):
        if not u.strip():
            continue

        uname_key = _norm_user(u)
        log(f"\n=== [{i}/{total}] Queue: u/{uname_key} ===")

        if (not visited_override) and is_visited(u):
            log(f"[skip] Already processed u/{uname_key}")
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
                visited_subs=visited_subs,
                new_subs_seen=new_subs_seen,
                hu_threshold=hu_threshold,
                detect_langs_func=detect_langs_func,
                hunspell_obj=hunspell_obj,
            )
            add_to_visited(u)

        except Exception as e:
            log(f"[ABORT USER] u/{uname_key} due to failure: {e!r}")
            add_to_timeouts(u)
            continue


if __name__ == "__main__":
    main()

#TODO try on szerver after subreddits are all done last used command:  python main.py --inputfile musers.txt --sleep 0.1 --filterhu 0.5
