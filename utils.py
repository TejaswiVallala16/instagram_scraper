import re

def parse_metric(value):
    """
    Convert Instagram-style shorthand numbers into integers.
    Handles commas, 'k', 'm', and bad cases like '.' safely.
    """
    if value is None:
        return 0
    if isinstance(value, (int, float)):
        return int(value)

    s = str(value).strip().replace(",", "")
    if not s:
        return 0

    # Special case: ignore stray dots or non-numeric
    if s in [".", "-", "_"]:
        return 0

    import re
    m = re.match(r"(?i)^([\d\.]+)\s*([km]?)$", s)
    if not m:
        digits = re.sub(r"[^\d]", "", s)
        return int(digits) if digits else 0

    try:
        num = float(m.group(1))
    except ValueError:
        return 0

    suf = m.group(2).lower()
    if suf == "k":
        num *= 1_000
    elif suf == "m":
        num *= 1_000_000

    return int(round(num))


def compute_engagement_score(likes, comments, views=0):
    """
    Engagement score as a function of likes, comments, and views.
    View weight = 0.02 (tweak as needed).
    """
    likes = parse_metric(likes)
    comments = parse_metric(comments)
    views = parse_metric(views)
    return likes + comments + int(0.02 * views)

def clean_caption(text):
    if text is None:
        return ""
    return " ".join(str(text).split()).strip()

def is_valid_url(url):
    return isinstance(url, str) and url.startswith("http")

def safe_int(x):
    try:
        return int(x)
    except Exception:
        return 0
