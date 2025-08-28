# clean_and_rank_100.py
import json, pandas as pd, argparse, re, sys

def parse_metric(value):
    if value is None: return 0
    if isinstance(value, (int, float)): return int(value)
    s = str(value).strip().replace(",", "")
    if not s or s in [".","-","_"]: return 0
    m = re.match(r"(?i)^([\d\.]+)\s*([km]?)$", s)
    if not m:
        digits = re.sub(r"[^\d]", "", s)
        return int(digits) if digits else 0
    try:
        num = float(m.group(1))
    except ValueError:
        return 0
    suf = m.group(2).lower()
    if suf == "k": num *= 1_000
    elif suf == "m": num *= 1_000_000
    return int(round(num))

def compute_engagement_score(likes, comments, views=0):
    return parse_metric(likes) + parse_metric(comments) + int(0.02 * parse_metric(views))

def clean_caption(text):
    return " ".join(str(text or "").split()).strip()

def is_valid_url(url):
    return isinstance(url, str) and url.startswith("http")

COLUMNS = [
    "influencer_username","post_reel_link","likes","comments","views","caption",
    "followers","total_posts","following","engagement_score","engagement_rate"
]

def main(args):
    rows = []
    try:
        with open(args.input, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))
    except FileNotFoundError:
        print(f"[!] Input missing: {args.input}")
        pd.DataFrame(columns=COLUMNS).to_csv(args.output_csv, index=False)
        open(args.output_json,"w",encoding="utf-8").write("[]")
        sys.exit(1)

    cleaned = []
    for r in rows:
        u = (r.get("influencer_username") or "").strip()
        if not u or u.lower() == "sherrihill":
            continue
        link = r.get("post_reel_link") or ""
        if not is_valid_url(link):
            continue

        likes = parse_metric(r.get("likes"))
        comments = parse_metric(r.get("comments"))
        views = parse_metric(r.get("views"))
        followers = parse_metric(r.get("followers"))
        following = parse_metric(r.get("following"))
        total_posts = parse_metric(r.get("total_posts"))

        engagement = compute_engagement_score(likes, comments, views)
        rate = round(engagement / followers, 6) if followers else 0.0

        cleaned.append({
            "influencer_username": u,
            "post_reel_link": link,
            "likes": likes,
            "comments": comments,
            "views": views,
            "caption": clean_caption(r.get("caption")),
            "followers": followers,
            "total_posts": total_posts,
            "following": following,
            "engagement_score": engagement,
            "engagement_rate": rate,
        })

    if not cleaned:
        print("[i] No valid rows after cleaning.")
        pd.DataFrame(columns=COLUMNS).to_csv(args.output_csv, index=False)
        open(args.output_json,"w",encoding="utf-8").write("[]")
        return

    df = pd.DataFrame(cleaned)

    # Ensure we have 1 row per influencer (best post by engagement)
    df.sort_values(["influencer_username","engagement_score"], ascending=[True, False], inplace=True)
    df = df.drop_duplicates(subset=["influencer_username"], keep="first")

    # Sort final by engagement_score desc, tie-breaker engagement_rate desc
    df.sort_values(["engagement_score","engagement_rate"], ascending=[False, False], inplace=True)

    # Keep at most top N if user wants exactly 100 (optional)
    if args.top_n:
        df = df.head(args.top_n)

    df.to_csv(args.output_csv, index=False)
    df.to_json(args.output_json, orient="records", indent=2, force_ascii=False)
    print(f"Saved {len(df)} influencers -> {args.output_csv} | {args.output_json}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="raw_sherrihill_rows.jsonl")
    ap.add_argument("--output_csv", default="sherrihill_influencers_ranked.csv")
    ap.add_argument("--output_json", default="sherrihill_influencers_ranked.json")
    ap.add_argument("--top_n", type=int, default=100, help="Export top N influencers")
    args = ap.parse_args()
    main(args)
