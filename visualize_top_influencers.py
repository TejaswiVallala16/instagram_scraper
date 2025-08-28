import argparse, pandas as pd
import matplotlib.pyplot as plt

def main(args):
    df = pd.read_csv(args.input)
    top = df.head(10).copy()
    plt.figure()
    plt.bar(top["influencer_username"], top["engagement_score"])
    plt.xticks(rotation=45, ha="right")
    plt.title("Top 10 by Engagement Score (Sherri Hill Collabs)")
    plt.xlabel("Influencer")
    plt.ylabel("Engagement Score")
    plt.tight_layout()
    plt.savefig(args.output)
    print("Saved chart to", args.output)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="sherrihill_influencers_ranked.csv")
    parser.add_argument("--output", default="top10_engagement.png")
    args = parser.parse_args()
    main(args)
