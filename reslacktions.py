import os
import pathlib

import pandas as pd
from slack_sdk import WebClient


client = WebClient(token=os.environ["SLACK_TOKEN"])


def get_users() -> dict:
    """Get all user IDs and names in this Slack team"""
    res = client.users_list().validate().data
    return {user["id"]: user["profile"]["real_name"] for user in res["members"]}


def get_reactions(user_id: str, page_size: int) -> pd.DataFrame:
    """Get all reactions someone has ever made"""
    user_reactions = {}
    current_page = 0
    all_pages = 1
    while current_page < all_pages:
        print(f"Getting page {current_page + 1} of {all_pages}")
        res = client.reactions_list(
            user=user_id, count=page_size, page=current_page + 1
        )
        res = res.validate().data
        current_page = res["paging"]["page"]
        all_pages = res["paging"]["pages"]
        for item in res["items"]:
            for react_name in extract_reactions(item, user_id):
                user_reactions[react_name] = user_reactions.get(react_name, 0) + 1
    return pd.DataFrame(data=user_reactions.items(), columns=["emoji", "count"])


def extract_reactions(item: dict, user_id: str) -> dict:
    """Slack returns a mixture of messages and file info. Parse through it to get the names of the
    reactions this user reacted with.
    """
    if "message" in item:
        reacts = item["message"]["reactions"]
    elif "comment" in item:
        reacts = item["comment"]["reactions"]
    elif "file" in item:
        reacts = item["file"]["reactions"]
    else:
        reacts = []

    return [react["name"] for react in reacts if user_id in react["users"]]


if __name__ == "__main__":
    users = get_users()
    all_reactions = {}
    react_dir = pathlib.Path("reactions")
    os.makedirs(react_dir, exist_ok=True)
    for user_id, user_name in users.items():
        file = react_dir / f"{user_name}.pkl"
        if not file.exists():
            print(f"Querying {user_name}")
            # cache to file in case something happens
            get_reactions(user_id, 100).to_pickle(file)

    # read them back in
    files = react_dir.glob("*.pkl")
    react_dict = {file.parts[1][:-4]: pd.read_pickle(file) for file in files}
    all_reacts = pd.concat(react_dict, names=["name"])
    all_reacts = all_reacts.reset_index(-1, drop=True).reset_index()
    all_reacts.to_csv("all_reactions.csv")
