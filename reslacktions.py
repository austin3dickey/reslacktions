import os
import pathlib
import time
from typing import Optional

import pandas as pd
from tqdm import tqdm
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError


client = WebClient(token=os.environ["SLACK_TOKEN"])


def get_users() -> dict:
    """Get all user IDs and names in this Slack team

    Returns:
        A dict like {'USERID': 'User Name'}
    """
    res = client.users_list().validate().data
    return {user["id"]: user["profile"]["real_name"].replace("/", " ") for user in res["members"]}


def get_reactions(user_id: str, page_size: int) -> pd.DataFrame:
    """Get all reactions someone has ever made

    Args:
        user_id: The user_id to filter to
        page_size: The max number of items to return

    Returns:
        A pd.DataFrame with columns 'emoji' (the emoji name) and 'count' (the number of times this
        user reacted with this emoji)
    """
    user_reactions = {}
    cursor = get_one_page(user_reactions, user_id, page_size, None)
    while cursor:
        cursor = get_one_page(user_reactions, user_id, page_size, cursor)

    return pd.DataFrame(data=user_reactions.items(), columns=["emoji", "count"])


def get_one_page(
    user_reactions: dict, user_id: str, page_size: int, cursor: Optional[str]
) -> Optional[str]:
    """Modifies the user_reactions dict in place, adding reaction counts based on one page of data

    Args:
        user_reactions: The dict of {"reaction_name": <count>}
        user_id: The user_id to filter to
        page_size: The max number of items to return
        cursor: Optional cursor in pagination

    Returns:
        A cursor to the next page, or None or an empty string if it's done
    """
    tqdm.write("Getting a page")
    kwargs = {"user": user_id, "count": page_size}
    if cursor:
        kwargs["cursor"] = cursor

    try:
        res = client.reactions_list(**kwargs).validate().data
    except SlackApiError as e:
        if e.response["error"] == "ratelimited":
            delay = int(e.response.headers["Retry-After"]) + 2
            tqdm.write(f"Rate limited. Retrying in {delay} seconds")
            time.sleep(delay)
            res = client.reactions_list(**kwargs).validate().data
        elif e.response["error"] == "internal_error":
            res = {"items": [], "response_metadata": {}}
        else:
            raise e

    for item in res["items"]:
        if "message" in item:
            reacts = item["message"]["reactions"]
        elif "comment" in item:
            reacts = item["comment"]["reactions"]
        elif "file" in item:
            reacts = item["file"]["reactions"]
        else:
            reacts = []

        for react in reacts:
            if user_id in react["users"]:
                user_reactions[react["name"]] = user_reactions.get(react["name"], 0) + 1

    return res["response_metadata"].get("next_cursor", None)


if __name__ == "__main__":
    users = get_users()
    all_reactions = {}
    react_dir = pathlib.Path("reactions")
    os.makedirs(react_dir, exist_ok=True)
    for user_id, user_name in tqdm(users.items()):
        file = react_dir / f"{user_name}.pkl"
        if not file.exists():
            tqdm.write(f"Querying {user_name}")
            # cache to file in case something happens
            get_reactions(user_id, page_size=1000).to_pickle(file)

    # read them back in
    files = react_dir.glob("*.pkl")
    react_dict = {file.parts[1][:-4]: pd.read_pickle(file) for file in files}
    all_reacts = pd.concat(react_dict, names=["name"])
    all_reacts = all_reacts.reset_index(-1, drop=True).reset_index()
    all_reacts.to_csv("all_reactions.csv", index=False)
