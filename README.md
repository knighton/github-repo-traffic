# github-repo-traffic

Steps:

1. Populate `data/config.json`. It should look like:

```
{
    "token": <your github personal access token>,
    "repos": <list of repos to track>,
    "raw": "data/raw.jsonl",
    "proc": "data/proc/",
    "plot": "data/plot/"
}
```

2. `python3 fetch.py` (run this daily, or whenever)

3. `python3 process.py` (generate clean data to plot)

4. `python3 plot.py` (plot it)
