from flask import Flask, render_template, request, jsonify
import urllib.request
import urllib.parse
import json
from datetime import datetime, timedelta

app = Flask(__name__)

API_BASE = "https://osmsg-1.onrender.com/api/v1/user-stats"


@app.context_processor
def inject_globals():
    return {
        "current_year": datetime.utcnow().year
    }


def _parse_dates(daterange_str):

    now = datetime.utcnow()
    yesterday = now - timedelta(days=1)

    fallback = (
        yesterday.strftime("%Y-%m-%dT00:00:00Z"),
        now.strftime("%Y-%m-%dT23:59:59Z"),
    )

    if not daterange_str:
        return fallback

    try:
        if "to" in daterange_str:
            left, right = daterange_str.split("to", 1)
            d1 = datetime.strptime(left.strip(), "%d-%m-%Y")
            d2 = datetime.strptime(right.strip(), "%d-%m-%Y")
        else:
            d1 = d2 = datetime.strptime(daterange_str.strip(), "%d-%m-%Y")

        return (
            d1.strftime("%Y-%m-%dT00:00:00Z"),
            d2.strftime("%Y-%m-%dT23:59:59Z"),
        )

    except (ValueError, AttributeError):
        return fallback


def _fetch(params):

    url = f"{API_BASE}?{urllib.parse.urlencode(params)}"

    req = urllib.request.Request(
        url,
        headers={"Accept": "application/json"}
    )

    with urllib.request.urlopen(req, timeout=90) as resp:
        raw = resp.read().decode()
        if not raw.strip():
            return {}
        return json.loads(raw)



@app.route("/")
def index():
    return render_template("index.html")

@app.route("/statistics")
def statistics():

    hashtag   = request.args.get("hashtag", "").strip()
    daterange = request.args.get("daterange", "").strip()
    limit     = int(request.args.get("limit", 25))
    offset    = int(request.args.get("offset", 0))

    start, end = _parse_dates(daterange)

    params = {
        "start":  start,
        "end":    end,
        "limit":  limit,
        "offset": offset,
    }

    if hashtag:
        params["hashtag"] = hashtag

    try:
        raw   = _fetch(params)
        users = raw.get("users", [])
        count = raw.get("count", len(users))
        meta  = {
            "start":   raw.get("start",   start),
            "end":     raw.get("end",     end),
            "hashtag": raw.get("hashtag", hashtag),
            "limit":   raw.get("limit",   limit),
            "offset":  raw.get("offset",  offset),
            "count":   count,
        }
        error = None

    except Exception as exc:
        users = []
        meta  = {
            "start":   start,
            "end":     end,
            "hashtag": hashtag,
            "limit":   limit,
            "offset":  offset,
            "count":   0,
        }
        error = str(exc)

    is_htmx  = request.headers.get("HX-Request") == "true"
    template = "partial/table.html" if is_htmx else "statistics.html"

    return render_template(
        template,
        hashtag=hashtag,
        daterange=daterange,
        users=users,
        meta=meta,
        error=error,
        limit=limit,
        offset=offset,
    )


@app.route("/api/proxy")
def api_proxy():

    hashtag   = request.args.get("hashtag", "").strip()
    daterange = request.args.get("daterange", "").strip()
    limit     = int(request.args.get("limit", 25))
    offset    = int(request.args.get("offset", 0))

    start, end = _parse_dates(daterange)

    params = {
        "start":  start,
        "end":    end,
        "limit":  limit,
        "offset": offset,
    }

    if hashtag:
        params["hashtag"] = hashtag

    try:
        return jsonify(_fetch(params))

    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")
        return jsonify({"error": f"HTTP {exc.code}", "detail": body}),

    except urllib.error.URLError as exc:
        return jsonify({"error": "Upstream unreachable", "detail": str(exc.reason)}), 

    except Exception as exc:
        return jsonify({"error": str(exc)})


@app.route("/about")
def about():
    return render_template("about.html")


@app.route("/contact")
def contact():
    return render_template("contact.html")


if __name__ == "__main__":
    app.run(debug=True, port=5004)