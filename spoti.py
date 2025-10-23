import os, io, csv, datetime
from flask import Flask, redirect, request, session, url_for, Response, render_template_string
import spotipy
from spotipy.oauth2 import SpotifyOAuth

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "change-me")

SCOPES = [
    "playlist-read-private",
    "playlist-read-collaborative",
    "playlist-modify-private",
    "playlist-modify-public",
    # добавьте при необходимости:
    # "user-library-read",
]

def sp_oauth():
    # В Spotify Dashboard должен быть добавлен РОВНО этот Redirect URI
    redirect_uri = os.environ["SPOTIPY_REDIRECT_URI"]  # например: https://<yourapp>.onrender.com/callback
    return SpotifyOAuth(
        client_id=os.environ["SPOTIPY_CLIENT_ID"],
        client_secret=os.environ["SPOTIPY_CLIENT_SECRET"],
        redirect_uri=redirect_uri,
        scope=" ".join(SCOPES),
        cache_path=None,   # не храним токены на диске
        show_dialog=False,
    )

def get_sp():
    token_info = session.get("token_info")
    oauth = sp_oauth()
    if not token_info:
        return None
    if oauth.is_token_expired(token_info):
        token_info = oauth.refresh_access_token(token_info["refresh_token"])
        session["token_info"] = token_info
    return spotipy.Spotify(auth=token_info["access_token"])

# ----- Бизнес-логика -----

def iter_playlists(sp):
    limit, offset = 50, 0
    while True:
        page = sp.current_user_playlists(limit=limit, offset=offset)
        for p in page["items"]:
            yield p
        if page["next"]:
            offset += limit
        else:
            break

def iter_playlist_items(sp, pid):
    limit, offset = 100, 0
    while True:
        page = sp.playlist_items(
            pid,
            market="from_token",
            additional_types=["track"],
            limit=limit,
            offset=offset,
        )
        for it in page["items"]:
            yield it
        if page["next"]:
            offset += limit
        else:
            break

def is_unavailable(track, user_country=None):
    if not track or track.get("id") is None:
        return True
    if track.get("is_playable") is False:
        return True
    restr = (track.get("restrictions") or {}).get("reason")
    if restr in {"market", "product", "explicit"}:
        return True
    if user_country and user_country not in (track.get("available_markets") or []):
        return True
    return False

# ----- Маршруты -----

INDEX_HTML = """
<!doctype html>
<title>Недоступные треки</title>
<h1>Собрать недоступные треки</h1>
<p><a href="{{ url_for('run_scan') }}">Запустить</a></p>
{% if session.get('last_result') %}
  <h2>Последний результат</h2>
  <pre>{{ session['last_result'] }}</pre>
  {% if session.get('ghost_rows') %}
    <p><a href="{{ url_for('ghost_csv') }}">Скачать CSV «призраков»</a></p>
  {% endif %}
{% endif %}
"""

@app.route("/")
def index():
    return render_template_string(INDEX_HTML)

@app.route("/login")
def login():
    oauth = sp_oauth()
    auth_url = oauth.get_authorize_url()
    return redirect(auth_url)

@app.route("/callback")
def callback():
    code = request.args.get("code")
    if not code:
        return "No code provided", 400
    oauth = sp_oauth()
    token_info = oauth.get_access_token(code, as_dict=True)
    session["token_info"] = token_info
    return redirect(url_for("run_scan"))

@app.route("/run")
def run_scan():
    sp = get_sp()
    if not sp:
        return redirect(url_for("login"))

    user = sp.current_user()
    user_id = user["id"]
    user_country = user.get("country")

    unplayable_uris = []
    ghost_rows = []

    for pl in iter_playlists(sp):
        pid = pl["id"]
        pname = pl["name"]
        for item in iter_playlist_items(sp, pid):
            t = item.get("track")
            if is_unavailable(t, user_country):
                if t and t.get("uri"):
                    unplayable_uris.append(t["uri"])
                else:
                    ghost_rows.append({
                        "playlist": pname,
                        "added_at": item.get("added_at"),
                        "note": "track object is null (no URI) — removed from catalog"
                    })

    # Создадим плейлист и добавим уникальные URI
    today = datetime.date.today().isoformat()
    new_pl_name = f"Недоступные треки — {today}"
    new_pl = sp.user_playlist_create(user_id, new_pl_name, public=False, description="Собрано автоматически")
    new_pid = new_pl["id"]

    seen, batch = set(), []
    added = 0
    for uri in unplayable_uris:
        if uri in seen:
            continue
        seen.add(uri)
        batch.append(uri)
        if len(batch) == 100:
            sp.playlist_add_items(new_pid, batch)
            added += len(batch)
            batch = []
    if batch:
        sp.playlist_add_items(new_pid, batch)
        added += len(batch)

    # Сохраним «призраков» в сессию для скачивания CSV
    session["ghost_rows"] = ghost_rows

    result = (
        f"Создан плейлист: {new_pl_name}\n"
        f"URI: https://open.spotify.com/playlist/{new_pid}\n"
        f"Добавлено треков с URI: {added}\n"
        f"Найдено «призрачных» без URI: {len(ghost_rows)}"
    )
    session["last_result"] = result
    return render_template_string(INDEX_HTML)

@app.route("/ghost.csv")
def ghost_csv():
    rows = session.get("ghost_rows") or []
    si = io.StringIO()
    writer = csv.DictWriter(si, fieldnames=["playlist", "added_at", "note"])
    writer.writeheader()
    writer.writerows(rows)
    out = si.getvalue()
    return Response(
        out,
        mimetype="text/csv",
        headers={"Content-Disposition": "attachment; filename=ghost_unavailable.csv"},
    )

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))