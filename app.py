# app.py
import os
import io
import re
import csv
import time
import uuid
import threading
import datetime
from typing import Dict, Any, Iterable, Optional, List, Tuple

from flask import (
    Flask, redirect, request, session, url_for,
    Response, render_template_string, abort
)

import spotipy
from spotipy.oauth2 import SpotifyOAuth

# Пытаемся импортировать тип исключения Spotipy (версионные различия учитываем)
try:
    from spotipy.exceptions import SpotifyException
except Exception:  # pragma: no cover
    class SpotifyException(Exception):
        pass

###############################################################################
# Конфиг
###############################################################################

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "change-me-please")

# Пауза между запросами (смягчает 429)
SLEEP_BETWEEN = float(os.environ.get("RATE_LIMIT_SLEEP", "0.05"))

# Базовая защита (опционально)
BASIC_USER = os.environ.get("BASIC_AUTH_USER")
BASIC_PASS = os.environ.get("BASIC_AUTH_PASS")

# ВСЕГДА сканируем «Понравившиеся» → нужен scope user-library-read
SCOPES = [
    "playlist-read-private",
    "playlist-read-collaborative",
    "playlist-modify-private",
    "playlist-modify-public",
    "user-library-read",  # обязательно
]

# Хранилище задач (в памяти процесса)
JOBS: Dict[str, Dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()

# Валидатор трек-URI
URI_RE = re.compile(r"^spotify:track:[0-9A-Za-z]{22}$")


###############################################################################
# Утилиты
###############################################################################

def basic_auth():
    if not BASIC_USER or not BASIC_PASS:
        return True
    auth = request.authorization
    return bool(auth and auth.username == BASIC_USER and auth.password == BASIC_PASS)


def basic_auth_required(fn):
    def wrapper(*args, **kwargs):
        if not basic_auth():
            return Response("Auth required", 401, {"WWW-Authenticate": 'Basic realm="Restricted"'})
        return fn(*args, **kwargs)
    wrapper.__name__ = fn.__name__
    return wrapper


def sp_oauth() -> SpotifyOAuth:
    redirect_uri = os.environ["SPOTIPY_REDIRECT_URI"]
    return SpotifyOAuth(
        client_id=os.environ["SPOTIPY_CLIENT_ID"],
        client_secret=os.environ["SPOTIPY_CLIENT_SECRET"],
        redirect_uri=redirect_uri,
        scope=" ".join(SCOPES),
        cache_path=None,
        show_dialog=False,
    )


def get_sp() -> Optional[spotipy.Spotify]:
    token_info = session.get("token_info")
    if not token_info:
        return None
    oauth = sp_oauth()
    if oauth.is_token_expired(token_info):
        token_info = oauth.refresh_access_token(token_info["refresh_token"])
        session["token_info"] = token_info

    kwargs = {"requests_timeout": float(os.environ.get("SPOTIPY_TIMEOUT", "20"))}
    try:
        sp = spotipy.Spotify(auth=token_info["access_token"], retries=int(os.environ.get("SPOTIPY_RETRIES", "3")), **kwargs)
    except TypeError:
        sp = spotipy.Spotify(auth=token_info["access_token"], **kwargs)
    return sp


def normalize_playlist_id(value: str) -> str:
    """
    Принимает что угодно: ID, spotify:playlist:ID, https://open.spotify.com/playlist/ID
    Возвращает чистый ID.
    """
    if not value:
        return value
    value = value.strip()

    # spotify:playlist:ID
    m = re.match(r"^spotify:playlist:([0-9A-Za-z]{22})$", value)
    if m:
        return m.group(1)

    # https://open.spotify.com/playlist/ID (c ?si=…)
    m = re.search(r"/playlist/([0-9A-Za-z]{22})", value)
    if m:
        return m.group(1)

    # уже ID?
    if re.match(r"^[0-9A-Za-z]{22}$", value):
        return value

    # ничего не подошло — вернём исходное (чтобы сервер честно упал и показал проблему)
    return value


###############################################################################
# Доступ к API
###############################################################################

def iter_user_playlists(sp: spotipy.Spotify, max_playlists: Optional[int] = None) -> Iterable[dict]:
    limit, offset = 50, 0
    yielded = 0
    while True:
        page = sp.current_user_playlists(limit=limit, offset=offset)
        for p in page.get("items", []):
            yield p
            yielded += 1
            if max_playlists and yielded >= max_playlists:
                return
        if not page.get("next"):
            break
        offset += limit
        time.sleep(SLEEP_BETWEEN)


def playlist_total_tracks(sp: spotipy.Spotify, playlist_id: str) -> int:
    data = sp.playlist_items(playlist_id, limit=1, fields="total")
    return int(data.get("total") or 0)


def iter_playlist_items(sp: spotipy.Spotify, pid: str) -> Iterable[dict]:
    limit, offset = 100, 0
    # Запросим ровно то, что нам нужно для CSV/логики
    fields = (
        "items(added_at,track("
        "uri,id,name,artists(name),album(name),"
        "is_playable,available_markets,restrictions"
        ")),next"
    )
    while True:
        page = sp.playlist_items(
            pid,
            market="from_token",
            additional_types=["track"],
            limit=limit,
            offset=offset,
            fields=fields,
        )
        for it in page.get("items", []):
            yield it
        if not page.get("next"):
            break
        offset += limit
        time.sleep(SLEEP_BETWEEN)


def is_unavailable_with_reason(track: Optional[dict], user_country: Optional[str]) -> Tuple[bool, str]:
    reasons: List[str] = []
    if not track or track.get("id") is None:
        reasons.append("no_track_object")
    else:
        if track.get("is_playable") is False:
            reasons.append("is_playable=false")
        restr = (track.get("restrictions") or {}).get("reason")
        if restr:
            reasons.append(f"restriction:{restr}")
        if user_country:
            am = track.get("available_markets") or []
            if user_country not in am:
                reasons.append("not_in_user_market")
    return (len(reasons) > 0, ";".join(reasons))


def safe_add_items(sp: spotipy.Spotify, playlist_id: str, uris: List[str], record_bad_uri) -> int:
    """
    Добавляет uris партиями, при 400/Unsupported URL рекурсивно делит партию,
    изолируя плохие URI. Возвращает число успешно добавленных треков.
    """

    added = 0

    def add_chunk(chunk: List[str]):
        nonlocal added
        if not chunk:
            return
        try:
            sp.playlist_add_items(playlist_id, chunk)
            added += len(chunk)
            time.sleep(SLEEP_BETWEEN)
        except SpotifyException as e:
            # Если партия из одного элемента — логируем и пропускаем
            if len(chunk) == 1:
                record_bad_uri(chunk[0], f"add_failed:{getattr(e, 'msg', str(e))}")
                return
            # Делим пополам и пытаемся добавить каждую половину
            mid = len(chunk) // 2
            add_chunk(chunk[:mid])
            add_chunk(chunk[mid:])

    # Пытаемся добавить максимально крупными кусками (до 100) — сюда уже приходят валидные URI
    i = 0
    while i < len(uris):
        batch = uris[i:i+100]
        add_chunk(batch)
        i += 100
    return added


###############################################################################
# Фоновая задача
###############################################################################

def start_job(sp: spotipy.Spotify, *, only_pl_id: Optional[str], max_playlists: Optional[int],
              dry_run: bool, batch_size: int) -> str:
    job_id = uuid.uuid4().hex
    with JOBS_LOCK:
        JOBS[job_id] = {
            "status": "running",
            "progress": {"processed": 0, "total": 0},
            "result": None,
            "playlist_url": None,
            "csv_rows": [],       # полный список недоступных (для выгрузки)
            "bad_uri_rows": [],   # проблемные URI при добавлении
        }

    def bump_progress(delta: int = 1):
        with JOBS_LOCK:
            JOBS[job_id]["progress"]["processed"] += delta

    def add_csv_row(row: dict):
        with JOBS_LOCK:
            JOBS[job_id]["csv_rows"].append(row)

    def add_bad_uri_row(uri: str, note: str):
        with JOBS_LOCK:
            JOBS[job_id]["bad_uri_rows"].append({"track_uri": uri, "note": note})

    def worker():
        try:
            user = sp.current_user()
            user_id = user["id"]
            user_country = user.get("country")

            # Собираем список плейлистов
            playlists = []
            if only_pl_id:
                norm = normalize_playlist_id(only_pl_id)
                pl = sp.playlist(norm, fields="id,name,owner(id)")
                playlists.append(pl)
            else:
                playlists = list(iter_user_playlists(sp, max_playlists=max_playlists))

            # Оценим общий объём работы (для прогресса)
            total_est = 0
            for p in playlists:
                try:
                    total_est += playlist_total_tracks(sp, p["id"])
                except Exception:
                    pass
                time.sleep(SLEEP_BETWEEN)

            # «Понравившиеся» — всегда сканируем
            liked_page = sp.current_user_saved_tracks(limit=1)
            total_est += int(liked_page.get("total") or 0)

            with JOBS_LOCK:
                JOBS[job_id]["progress"]["total"] = total_est

            seen_uris = set()
            good_uris: List[str] = []  # валидные spotify:track:<22>, которые будем добавлять

            # Скан плейлистов
            for pl in playlists:
                pid = pl["id"]
                pname = pl.get("name", pid)
                for item in iter_playlist_items(sp, pid):
                    t = item.get("track")
                    unavailable, reason = is_unavailable_with_reason(t, user_country)
                    if unavailable:
                        # Для CSV
                        row = {
                            "source": pname,
                            "added_at": item.get("added_at"),
                            "track_name": (t or {}).get("name") if t else "",
                            "artists": ", ".join(a["name"] for a in (t.get("artists") if t else []) or []),
                            "album": (t.get("album") or {}).get("name") if t else "",
                            "track_uri": (t or {}).get("uri") or "",
                            "track_id": (t or {}).get("id") or "",
                            "reason": reason or "",
                        }
                        add_csv_row(row)

                        # Кандидаты в новый плейлист — только валидные track-URI
                        uri = (t or {}).get("uri")
                        if uri and URI_RE.match(uri) and uri not in seen_uris:
                            seen_uris.add(uri)
                            good_uris.append(uri)
                    bump_progress()
                time.sleep(SLEEP_BETWEEN)

            # Скан «Понравившихся»
            limit, offset = 50, 0
            fields = (
                "items(added_at,track("
                "uri,id,name,artists(name),album(name),"
                "is_playable,available_markets,restrictions"
                ")),next,total"
            )
            while True:
                page = sp.current_user_saved_tracks(limit=limit, offset=offset, market="from_token", fields=fields)
                for it in page.get("items", []):
                    t = it.get("track")
                    unavailable, reason = is_unavailable_with_reason(t, user_country)
                    if unavailable:
                        row = {
                            "source": "Liked Songs",
                            "added_at": it.get("added_at"),
                            "track_name": (t or {}).get("name") if t else "",
                            "artists": ", ".join(a["name"] for a in (t.get("artists") if t else []) or []),
                            "album": (t.get("album") or {}).get("name") if t else "",
                            "track_uri": (t or {}).get("uri") or "",
                            "track_id": (t or {}).get("id") or "",
                            "reason": reason or "",
                        }
                        add_csv_row(row)

                        uri = (t or {}).get("uri")
                        if uri and URI_RE.match(uri) and uri not in seen_uris:
                            seen_uris.add(uri)
                            good_uris.append(uri)
                    bump_progress()
                if not page.get("next"):
                    break
                offset += limit
                time.sleep(SLEEP_BETWEEN)

            # Сборка результирующего плейлиста
            today = datetime.date.today().isoformat()
            new_pl_name = f"Недоступные треки — {today}"
            playlist_url = None
            added_total = 0
            skipped_invalid = 0

            # Дополнительная фильтрация на случай мусора
            filtered_uris: List[str] = []
            for u in good_uris:
                if URI_RE.match(u):
                    filtered_uris.append(u)
                else:
                    skipped_invalid += 1
                    add_bad_uri_row(u, "invalid_uri_format")

            if dry_run:
                result = (
                    f"[DRY RUN] Плейлист НЕ создавался.\n"
                    f"Годных для добавления URI: {len(filtered_uris)}\n"
                    f"Пропущено из-за неверного формата: {skipped_invalid}\n"
                    f"Всего недоступных (для CSV): {len(JOBS[job_id]['csv_rows'])}"
                )
            else:
                new_pl = sp.user_playlist_create(
                    user_id, new_pl_name, public=False, description="Собрано автоматически"
                )
                new_pid = new_pl["id"]
                playlist_url = f"https://open.spotify.com/playlist/{new_pid}"

                # Добавляем «умно»: пытаемся батчами, при ошибке — раскалываем
                # (batch_size ограничивает начальный размер партии; safe_add_items сам бьёт до единичных)
                i = 0
                while i < len(filtered_uris):
                    batch = filtered_uris[i:i+batch_size]
                    added_total += safe_add_items(sp, new_pid, batch, add_bad_uri_row)
                    i += batch_size

                result = (
                    f"Создан плейлист: {new_pl_name}\n"
                    f"URI: {playlist_url}\n"
                    f"Добавлено треков: {added_total}\n"
                    f"Отброшено по формату: {skipped_invalid}\n"
                    f"Проблемных при добавлении (см. CSV baduris): {len(JOBS[job_id]['bad_uri_rows'])}\n"
                    f"Всего недоступных (в выгрузке CSV): {len(JOBS[job_id]['csv_rows'])}"
                )

            with JOBS_LOCK:
                JOBS[job_id]["playlist_url"] = playlist_url
                JOBS[job_id]["result"] = result
                JOBS[job_id]["status"] = "done"

        except Exception as e:
            with JOBS_LOCK:
                JOBS[job_id]["status"] = f"error: {e}"

    threading.Thread(target=worker, daemon=True).start()
    return job_id


###############################################################################
# Веб-маршруты и шаблоны
###############################################################################

INDEX_HTML = """
<!doctype html>
<meta charset="utf-8">
<title>Недоступные треки — сборщик</title>
<h1>Собрать недоступные треки</h1>
<p>Просканирует ваши плейлисты и «Понравившиеся», соберёт недоступные, создаст плейлист и выгрузит CSV.</p>

<form action="{{ url_for('start') }}" method="get">
  <fieldset>
    <legend>Параметры</legend>
    <label>Сканировать только один плейлист (ID/URI/URL): <input type="text" name="only" placeholder="ID или ссылка"></label><br>
    <label>Ограничить количество плейлистов: <input type="number" name="max_playlists" min="1"></label><br>
    <label><input type="checkbox" name="dry_run" value="1"> Dry-run (не создавать плейлист, только посчитать)</label><br>
    <label>Начальный Batch Size (1–100): <input type="number" name="batch_size" value="100" min="1" max="100"></label><br>
  </fieldset>
  <p><button type="submit">Запустить</button></p>
</form>

{% if last_job %}
  <p>Последняя задача: <a href="{{ url_for('status', job_id=last_job) }}">{{ last_job }}</a></p>
{% endif %}
"""

STATUS_HTML = """
<!doctype html>
<meta charset="utf-8">
<title>Статус задачи {{ job_id }}</title>
{% if running %}
  <meta http-equiv="refresh" content="3">
{% endif %}
<h1>Статус задачи</h1>
<p><b>ID:</b> {{ job_id }}</p>
<p><b>Состояние:</b> {{ status }}</p>
<p><b>Прогресс:</b> {{ processed }} / {{ total }}</p>

{% if result %}
  <h2>Результат</h2>
  <pre style="white-space: pre-wrap">{{ result }}</pre>
  {% if playlist_url %}
    <p><a href="{{ playlist_url }}" target="_blank" rel="noopener">Открыть плейлист</a></p>
  {% endif %}
  <h2>Выгрузки CSV</h2>
  <ul>
    <li><a href="{{ url_for('csv_unavailable', job_id=job_id) }}">Все недоступные (unavailable)</a></li>
    <li><a href="{{ url_for('csv_baduris', job_id=job_id) }}">Проблемные URI при добавлении (baduris)</a></li>
  </ul>
{% endif %}

<p><a href="{{ url_for('index') }}">← На главную</a></p>
"""

@app.route("/")
@basic_auth_required
def index():
    last_job = session.get("last_job_id")
    return render_template_string(INDEX_HTML, last_job=last_job)

@app.route("/login")
def login():
    oauth = sp_oauth()
    return redirect(oauth.get_authorize_url())

@app.route("/callback")
def callback():
    code = request.args.get("code")
    if not code:
        return "No code provided", 400
    oauth = sp_oauth()
    token_info = oauth.get_access_token(code, as_dict=True)
    session["token_info"] = token_info
    return redirect(url_for("start"))

@app.route("/start")
@basic_auth_required
def start():
    sp = get_sp()
    if not sp:
        return redirect(url_for("login"))

    only_pl_id = request.args.get("only") or None
    max_playlists = request.args.get("max_playlists")
    max_playlists = int(max_playlists) if max_playlists else None
    dry_run = request.args.get("dry_run") == "1"
    batch_size = int(request.args.get("batch_size") or "100")
    batch_size = max(1, min(100, batch_size))

    job_id = start_job(
        sp,
        only_pl_id=only_pl_id,
        max_playlists=max_playlists,
        dry_run=dry_run,
        batch_size=batch_size,
    )
    session["last_job_id"] = job_id
    return redirect(url_for("status", job_id=job_id))

@app.route("/status/<job_id>")
@basic_auth_required
def status(job_id: str):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            abort(404)
        status_str = job["status"]
        running = status_str == "running"
        processed = job["progress"]["processed"]
        total = job["progress"]["total"]
        result = job["result"]
        playlist_url = job["playlist_url"]

    return render_template_string(
        STATUS_HTML,
        job_id=job_id,
        status=status_str,
        running=running,
        processed=processed,
        total=total,
        result=result,
        playlist_url=playlist_url,
    )

@app.route("/unavailable/<job_id>.csv")
@basic_auth_required
def csv_unavailable(job_id: str):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            abort(404)
        rows = job.get("csv_rows") or []

    # Гарантируем стабильный порядок колонок
    cols = ["source", "added_at", "track_name", "artists", "album", "track_uri", "track_id", "reason"]
    si = io.StringIO()
    w = csv.DictWriter(si, fieldnames=cols)
    w.writeheader()
    for r in rows:
        w.writerow({k: r.get(k, "") for k in cols})
    out = si.getvalue()
    return Response(out, mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=unavailable_{job_id}.csv"})

@app.route("/baduris/<job_id>.csv")
@basic_auth_required
def csv_baduris(job_id: str):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            abort(404)
        rows = job.get("bad_uri_rows") or []

    si = io.StringIO()
    w = csv.DictWriter(si, fieldnames=["track_uri", "note"])
    w.writeheader()
    w.writerows(rows)
    out = si.getvalue()
    return Response(out, mimetype="text/csv",
                    headers={"Content-Disposition": f"attachment; filename=baduris_{job_id}.csv"})

@app.route("/healthz")
def healthz():
    return "ok", 200

if __name__ == "__main__":
    # Локальный запуск (на проде используйте gunicorn с увеличенным таймаутом)
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=False)
