# app.py
import os
import io
import csv
import time
import uuid
import html
import threading
import datetime
from functools import wraps
from typing import Dict, Any, Iterable, Optional

from flask import (
    Flask, redirect, request, session, url_for,
    Response, render_template_string, abort
)

import spotipy
from spotipy.oauth2 import SpotifyOAuth

###############################################################################
# Конфиг и "удобняшки"
###############################################################################

app = Flask(__name__)

# Flask session (для OAuth) — задайте переменную окружения FLASK_SECRET_KEY
app.secret_key = os.environ.get("FLASK_SECRET_KEY", "change-me-please")

# Небольшая пауза между запросами к API (в секундах) — мягкая защита от 429
SLEEP_BETWEEN = float(os.environ.get("RATE_LIMIT_SLEEP", "0.05"))

# Базовая защита паролем (опционально): выставьте BASIC_AUTH_USER/PASS
BASIC_USER = os.environ.get("BASIC_AUTH_USER")
BASIC_PASS = os.environ.get("BASIC_AUTH_PASS")

SCOPES = [
    "playlist-read-private",
    "playlist-read-collaborative",
    "playlist-modify-private",
    "playlist-modify-public",
    "user-library-read",
    # при необходимости добавьте "user-library-read" (для «Понравившихся»)
]

# Простейшее хранилище джобов в памяти процесса
# JOBS[job_id] = {
#   "status": "running"|"done"|"error: ...",
#   "progress": {"processed": int, "total": int},
#   "result": str or None,
#   "ghost_rows": list[dict],
#   "playlist_url": str or None,
# }
JOBS: Dict[str, Dict[str, Any]] = {}
JOBS_LOCK = threading.Lock()


def basic_auth_required(fn):
    """Опциональная базовая авторизация для защищённых маршрутов."""
    if not BASIC_USER or not BASIC_PASS:
        return fn

    @wraps(fn)
    def wrapper(*args, **kwargs):
        auth = request.authorization
        if not auth or not (auth.username == BASIC_USER and auth.password == BASIC_PASS):
            return Response(
                "Auth required",
                401,
                {"WWW-Authenticate": 'Basic realm="Restricted"'},
            )
        return fn(*args, **kwargs)

    return wrapper


def sp_oauth() -> SpotifyOAuth:
    """Инициализация OAuth клиента. Redirect URI должен совпадать 1:1."""
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
    """Достаёт из сессии access_token и создаёт клиента Spotipy с таймаутом."""
    token_info = session.get("token_info")
    if not token_info:
        return None
    oauth = sp_oauth()
    if oauth.is_token_expired(token_info):
        token_info = oauth.refresh_access_token(token_info["refresh_token"])
        session["token_info"] = token_info

    # Таймауты HTTP-запросов
    sp_kwargs = {"requests_timeout": float(os.environ.get("SPOTIPY_TIMEOUT", "20"))}
    # Параметр retries есть не во всех версиях spotipy → пробуем мягко
    try:
        sp = spotipy.Spotify(auth=token_info["access_token"], retries=int(os.environ.get("SPOTIPY_RETRIES", "3")), **sp_kwargs)
    except TypeError:
        sp = spotipy.Spotify(auth=token_info["access_token"], **sp_kwargs)
    return sp


###############################################################################
# Бизнес-логика
###############################################################################

def iter_user_playlists(sp: spotipy.Spotify, max_playlists: Optional[int] = None) -> Iterable[dict]:
    """Итерация по плейлистам пользователя."""
    limit, offset = 50, 0
    yielded = 0
    while True:
        page = sp.current_user_playlists(limit=limit, offset=offset)
        for p in page["items"]:
            yield p
            yielded += 1
            if max_playlists and yielded >= max_playlists:
                return
        if not page["next"]:
            break
        offset += limit
        time.sleep(SLEEP_BETWEEN)


def playlist_total_tracks(sp: spotipy.Spotify, playlist_id: str) -> int:
    """Аккуратно достаём только total, чтобы прикинуть общий объём работы."""
    data = sp.playlist_items(playlist_id, limit=1, fields="total")
    return int(data.get("total") or 0)


def iter_playlist_items(sp: spotipy.Spotify, pid: str) -> Iterable[dict]:
    """Итерация по элементам плейлиста, запрашиваем только нужные поля."""
    limit, offset = 100, 0
    while True:
        page = sp.playlist_items(
            pid,
            market="from_token",
            additional_types=["track"],
            limit=limit,
            offset=offset,
            fields="items(added_at,track(uri,id,is_playable,available_markets,restrictions)),next",
        )
        for it in page["items"]:
            yield it
        if not page.get("next"):
            break
        offset += limit
        time.sleep(SLEEP_BETWEEN)


def is_unavailable(track: Optional[dict], user_country: Optional[str]) -> bool:
    """Определяем недоступность трека в вашем регионе или полное удаление."""
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


def start_job(sp: spotipy.Spotify, *, only_pl_id: Optional[str], max_playlists: Optional[int],
              include_liked: bool, dry_run: bool, batch_size: int) -> str:
    """Стартуем фоновую задачу сканирования."""
    job_id = uuid.uuid4().hex
    with JOBS_LOCK:
        JOBS[job_id] = {
            "status": "running",
            "progress": {"processed": 0, "total": 0},
            "result": None,
            "ghost_rows": [],
            "playlist_url": None,
        }

    def worker():
        try:
            user = sp.current_user()
            user_id = user["id"]
            user_country = user.get("country")

            # Сбор плейлистов к сканированию
            playlists = []
            if only_pl_id:
                pl = sp.playlist(only_pl_id, fields="id,name,owner(id)")
                playlists.append(pl)
            else:
                playlists = list(iter_user_playlists(sp, max_playlists=max_playlists))

            # Прикидываем общий total для прогресса
            total_est = 0
            for p in playlists:
                try:
                    total_est += playlist_total_tracks(sp, p["id"])
                except Exception:
                    # если что-то не получилось — просто не учитываем
                    pass
                time.sleep(SLEEP_BETWEEN)

            if include_liked:
                # Для «Понравившихся» возьмём total=число треков в библиотеке
                liked_page = sp.current_user_saved_tracks(limit=1)
                total_est += int(liked_page.get("total") or 0)

            with JOBS_LOCK:
                JOBS[job_id]["progress"]["total"] = total_est

            seen_uris = set()
            unplayable_uris = []
            ghost_rows = []

            def bump_progress(delta: int = 1):
                with JOBS_LOCK:
                    JOBS[job_id]["progress"]["processed"] += delta

            # Основной скан по плейлистам
            for pl in playlists:
                pid = pl["id"]
                pname = pl.get("name", pid)
                for item in iter_playlist_items(sp, pid):
                    t = item.get("track")
                    if is_unavailable(t, user_country):
                        if t and t.get("uri"):
                            uri = t["uri"]
                            if uri not in seen_uris:
                                seen_uris.add(uri)
                                unplayable_uris.append(uri)
                        else:
                            ghost_rows.append({
                                "playlist": pname,
                                "added_at": item.get("added_at"),
                                "note": "track object is null (no URI) — removed from catalog",
                            })
                    bump_progress()
                # короткий "удых"
                time.sleep(SLEEP_BETWEEN)

            # Опционально сканируем «Понравившиеся»
            if include_liked and "user-library-read" in SCOPES:
                limit, offset = 50, 0
                while True:
                    page = sp.current_user_saved_tracks(limit=limit, offset=offset, market="from_token")
                    for it in page.get("items", []):
                        t = it.get("track")
                        if is_unavailable(t, user_country):
                            if t and t.get("uri"):
                                uri = t["uri"]
                                if uri not in seen_uris:
                                    seen_uris.add(uri)
                                    unplayable_uris.append(uri)
                            else:
                                ghost_rows.append({
                                    "playlist": "Liked Songs",
                                    "added_at": it.get("added_at"),
                                    "note": "track object is null (no URI)",
                                })
                        bump_progress()
                    if not page.get("next"):
                        break
                    offset += limit
                    time.sleep(SLEEP_BETWEEN)

            added = 0
            playlist_url = None
            today = datetime.date.today().isoformat()
            new_pl_name = f"Недоступные треки — {today}"

            if dry_run:
                result = (
                    f"[DRY RUN] Плейлист НЕ создавался.\n"
                    f"Нашли с URI: {len(unplayable_uris)}\n"
                    f"«Призраков» без URI: {len(ghost_rows)}"
                )
            else:
                # Создаём плейлист и добавляем URI батчами
                new_pl = sp.user_playlist_create(
                    user_id, new_pl_name, public=False, description="Собрано автоматически"
                )
                new_pid = new_pl["id"]
                playlist_url = f"https://open.spotify.com/playlist/{new_pid}"

                batch = []
                for uri in unplayable_uris:
                    batch.append(uri)
                    if len(batch) >= batch_size:
                        sp.playlist_add_items(new_pid, batch)
                        added += len(batch)
                        batch = []
                        time.sleep(SLEEP_BETWEEN)
                if batch:
                    sp.playlist_add_items(new_pid, batch)
                    added += len(batch)

                result = (
                    f"Создан плейлист: {new_pl_name}\n"
                    f"URI: {playlist_url}\n"
                    f"Добавлено треков с URI: {added}\n"
                    f"Найдено «призрачных» без URI: {len(ghost_rows)}"
                )

            with JOBS_LOCK:
                JOBS[job_id]["ghost_rows"] = ghost_rows
                JOBS[job_id]["result"] = result
                JOBS[job_id]["playlist_url"] = playlist_url
                JOBS[job_id]["status"] = "done"

        except Exception as e:
            with JOBS_LOCK:
                JOBS[job_id]["status"] = f"error: {e}"

    threading.Thread(target=worker, daemon=True).start()
    return job_id


###############################################################################
# Веб-маршруты
###############################################################################

INDEX_HTML = """
<!doctype html>
<meta charset="utf-8">
<title>Недоступные треки — сборщик</title>
<h1>Собрать недоступные треки</h1>
<p>Этот сервис просканирует ваши плейлисты (и опционально «Понравившиеся»), соберёт недоступные треки в новый плейлист и даст CSV для «призраков» (без URI).</p>

<form action="{{ url_for('start') }}" method="get">
  <fieldset>
    <legend>Параметры запуска</legend>
    <label>Только один плейлист (ID): <input type="text" name="only" placeholder="например, 37i9dQZF1DXcBWIGoYBM5M"></label><br>
    <label>Ограничить количество плейлистов: <input type="number" name="max_playlists" min="1"></label><br>
    <label><input type="checkbox" name="include_liked" value="1"> Сканировать «Понравившиеся» (нужен scope user-library-read)</label><br>
    <label><input type="checkbox" name="dry_run" value="1"> Dry-run (не создавать плейлист, только посчитать)</label><br>
    <label>Batch size при добавлении в плейлист: <input type="number" name="batch_size" value="100" min="1" max="100"></label><br>
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
<p><b>Прогресс:</b> {{ processed }} / {{ total }}{% if total == 0 %} (идёт сканирование, оценка) {% endif %}</p>

{% if result %}
  <h2>Результат</h2>
  <pre style="white-space: pre-wrap">{{ result }}</pre>
  {% if playlist_url %}
    <p><a href="{{ playlist_url }}" target="_blank" rel="noopener">Открыть плейлист</a></p>
  {% endif %}
  <p><a href="{{ url_for('ghost_csv_job', job_id=job_id) }}">Скачать CSV «призраков»</a></p>
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
    auth_url = oauth.get_authorize_url()
    return redirect(auth_url)


@app.route("/callback")
def callback():
    code = request.args.get("code")
    if not code:
        return "No code provided", 400
    oauth = sp_oauth()
    # В spotipy>=2.23 get_access_token возвращает dict
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
    include_liked = request.args.get("include_liked") == "1"
    dry_run = request.args.get("dry_run") == "1"
    batch_size = int(request.args.get("batch_size") or "100")
    batch_size = max(1, min(100, batch_size))

    job_id = start_job(
        sp,
        only_pl_id=only_pl_id,
        max_playlists=max_playlists,
        include_liked=True,
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


@app.route("/ghost/<job_id>.csv")
@basic_auth_required
def ghost_csv_job(job_id: str):
    with JOBS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            abort(404)
        rows = job.get("ghost_rows") or []

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


@app.route("/healthz")
def healthz():
    return "ok", 200


if __name__ == "__main__":
    # Локальный запуск: python app.py
    # На проде используйте gunicorn с увеличенным timeout (см. подсказку выше).
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", "5000")), debug=False)
