import os, sys
import threading
import time
from uuid import uuid4

from flask import Flask, render_template, request, jsonify, send_file
from werkzeug.utils import secure_filename

def resource_path(rel_path: str) -> str:
    base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, rel_path)


def setup_bundled_java() -> str | None:
    """
    Подкладываем portable JRE внутрь onefile и делаем её приоритетной для tabula-py.
    Возвращаем путь к java.exe если нашли.
    """
    java_home = resource_path("jre")
    java_exe = os.path.join(java_home, "bin", "java.exe")

    if os.path.exists(java_exe):
        os.environ["JAVA_HOME"] = java_home
        os.environ["JAVACMD"] = java_exe
        os.environ["JAVA"] = java_exe

        # Важно: java из bundled jre/bin должна быть первой в PATH
        os.environ["PATH"] = os.path.join(java_home, "bin") + os.pathsep + os.environ.get("PATH", "")
        return java_exe

    return None


JAVA_EXE = setup_bundled_java()

import tabula  # noqa: E402

# Пытаемся задать java_path в tabula-py (в 2.10.0 обычно есть tabula.io._java_options)
try:
    if JAVA_EXE and hasattr(tabula, "io") and hasattr(tabula.io, "_java_options"):
        tabula.io._java_options["java_path"] = JAVA_EXE
except Exception:
    pass

from processor import process_pdfs, to_excel_bytes  # noqa: E402


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_DIR = os.path.join(BASE_DIR, "tmp_uploads")
RESULT_DIR = os.path.join(BASE_DIR, "tmp_results")
os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(RESULT_DIR, exist_ok=True)

app = Flask(__name__, template_folder="templates", static_folder="static")
app.config["MAX_CONTENT_LENGTH"] = 200 * 1024 * 1024  # 200MB

STATE = {
    "running": False,
    "logs": [],
    "result_file": None,
    "job_id": None,
    "uploaded_files": [],
}


def add_log(line: str):
    ts = time.strftime("%H:%M:%S")
    STATE["logs"].append(f"[{ts}] {line}")


def cleanup_all():
    # удаление загруженных pdf
    for p in STATE.get("uploaded_files") or []:
        try:
            if p and os.path.exists(p):
                os.remove(p)
        except Exception:
            pass

    # удаление результата
    try:
        p = STATE.get("result_file")
        if p and os.path.exists(p):
            os.remove(p)
    except Exception:
        pass

    # очистка состояния
    STATE["uploaded_files"] = []
    STATE["result_file"] = None
    STATE["logs"] = []
    STATE["job_id"] = None
    STATE["running"] = False


@app.get("/")
def index():
    return render_template("index.html")


@app.post("/upload")
def upload():
    if STATE["running"]:
        return jsonify({"ok": False, "error": "Нельзя загружать во время обработки"}), 409

    files = request.files.getlist("files")
    if not files:
        return jsonify({"ok": False, "error": "Файлы не выбраны"}), 400

    # новая загрузка — чистим хвосты от предыдущих запусков
    cleanup_all()

    saved = []
    skipped = 0

    for f in files:
        name = secure_filename(f.filename or "")
        if not name.lower().endswith(".pdf"):
            skipped += 1
            continue
        uid = uuid4().hex
        path = os.path.join(UPLOAD_DIR, f"{uid}_{name}")
        f.save(path)
        saved.append(path)

    if not saved:
        return jsonify({"ok": False, "error": "Нет PDF для обработки"}), 400

    STATE["uploaded_files"] = saved
    add_log(f"Загружено PDF: {len(saved)} (пропущено не-PDF: {skipped})")
    return jsonify({"ok": True, "count": len(saved)})


def _worker(file_paths):
    try:
        add_log("Старт обработки...")
        df = process_pdfs(file_paths, log=add_log)

        xlsx_bytes = to_excel_bytes(df)
        out_name = f"result_{uuid4().hex}.xlsx"
        out_path = os.path.join(RESULT_DIR, out_name)
        with open(out_path, "wb") as f:
            f.write(xlsx_bytes)

        STATE["result_file"] = out_path
        add_log("Excel сформирован. Можно скачивать.")
    except Exception as e:
        add_log(f"ERROR: {e}")
    finally:
        STATE["running"] = False
        add_log("Завершено.")


@app.post("/start")
def start():
    if STATE["running"]:
        return jsonify({"ok": False, "error": "Уже выполняется"}), 409

    files = STATE.get("uploaded_files") or []
    if not files:
        return jsonify({"ok": False, "error": "Сначала загрузите PDF"}), 400

    STATE["running"] = True
    STATE["result_file"] = None
    STATE["job_id"] = uuid4().hex
    add_log("Запущена задача...")

    t = threading.Thread(target=_worker, args=(files,), daemon=True)
    t.start()
    return jsonify({"ok": True, "job_id": STATE["job_id"]})


@app.get("/status")
def status():
    return jsonify({
        "running": STATE["running"],
        "logs": STATE["logs"][-800:],
        "has_result": STATE["result_file"] is not None,
        "uploaded": len(STATE.get("uploaded_files") or []),
    })


@app.get("/download")
def download():
    path = STATE.get("result_file")
    if not path or not os.path.exists(path):
        return jsonify({"ok": False, "error": "Результат не готов"}), 404

    # Только отдаём файл. Очистку делаем отдельным /reset после успешного скачивания на фронте.
    return send_file(
        path,
        as_attachment=True,
        download_name="result.xlsx",
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        conditional=False,  # меньше шансов на 304/частичные ответы
        max_age=0
    )


@app.post("/reset")
def reset():
    # нельзя сбрасывать во время выполнения
    if STATE["running"]:
        return jsonify({"ok": False, "error": "Нельзя очищать во время обработки"}), 409
    cleanup_all()
    return jsonify({"ok": True})


@app.post("/shutdown")
def shutdown():
    """
    Попытка завершить dev-сервер при закрытии вкладки.
    В проде так обычно не делают (gunicorn/uwsgi управляются иначе).
    """
    func = request.environ.get("werkzeug.server.shutdown")
    if func is None:
        return jsonify({"ok": False, "error": "Shutdown доступен только на dev-сервере Werkzeug"}), 400
    cleanup_all()
    func()
    return jsonify({"ok": True})


if __name__ == "__main__":
    import webbrowser
    from threading import Timer

    def open_browser():
        webbrowser.open("http://127.0.0.1:5000")

    Timer(0.8, open_browser).start()
    app.run(host="127.0.0.1", port=5000, debug=False, threaded=True)
