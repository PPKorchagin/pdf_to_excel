let pollTimer = null;

const fileInput = document.getElementById("fileInput");
const btnUpload = document.getElementById("btnUpload");
const btnStart = document.getElementById("btnStart");
const btnDownload = document.getElementById("btnDownload");
const logBox = document.getElementById("logBox");
const fileList = document.getElementById("fileList");
const statusBox = document.getElementById("statusBox");

function setStartEnabled(enabled) { btnStart.disabled = !enabled; }
function setDownloadEnabled(enabled) { btnDownload.disabled = !enabled; }

function renderFilePills(files) {
  fileList.innerHTML = "";
  for (const f of files) {
    const el = document.createElement("div");
    el.className = "pill";
    el.textContent = f.name;
    fileList.appendChild(el);
  }
}

function setStatus(text) { statusBox.textContent = text; }

function renderLogs(lines) {
  logBox.textContent = (lines || []).join("\n");
  logBox.scrollTop = logBox.scrollHeight;
}

function resetUI() {
  // сброс визуального состояния страницы
  fileInput.value = "";
  fileList.innerHTML = "";
  logBox.textContent = "";
  setStatus("Ожидание...");
  setStartEnabled(false);
  setDownloadEnabled(false);
}

async function pollStatus() {
  const r = await fetch("/status", { cache: "no-store" });
  const st = await r.json();

  renderLogs(st.logs);
  setStatus(
    st.running
      ? `Выполняется... Загружено PDF: ${st.uploaded}`
      : `Ожидание. Загружено PDF: ${st.uploaded}`
  );

  setStartEnabled(!st.running && st.uploaded > 0);
  setDownloadEnabled(st.has_result && !st.running);

  if (!st.running && pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
}

btnUpload.addEventListener("click", async () => {
  const files = fileInput.files;
  if (!files || files.length === 0) return;

  renderFilePills(Array.from(files));
  setStatus("Загрузка файлов...");
  renderLogs(["Загрузка..."]);

  const fd = new FormData();
  for (const f of files) fd.append("files", f);

  const r = await fetch("/upload", { method: "POST", body: fd });
  const j = await r.json();

  if (!j.ok) {
    alert(j.error || "Ошибка загрузки");
    await pollStatus();
    return;
  }

  setStartEnabled(true);
  setDownloadEnabled(false);
  await pollStatus();
});

btnStart.addEventListener("click", async () => {
  setStartEnabled(false);
  setDownloadEnabled(false);
  renderLogs(["Запуск..."]);
  setStatus("Старт...");

  const r = await fetch("/start", { method: "POST" });
  const j = await r.json();

  if (!j.ok) {
    alert(j.error || "Ошибка старта");
    await pollStatus();
    return;
  }

  pollTimer = setInterval(pollStatus, 700);
});

btnDownload.addEventListener("click", async () => {
  btnDownload.disabled = true;
  setStatus("Скачивание Excel...");

  const resp = await fetch("/download", { method: "GET" });
  if (!resp.ok) {
    const j = await resp.json().catch(() => null);
    alert((j && j.error) ? j.error : "Не удалось скачать файл");
    await pollStatus();
    return;
  }

  const blob = await resp.blob();

  // сохраняем файл
  const url = window.URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "result.xlsx";
  document.body.appendChild(a);
  a.click();
  a.remove();
  window.URL.revokeObjectURL(url);

  // после успешного скачивания — сбрасываем серверное состояние и чистим файлы
  await fetch("/reset", { method: "POST" });

  // сброс UI на “как при первом открытии”
  resetUI();
});

// попытка завершить dev-сервер при закрытии вкладки (как было)
window.addEventListener("beforeunload", () => {
  try {
    const data = new Blob([JSON.stringify({})], { type: "application/json" });
    navigator.sendBeacon("/shutdown", data);
  } catch (e) {}
});

// первичный статус
pollStatus();
