import os
import ffmpeg
import tempfile
import threading
import asyncio
import uuid
from datetime import timedelta
import torch
from fastapi import FastAPI, UploadFile, HTTPException, Form, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
import whisperx
from deep_translator import GoogleTranslator

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory="frontend"), name="static")

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
COMPUTE_TYPE = "float16" if DEVICE == "cuda" else "float32"
HF_TOKEN = os.getenv("HF_TOKEN", None)

LANG_MAP = {
    "pt": "pt",
    "ing": "en",
    "jp": "ja",
}

QUALITY_CONFIG = {
    "rapido": {"model": "small", "batch_size": 16, "align": False},
    "bom": {"model": "medium", "batch_size": 8, "align": True},
    "perfeito": {"model": "large-v2", "batch_size": 4, "align": True},
}


class WhisperXRuntime:
    def __init__(self):
        self.device = DEVICE
        self.compute_type = COMPUTE_TYPE
        self._models = {}
        self._model_lock = threading.Lock()
        self._align_models = {}
        self._align_lock = threading.Lock()
        self._diarize_pipeline = None
        self._diarize_lock = threading.Lock()

    def get_model(self, model_name):
        with self._model_lock:
            if model_name not in self._models:
                self._models[model_name] = whisperx.load_model(
                    model_name,
                    self.device,
                    compute_type=self.compute_type,
                    language="pt",
                )
            return self._models[model_name]

    def get_align_model(self, language_code):
        with self._align_lock:
            if language_code not in self._align_models:
                self._align_models[language_code] = whisperx.load_align_model(
                    language_code=language_code,
                    device=self.device,
                )
            return self._align_models[language_code]

    def get_diarization_pipeline(self):
        if not HF_TOKEN:
            raise RuntimeError(
                "HF_TOKEN não configurado. Defina a variável de ambiente para habilitar diferenciação de narrador."
            )
        with self._diarize_lock:
            if self._diarize_pipeline is None:
                self._diarize_pipeline = whisperx.DiarizationPipeline(
                    use_auth_token=HF_TOKEN,
                    device=self.device,
                )
            return self._diarize_pipeline


runtime = WhisperXRuntime()
last_export_lock = threading.Lock()
last_export_payload = None


def format_srt_time(seconds_value):
    total_ms = int(seconds_value * 1000)
    td = timedelta(milliseconds=total_ms)
    total_seconds = int(td.total_seconds())
    hours = total_seconds // 3600
    minutes = (total_seconds % 3600) // 60
    seconds = total_seconds % 60
    millis = total_ms % 1000
    return f"{hours:02}:{minutes:02}:{seconds:02},{millis:03}"


def build_output_text(segments, with_timestamp=False, with_speaker=False):
    if with_timestamp:
        lines = []
        for idx, seg in enumerate(segments, start=1):
            start = format_srt_time(float(seg.get("start", 0.0)))
            end = format_srt_time(float(seg.get("end", seg.get("start", 0.0))))
            text = seg.get("text", "").strip()
            if with_speaker and seg.get("speaker"):
                text = f"{seg['speaker']}: {text}"
            lines.append(f"{idx}\n{start} --> {end}\n{text}\n")
        return "\n".join(lines).strip()

    texts = []
    for seg in segments:
        text = seg.get("text", "").strip()
        if not text:
            continue
        if with_speaker and seg.get("speaker"):
            texts.append(f"{seg['speaker']}: {text}")
        else:
            texts.append(text)
    return " ".join(texts).strip()

# Controle global (suporta UMA transcrição ativa de cada vez)
current_task_lock = threading.Lock()
current_stop_event = None  # threading.Event quando há uma transcrição ativa

def convert_to_wav(input_path):
    output_path = os.path.splitext(input_path)[0] + "_converted.wav"
    (
        ffmpeg.input(input_path)
        .output(output_path, ac=1, ar=16000)
        .overwrite_output()
        .run(capture_stdout=True, capture_stderr=True)
    )
    return output_path

def transcribe_with_cancel(
    wav_path,
    stop_event,
    with_timestamp=False,
    with_speaker=False,
    target_language="pt",
    precision="bom",
):
    """
    Função executada em executor (thread). Itera sobre os segmentos e
    verifica stop_event a cada segmento.
    Retorna (aborted_flag, text)
    """
    cfg = QUALITY_CONFIG.get(precision, QUALITY_CONFIG["bom"])
    audio = whisperx.load_audio(wav_path)
    if stop_event.is_set():
        return True, "", [], "pt"

    model = runtime.get_model(cfg["model"])
    result = model.transcribe(
        audio,
        batch_size=cfg["batch_size"],
        language="pt",
    )
    segments = result.get("segments", [])
    detected_language = result.get("language") or "pt"

    if stop_event.is_set():
        partial_text = build_output_text(segments, with_timestamp=with_timestamp, with_speaker=False)
        return True, partial_text, segments, detected_language

    if segments and cfg["align"]:
        try:
            model_a, metadata = runtime.get_align_model(detected_language)
            aligned = whisperx.align(
                segments,
                model_a,
                metadata,
                audio,
                runtime.device,
                return_char_alignments=False,
            )
            if isinstance(aligned, dict) and aligned.get("segments"):
                segments = aligned["segments"]
        except Exception:
            # Se alinhamento falhar, retorna transcrição base.
            pass

    if with_speaker and segments:
        try:
            diarize = runtime.get_diarization_pipeline()
            diarize_segments = diarize(audio)
            speaker_assigned = whisperx.assign_word_speakers(diarize_segments, {"segments": segments})
            if isinstance(speaker_assigned, dict) and speaker_assigned.get("segments"):
                segments = speaker_assigned["segments"]
        except Exception:
            for seg in segments:
                seg.setdefault("speaker", "NARRADOR")

    target_code = LANG_MAP.get(target_language, "pt")
    if target_code != detected_language and segments:
        for seg in segments:
            text_val = seg.get("text", "").strip()
            if not text_val:
                continue
            try:
                translator = GoogleTranslator(source_language=detected_language, target_language=target_code)
                seg["text"] = translator.translate(text_val)
            except Exception:
                # Mantém texto original se tradução falhar (rede/provedor).
                seg["text"] = text_val

    text = build_output_text(segments, with_timestamp=with_timestamp, with_speaker=with_speaker)
    return stop_event.is_set(), text, segments, detected_language

@app.post("/transcribe")
async def transcribe(
    audio: UploadFile = File(...),
    timestamp: bool = Form(False),
    diferenciar_narrador: bool = Form(False),
    idioma: str = Form("pt"),
    precisao: str = Form("bom"),
):
    global current_stop_event
    # bloqueia para garantir apenas uma transcrição ativa
    if not current_task_lock.acquire(blocking=False):
        # já existe transcrição rodando
        raise HTTPException(status_code=429, detail="Servidor ocupado com outra transcrição. Tente novamente.")

    tmp_path = None
    wav_path = None
    try:
        # cria Event para esta transcrição
        stop_event = threading.Event()
        current_stop_event = stop_event

        # salva arquivo recebido
        with tempfile.NamedTemporaryFile(suffix=os.path.splitext(audio.filename)[1] or ".wav", delete=False) as tmp:
            tmp.write(await audio.read())
            tmp.flush()
            tmp_path = tmp.name

        wav_path = convert_to_wav(tmp_path)

        # executa transcrição em thread pool para não bloquear o loop async
        loop = asyncio.get_running_loop()
        aborted, text, segments, detected_language = await loop.run_in_executor(
            None,
            transcribe_with_cancel,
            wav_path,
            stop_event,
            timestamp,
            diferenciar_narrador,
            idioma,
            precisao,
        )

        job_id = str(uuid.uuid4())
        txt_content = build_output_text(
            segments,
            with_timestamp=False,
            with_speaker=diferenciar_narrador,
        )
        srt_content = build_output_text(
            segments,
            with_timestamp=True,
            with_speaker=diferenciar_narrador,
        ) if timestamp else None

        with last_export_lock:
            global last_export_payload
            last_export_payload = {
                "job_id": job_id,
                "txt": txt_content,
                "srt": srt_content,
                "srt_enabled": timestamp,
            }

        if aborted:
            return JSONResponse(
                {
                    "text": text,
                    "aborted": True,
                    "job_id": job_id,
                    "timestamp_enabled": timestamp,
                    "detected_language": detected_language,
                }
            )
        return {
            "text": text,
            "job_id": job_id,
            "timestamp_enabled": timestamp,
            "detected_language": detected_language,
        }

    except ffmpeg.Error as err:
        stderr = err.stderr.decode("utf-8", errors="ignore") if err.stderr else str(err)
        raise HTTPException(status_code=400, detail=f"Falha ao converter áudio: {stderr}")
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Falha na transcrição: {err}")
    finally:
        if tmp_path:
            try:
                os.remove(tmp_path)
            except OSError:
                pass
        if wav_path:
            try:
                os.remove(wav_path)
            except OSError:
                pass
        # libera lock para a próxima transcrição
        if current_task_lock.locked():
            current_task_lock.release()
        current_stop_event = None

@app.post("/stop")
async def stop_processing():
    """
    Endpoint chamado pelo frontend para interromper o processamento atual.
    """
    global current_stop_event
    if current_stop_event is None:
        return {"status": "no_active_task"}
    current_stop_event.set()
    return {"status": "stopping"}


@app.get("/export")
async def export_transcription(job_id: str, formato: str = "txt"):
    with last_export_lock:
        data = last_export_payload

    if data is None or data.get("job_id") != job_id:
        raise HTTPException(status_code=404, detail="Transcrição não encontrada para exportação.")

    fmt = formato.lower().strip()
    if fmt not in ("txt", "srt"):
        raise HTTPException(status_code=400, detail="Formato inválido. Use txt ou srt.")

    if fmt == "srt":
        if not data.get("srt_enabled") or not data.get("srt"):
            raise HTTPException(status_code=400, detail="SRT disponível apenas quando timestamp está ativado.")
        content = data["srt"]
        media_type = "application/x-subrip"
        filename = f"transcricao_{job_id}.srt"
    else:
        content = data.get("txt", "")
        media_type = "text/plain; charset=utf-8"
        filename = f"transcricao_{job_id}.txt"

    return Response(
        content=content,
        media_type=media_type,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )

@app.get("/")
def home():
    return FileResponse("frontend/index.html")
