import os
import json
import base64
import asyncio
import websockets
import time
from datetime import datetime

from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.websockets import WebSocketDisconnect
from twilio.twiml.voice_response import VoiceResponse, Connect
from dotenv import load_dotenv
from twilio.request_validator import RequestValidator

load_dotenv()

# Validator Global numere telefon
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
if not TWILIO_AUTH_TOKEN:
    raise ValueError("Missing TWILIO_AUTH_TOKEN in .env")
validator = RequestValidator(TWILIO_AUTH_TOKEN)

ALLOWED_NUMBERS = set(os.getenv("ALLOWED_NUMBERS", "").split(","))
MAX_SILENCE_SECONDS = 30

# Configuration
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')  # requires OpenAI Realtime API Access
PORT = int(os.getenv('PORT', 5050))
SYSTEM_MESSAGE = """
Ești Aria, un asistent vocal empatic și calm, specializat în suport emoțional prin ancorare în prezent.

ROLUL TĂU:
Ajuți persoana să iasă din gândurile negative prin redirecționarea atenției către lucrurile bune, concrete și reale din viața și mediul lor imediat — acum, în acest moment.

CUM VORBEȘTI:
- Voce caldă, lentă, liniștitoare
- Propoziții scurte
- Întrebi mult — conversația e despre EI, nu despre tine
- Asculți răspunsul și construiești pe el
- Vorbești exclusiv în română

FILOZOFIA DE BAZĂ:
Mintea umană tinde să fie ocupată de probleme și oameni care ne fac rău. 
Scopul tău este să muți atenția spre ce este BUN și PREZENT în viața lor — 
lucruri concrete pe care le au deja, nu abstracte sau viitoare.

FLUXUL CONVERSAȚIEI:

1. SALUT ȘI ASCULTARE
   - "Bună ziua, sunt Aria. Cum te simți astăzi?"
   - Lasă persoana să vorbească. Nu întrerupe.
   - Validezi scurt: "Înțeleg, îmi pare rău că treci prin asta."

2. ANCORARE ÎN PREZENT — ÎNTREBĂRI CONCRETE
   Treci rapid la întrebări practice despre viața lor reală:

   DESPRE OAMENI BUNI:
   - "Spune-mi — există cineva în viața ta care îți face bine când îl vezi sau îi auzi vocea?"
   - "Cine este persoana care te face să zâmbești când te gândești la ea?"
   - "Ai pe cineva aproape — familie, prieten, vecin — care e de încredere?"
   - Dacă răspund: "Când ai vorbit ultima dată cu [numele]? Cum te-ai simțit?"

   DESPRE ANIMALE:
   - "Ai animale acasă? Un câine, o pisică?"
   - "Cum îl cheamă? Ce face de obicei când ajungi acasă?"
   - "Te gândești la el acum — cum te face să te simți?"

   DESPRE NATURĂ ȘI SPAȚII:
   - "Ai o grădină sau o curte? Sau un balcon?"
   - "Ce crește acolo acum? Flori, pomi, iarbă?"
   - "Când ai stat ultima dată afară și ai simțit aerul?"
   - "Dacă ai ieși acum afară, ce ai vedea primul lucru?"

   DESPRE OBIECTE CARE FAC BINE:
   - "Ce obiect din casa ta îți place cel mai mult — ceva care te face să te simți bine când îl privești?"
   - "Ai o ceașcă preferată, o pătură confortabilă, un fotoliu?"
   - "Unde ești acum fizic — stai, ești în picioare?"

   DESPRE MOMENTE MICI BUNE:
   - "Ce ai mâncat azi? A fost ceva bun?"
   - "Când ai dormit ultima dată bine?"
   - "Există un loc unde te simți în siguranță — acasă, la cineva, în natură?"

3. CONSTRUIEȘTI PE RĂSPUNSURI
   - Dacă spune că are un câine: "Închide ochii o secundă și imaginează-ți că [numele câinelui] e lângă tine acum. Ce face?"
   - Dacă spune că are o grădină: "Imaginează-te acolo acum. Ce miroase? Ce culori vezi?"
   - Dacă menționează o persoană dragă: "Dacă i-ai trimite un mesaj acum, ce i-ai spune?"

4. MINIMIZAREA PROBLEMEI
   După ce persoana a vorbit despre lucruri bune, introduci blând:
   - "Observi cum mintea ta poate fi și în alte locuri decât în acea problemă?"
   - "Acea situație există, dar și [câinele/grădina/persoana] există. Ambele sunt reale."
   - "Problema îți ocupă mintea pentru că o lași să stea acolo. Poți să o pui jos pentru câteva minute?"
   - "Nu trebuie să rezolvi nimic acum. Acum ești în siguranță."

5. EXERCIȚIU PRACTIC DE ÎNCHEIERE
   "Hai să facem ceva simplu:
   Spune-mi 3 lucruri pe care le ai și care îți fac bine — 
   poate [persoana menționată], poate [animalul], poate [obiectul].
   Le spui cu voce tare."

   Asculți. Confirmi fiecare.
   "Acestea sunt ale tale. Nimeni nu ți le poate lua."

LIMITE:
- Dacă menționează gânduri de automutilare sau suicid — calm și ferm: 
  "Îți mulțumesc că mi-ai spus. Este important să vorbești cu cineva specializat acum. 
   Sună la 0800 801 200 — Telefonul Speranței, este gratuit și disponibil oricând."
- Nu diagnostichezi
- Nu dai sfaturi medicale
- Nu înlocuiești un specialist

TONUL GENERAL:
Cald. Prezent. Practic. Ca un prieten bun care știe să asculte și să întrebe lucrurile potrivite.
"""
VOICE = 'shimmer'
TEMPERATURE = float(os.getenv('TEMPERATURE', 0.3))
LOG_EVENT_TYPES = [
    'response.content.done', 'rate_limits.updated', 'response.done',
    'input_audio_buffer.committed', 'input_audio_buffer.speech_stopped',
    'input_audio_buffer.speech_started', 'session.created'
]

app = FastAPI()


@app.get("/", response_class=JSONResponse)
async def index_page():
    return {"message": "Twilio Media Stream Server is running!"}


@app.api_route("/incoming-call", methods=["GET", "POST"])
async def handle_incoming_call(request: Request):
    form = await request.form()
    caller = form.get("From")
    print("Incoming call from:", caller)

    # --- VALIDARE TWILIO SIGNATURE ---
    url = str(request.url)
    signature = request.headers.get("X-Twilio-Signature", "")
    is_valid = validator.validate(
        url,
        dict(form),
        signature
    )
    if not is_valid:
        print("❌ Invalid Twilio signature!")
        return JSONResponse(status_code=403, content={"error": "Forbidden"})

    # --- Verificare WHITELIST ---
    response = VoiceResponse()

    if caller not in ALLOWED_NUMBERS:
        response.say("Ne pare rau, acest numar nu are acces.", language="ro-RO")
        response.hangup()
        return HTMLResponse(content=str(response), media_type="application/xml")

    response.say(
        "Buna ziua, spune-mi te rog cu ce te pot ajuta",
        language="ro-RO"
    )

    host = request.url.hostname
    connect = Connect()
    connect.stream(url=f'wss://asistent-563ac748a576.herokuapp.com/media-stream')
    response.append(connect)

    return HTMLResponse(content=str(response), media_type="application/xml")


@app.websocket("/media-stream")
async def handle_media_stream(websocket: WebSocket):
    """Handle WebSocket connections between Twilio and OpenAI."""
    print("Client connected")
    await websocket.accept()
    async with websockets.connect(
            f"wss://api.openai.com/v1/realtime?model=gpt-realtime-mini&temperature={TEMPERATURE}",
            additional_headers={
                "Authorization": f"Bearer {OPENAI_API_KEY}"
            }
    ) as openai_ws:
        await send_session_update(openai_ws)
        stream_sid = None
        deadline = None
        call_active = True

        async def receive_from_twilio():
            """Receive audio data from Twilio and send it to the OpenAI Realtime API."""
            nonlocal stream_sid, deadline
            try:
                async for message in websocket.iter_text():
                    data = json.loads(message)

                    if data['event'] == 'media' and openai_ws.state.name == 'OPEN':
                        audio_append = {
                            "type": "input_audio_buffer.append",
                            "audio": data['media']['payload']
                        }
                        await openai_ws.send(json.dumps(audio_append))
                    elif data['event'] == 'start':
                        stream_sid = data['start']['streamSid']
                        print(f"Incoming stream has started {stream_sid}")
            except WebSocketDisconnect:
                print("Client disconnected.")

                if openai_ws.state.name == 'OPEN':
                    await openai_ws.close()

        async def send_to_twilio():
            """Receive events from the OpenAI Realtime Mini API, send audio back to Twilio."""
            nonlocal stream_sid, deadline
            try:
                async for openai_message in openai_ws:
                    response = json.loads(openai_message)

                    if response['type'] == 'input_audio_buffer.speech_started':
                        print("User started speaking. Cancelling idle timer.")
                        deadline = None

                    if response['type'] == 'response.done':
                        now = datetime.now()
                        formatted_time = now.strftime("%H:%M:%S")

                        print(
                            f"[{formatted_time}] Agent finished speaking. Starting idle timer ({MAX_SILENCE_SECONDS}s).")

                        deadline = time.time() + MAX_SILENCE_SECONDS

                    if response['type'] == 'session.updated':
                        print("SESSION UPDATED SUCCESFULLY:", response)

                    if response['type'] == 'response.output_audio.delta' and response.get('delta'):
                        # Audio from OpenAI
                        try:
                            audio_payload = base64.b64encode(base64.b64decode(response['delta'])).decode('utf-8')
                            audio_delta = {
                                "event": "media",
                                "streamSid": stream_sid,
                                "media": {
                                    "payload": audio_payload
                                }
                            }
                            await websocket.send_json(audio_delta)
                        except Exception as e:
                            print(f"Error processing audio data: {e}")
            except Exception as e:
                print(f"Error in send_to_twilio: {e}")

        async def idle_watcher():
            nonlocal deadline, call_active
            while call_active:
                await asyncio.sleep(3)

                if deadline and time.time() > deadline:
                    print("⏰ Idle timeout reached. Closing call.")
                    call_active = False

                    try:
                        await websocket.close()
                    except:
                        pass

                    if openai_ws.state.name == 'OPEN':
                        await openai_ws.close()

                    break

        await asyncio.gather(
            receive_from_twilio(),
            send_to_twilio(),
            idle_watcher()
        )


async def send_session_update(openai_ws):
    """Send session update to OpenAI WebSocket."""
    session_update = {
        "type": "session.update",
        "session": {
            "type": "realtime",
            "model": "gpt-realtime",
            "output_modalities": ["audio"],
            "audio": {
                "input": {
                    "format": {"type": "audio/pcmu"},
                    "turn_detection": {
                        "type": "server_vad",
                        "silence_duration_ms": 1600
                    }
                },
                "output": {
                    "format": {"type": "audio/pcmu"},
                    "voice": VOICE
                }
            },
            "instructions": SYSTEM_MESSAGE,
        }
    }
    print('Sending session update:', json.dumps(session_update))
    await openai_ws.send(json.dumps(session_update))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
