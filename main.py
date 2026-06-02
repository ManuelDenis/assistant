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
from tavily import TavilyClient
import random

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
Ești Aria, un asistent vocal empatic și calm.

ROLUL TĂU:
Raspunzi la intrebari, daca este o intrebare evaziva fara un obiectiv clar, oferi perspective si detalii diferite si dezvolti subiectul.
Dacă utilizatorul cere anumite informații folosește tool-ul search_web înainte să răspunzi.

Nu inventa informații actuale.
CUM VORBEȘTI:
- Voce caldă, lentă, liniștitoare
- Asculți răspunsul și construiești pe el
- Vorbești exclusiv în română

FILOZOFIA DE BAZĂ:
Mintea umană tinde să fie ocupată de probleme și oameni care ne fac rău. 
Scopul tău este să muți atenția spre ce este BUN în viața lor — 
lucruri concrete care dau o stare de bine, dorinte, planuri, diverse placeri, activitati in aer liber, sport, hrana, diverse produse facute acasa- prajituri, placinte, compoturi... etc.
Muta atentia pe amintiri placute, pe incredere, pe oameni despre care amintim cu placere, despre diverse personalitati care apar la tv si radio romania actualitati.  

FLUXUL CONVERSAȚIEI:

1. SALUT ȘI ASCULTARE
   - "Bună ziua, numele meu este Aria. Cum te simți astăzi?"
   - Lasă persoana să vorbească. Nu întrerupe.

LIMITE:
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
        "Alo!",
        language="ro-RO"
    )

    host = request.url.hostname
    connect = Connect()
    connect.stream(url=f'wss://assistant-oqyt.onrender.com/media-stream')
    response.append(connect)

    return HTMLResponse(content=str(response), media_type="application/xml")


WAITING_MESSAGES = [
    "O clipă, caut informațiile pentru tine...",
    "Stai puțin, verific acum...",
    "Imediat, las-mă să caut...",
    "O secundă, mă uit acum...",
    "Bine, caut informația aceasta pentru tine...",
    "Să văd... o clipă te rog...",
    "Stai un moment, verific pentru tine...",
]


async def send_waiting_message(openai_ws):
    """Trimite un mesaj de așteptare audio înainte de search."""
    message = random.choice(WAITING_MESSAGES)
    print(f"⏳ Sending waiting message: {message}")

    waiting_event = {
        "type": "conversation.item.create",
        "item": {
            "type": "message",
            "role": "assistant",
            "content": [{
                "type": "text",
                "text": message
            }]
        }
    }
    await openai_ws.send(json.dumps(waiting_event))
    await openai_ws.send(json.dumps({"type": "response.create"}))


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

                    if response["type"] == "response.function_call_arguments.done":

                        if response["name"] == "search_web":
                            args = json.loads(response["arguments"])

                            # ✅ trimitem mesaj de așteptare
                            await send_waiting_message(openai_ws)

                            # mică pauză să se audă mesajul
                            await asyncio.sleep(0.5)

                            # executam search-ul
                            results = search_web(
                                args["query"]
                            )

                            await openai_ws.send(json.dumps({
                                "type": "conversation.item.create",
                                "item": {
                                    "type": "function_call_output",
                                    "call_id": response["call_id"],
                                    "output": results
                                }
                            }))

                            await openai_ws.send(json.dumps({
                                "type": "response.create"
                            }))

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
            "tools": [
                {
                    "type": "function",
                    "name": "search_web",
                    "description": "Caută informații actuale pe internet.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string"
                            }
                        },
                        "required": ["query"]
                    }
                }
            ]
        }
    }
    print('Sending session update:', json.dumps(session_update))
    await openai_ws.send(json.dumps(session_update))


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)


tavily = TavilyClient(api_key=os.getenv("TAVILY_API_KEY"))


def search_web(query: str):
    result = tavily.search(
        query=query,
        search_depth="advanced",
        max_results=5
    )

    snippets = []

    for r in result["results"]:
        snippets.append(
            f"{r['title']}\n{r['content']}"
        )

    return "\n\n".join(snippets)