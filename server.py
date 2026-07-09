from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
import httpx
import asyncio
from datetime import datetime
import logging
import json
import os

app = FastAPI()

# ==============================
# ✅ CORS (allow frontend calls)
# ==============================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==============================
# 🪵 LOGGING CONFIG
# ==============================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger("MCP_SERVER_V2")

# Shared async client
client = httpx.AsyncClient(timeout=10)


# ==============================
# 🔧 SAFE ASYNC REQUEST HELPERS
# ==============================

async def safe_get_json(url, method="GET", json_body=None, timeout=10):
    logger.info(f"➡️ {method} JSON: {url}")
    try:
        if method == "POST":
            res = await client.post(url, json=json_body, timeout=timeout)
        else:
            res = await client.get(url, timeout=timeout)
        logger.info(f"⬅️ Status: {res.status_code}")
        if res.status_code != 200:
            logger.error(f"❌ Bad status: {res.status_code}")
            return None
        if not res.text.strip():
            logger.error("❌ Empty response")
            return None
        return res.json()
    except httpx.ReadTimeout:
        logger.error("⏳ Timeout")
        return None
    except Exception as e:
        logger.exception(f"❌ JSON Error: {e}")
        return None


async def safe_get_text(url, timeout=10):
    logger.info(f"➡️ GET TEXT: {url}")
    try:
        res = await client.get(url, timeout=timeout)
        logger.info(f"⬅️ Status: {res.status_code}")
        if res.status_code != 200:
            logger.error(f"❌ Text status: {res.status_code}")
            return None
        return res.text
    except httpx.ReadTimeout:
        logger.error("⏳ Text timeout")
        return None
    except Exception as e:
        logger.exception(f"❌ TEXT Error: {e}")
        return None


# ==============================
# 🌍 COORDINATES — MULTIPLE SOURCES
# ==============================

async def get_coordinates_openmeteo(city):
    """Primary: Open-Meteo Geocoding"""
    url = f"https://geocoding-api.open-meteo.com/v1/search?name={city}&count=1"
    res = await safe_get_json(url)
    if not res or "results" not in res or not res["results"]:
        return None
    data = res["results"][0]
    return {
        "city": data.get("name"),
        "country": data.get("country"),
        "latitude": data.get("latitude"),
        "longitude": data.get("longitude"),
        "timezone": data.get("timezone")
    }


async def get_coordinates_nominatim(city):
    """Fallback 1: OpenStreetMap Nominatim"""
    url = f"https://nominatim.openstreetmap.org/search?q={city}&format=json&limit=1"
    headers = {"User-Agent": "GeoBot-MCP/2.0"}
    try:
        res = await client.get(url, headers=headers, timeout=10)
        if res.status_code != 200:
            return None
        data = res.json()
        if not data or len(data) == 0:
            return None
        place = data[0]
        return {
            "city": place.get("display_name", "").split(",")[0],
            "country": place.get("display_name", "").split(",")[-1].strip(),
            "latitude": float(place.get("lat")),
            "longitude": float(place.get("lon")),
            "timezone": "UTC"
        }
    except Exception as e:
        logger.warning(f"Nominatim failed: {e}")
        return None


async def get_coordinates(city):
    """Try multiple coordinate sources with fallback"""
    logger.info(f"🌍 Fetching coordinates for: {city}")
    sources = [
        ("Open-Meteo", get_coordinates_openmeteo),
        ("Nominatim", get_coordinates_nominatim),
    ]
    for name, fn in sources:
        try:
            logger.info(f"🌍 Trying {name}...")
            result = await fn(city)
            if result and result.get("latitude") and result.get("longitude"):
                logger.info(f"✅ {name} success: {result['city']}, {result['country']}")
                return result
        except Exception as e:
            logger.warning(f"❌ {name} failed: {e}")
    logger.error("❌ All coordinate sources failed")
    return None


# ==============================
# 💧 HUMIDITY — MULTIPLE SOURCES
# ==============================

async def get_humidity_openmeteo(lat, lon):
    """Primary: Open-Meteo Humidity"""
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=relative_humidity_2m"
    res = await safe_get_json(url)
    if not res or "current" not in res:
        return None
    current = res["current"]
    return {
        "humidity_percent": current.get("relative_humidity_2m"),
        "source": "openmeteo"
    }


async def get_humidity_7timer(lat, lon):
    """Fallback 1: 7Timer Humidity"""
    url = f"https://www.7timer.info/bin/api.pl?lon={lon}&lat={lat}&product=civil&output=json"
    try:
        res = await safe_get_json(url, timeout=8)
        if not res or "dataseries" not in res or not res["dataseries"]:
            return None
        current = res["dataseries"][0]
        rh = current.get("rh2m")
        humidity = None
        if isinstance(rh, str):
            if "%" in rh:
                humidity = int(rh.replace("%", ""))
            elif rh.isdigit():
                humidity = int(rh)
        elif isinstance(rh, (int, float)):
            humidity = rh
        return {
            "humidity_percent": humidity,
            "source": "7timer"
        }
    except Exception as e:
        logger.warning(f"7Timer humidity failed: {e}")
        return None


async def get_humidity(lat, lon):
    """Try multiple humidity sources with fallback"""
    logger.info(f"💧 Fetching humidity for: {lat}, {lon}")
    sources = [
        ("Open-Meteo Humidity", get_humidity_openmeteo),
        ("7Timer Humidity", get_humidity_7timer),
    ]
    for name, fn in sources:
        try:
            logger.info(f"💧 Trying {name}...")
            result = await fn(lat, lon)
            if result and result.get("humidity_percent") is not None:
                logger.info(f"✅ {name} success: {result['humidity_percent']}%")
                return result
        except Exception as e:
            logger.warning(f"❌ {name} failed: {e}")
    logger.warning("⚠️ All humidity sources failed")
    return None


# ==============================
# ☀️ UV INDEX — MULTIPLE SOURCES
# ==============================

async def get_uv_openmeteo(lat, lon):
    """Primary: Open-Meteo UV Index"""
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=uv_index"
    res = await safe_get_json(url)
    if not res or "current" not in res:
        return None
    current = res["current"]
    uv = current.get("uv_index")
    if uv is None:
        return None
    risk = "Low"
    if uv >= 11:
        risk = "Extreme"
    elif uv >= 8:
        risk = "Very High"
    elif uv >= 6:
        risk = "High"
    elif uv >= 3:
        risk = "Moderate"
    return {
        "uv_index": uv,
        "uv_risk": risk,
        "source": "openmeteo"
    }


async def get_uv_openweather(lat, lon):
    """Fallback 1: OpenWeatherMap UV (requires free API key)"""
    api_key = os.environ.get("OPENWEATHER_API_KEY")
    if not api_key:
        return None
    url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&appid={api_key}"
    try:
        res = await safe_get_json(url, timeout=8)
        if not res or "current" not in res:
            return None
        current = res["current"]
        uv = current.get("uvi")
        if uv is None:
            return None
        risk = "Low"
        if uv >= 11:
            risk = "Extreme"
        elif uv >= 8:
            risk = "Very High"
        elif uv >= 6:
            risk = "High"
        elif uv >= 3:
            risk = "Moderate"
        return {
            "uv_index": uv,
            "uv_risk": risk,
            "source": "openweather"
        }
    except Exception as e:
        logger.warning(f"OpenWeather UV failed: {e}")
        return None


async def get_uv(lat, lon):
    """Try multiple UV sources with fallback"""
    logger.info(f"☀️ Fetching UV index for: {lat}, {lon}")
    sources = [
        ("Open-Meteo UV", get_uv_openmeteo),
        ("OpenWeather UV", get_uv_openweather),
    ]
    for name, fn in sources:
        try:
            logger.info(f"☀️ Trying {name}...")
            result = await fn(lat, lon)
            if result and result.get("uv_index") is not None:
                logger.info(f"✅ {name} success: UV {result['uv_index']} ({result['uv_risk']})")
                return result
        except Exception as e:
            logger.warning(f"❌ {name} failed: {e}")
    logger.warning("⚠️ All UV sources failed")
    return None


# ==============================
# 🌙 MOON PHASE — MULTIPLE SOURCES
# ==============================

async def get_moon_phase_freeastro():
    """Primary: FreeAstroAPI Moon Phase"""
    url = "https://api.freeastroapi.com/api/v1/moon/phase"
    try:
        res = await safe_get_json(url, timeout=8)
        if not res or "phase" not in res:
            return None
        phase = res["phase"]
        return {
            "phase_name": phase.get("name"),
            "illumination_percent": round(phase.get("illumination", 0) * 100, 1) if phase.get("illumination") else None,
            "age_days": round(phase.get("age_days", 0), 1) if phase.get("age_days") else None,
            "is_waxing": phase.get("is_waxing"),
            "source": "freeastro"
        }
    except Exception as e:
        logger.warning(f"FreeAstro moon failed: {e}")
        return None


async def get_moon_phase_phaseoftoday():
    """Fallback 1: PhaseOfTheMoonToday"""
    url = "https://api.phaseofthemoontoday.com/v1/current"
    try:
        res = await safe_get_json(url, timeout=8)
        if not res or "phase" not in res:
            return None
        return {
            "phase_name": res.get("phase"),
            "illumination_percent": res.get("illumination"),
            "days_since_new": res.get("days_since_new"),
            "next_full_moon": res.get("next_full_moon"),
            "next_new_moon": res.get("next_new_moon"),
            "source": "phaseoftoday"
        }
    except Exception as e:
        logger.warning(f"PhaseOfToday moon failed: {e}")
        return None


async def get_moon_phase():
    """Try multiple moon phase sources with fallback"""
    logger.info("🌙 Fetching moon phase...")
    sources = [
        ("FreeAstro Moon", get_moon_phase_freeastro),
        ("PhaseOfToday Moon", get_moon_phase_phaseoftoday),
    ]
    for name, fn in sources:
        try:
            logger.info(f"🌙 Trying {name}...")
            result = await fn()
            if result and result.get("phase_name"):
                logger.info(f"✅ {name} success: {result['phase_name']}")
                return result
        except Exception as e:
            logger.warning(f"❌ {name} failed: {e}")
    logger.warning("⚠️ All moon phase sources failed")
    return None


# ==============================
# 🌡️ SOLAR RADIATION — MULTIPLE SOURCES
# ==============================

async def get_solar_radiation_openmeteo(lat, lon):
    """Primary: Open-Meteo Solar Radiation"""
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=shortwave_radiation"
    res = await safe_get_json(url)
    if not res or "current" not in res:
        return None
    current = res["current"]
    sw = current.get("shortwave_radiation")
    if sw is None:
        return None
    return {
        "solar_radiation_w_m2": sw,
        "source": "openmeteo"
    }


async def get_solar_radiation(lat, lon):
    """Try solar radiation sources with fallback"""
    logger.info(f"🌡️ Fetching solar radiation for: {lat}, {lon}")
    sources = [
        ("Open-Meteo Solar", get_solar_radiation_openmeteo),
    ]
    for name, fn in sources:
        try:
            logger.info(f"🌡️ Trying {name}...")
            result = await fn(lat, lon)
            if result and result.get("solar_radiation_w_m2") is not None:
                logger.info(f"✅ {name} success: {result['solar_radiation_w_m2']} W/m²")
                return result
        except Exception as e:
            logger.warning(f"❌ {name} failed: {e}")
    logger.warning("⚠️ All solar radiation sources failed")
    return None


# ==============================
# 🌫️ PRESSURE — MULTIPLE SOURCES
# ==============================

async def get_pressure_openmeteo(lat, lon):
    """Primary: Open-Meteo Pressure"""
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=surface_pressure"
    res = await safe_get_json(url)
    if not res or "current" not in res:
        return None
    current = res["current"]
    pressure = current.get("surface_pressure")
    if pressure is None:
        return None
    return {
        "pressure_hpa": pressure,
        "source": "openmeteo"
    }


async def get_pressure(lat, lon):
    """Try pressure sources with fallback"""
    logger.info(f"🌫️ Fetching pressure for: {lat}, {lon}")
    sources = [
        ("Open-Meteo Pressure", get_pressure_openmeteo),
    ]
    for name, fn in sources:
        try:
            logger.info(f"🌫️ Trying {name}...")
            result = await fn(lat, lon)
            if result and result.get("pressure_hpa") is not None:
                logger.info(f"✅ {name} success: {result['pressure_hpa']} hPa")
                return result
        except Exception as e:
            logger.warning(f"❌ {name} failed: {e}")
    logger.warning("⚠️ All pressure sources failed")
    return None


# ==============================
# 👁️ VISIBILITY — MULTIPLE SOURCES
# ==============================

async def get_visibility_openmeteo(lat, lon):
    """Primary: Open-Meteo Visibility"""
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=visibility"
    res = await safe_get_json(url)
    if not res or "current" not in res:
        return None
    current = res["current"]
    vis = current.get("visibility")
    if vis is None:
        return None
    return {
        "visibility_meters": vis,
        "visibility_km": round(vis / 1000, 1),
        "source": "openmeteo"
    }


async def get_visibility(lat, lon):
    """Try visibility sources with fallback"""
    logger.info(f"👁️ Fetching visibility for: {lat}, {lon}")
    sources = [
        ("Open-Meteo Visibility", get_visibility_openmeteo),
    ]
    for name, fn in sources:
        try:
            logger.info(f"👁️ Trying {name}...")
            result = await fn(lat, lon)
            if result and result.get("visibility_meters") is not None:
                logger.info(f"✅ {name} success: {result['visibility_km']} km")
                return result
        except Exception as e:
            logger.warning(f"❌ {name} failed: {e}")
    logger.warning("⚠️ All visibility sources failed")
    return None


# ==============================
# 💨 DEW POINT — MULTIPLE SOURCES
# ==============================

async def get_dewpoint_openmeteo(lat, lon):
    """Primary: Open-Meteo Dew Point"""
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=dew_point_2m"
    res = await safe_get_json(url)
    if not res or "current" not in res:
        return None
    current = res["current"]
    dew = current.get("dew_point_2m")
    if dew is None:
        return None
    return {
        "dew_point_celsius": dew,
        "source": "openmeteo"
    }


async def get_dewpoint(lat, lon):
    """Try dew point sources with fallback"""
    logger.info(f"💨 Fetching dew point for: {lat}, {lon}")
    sources = [
        ("Open-Meteo Dew Point", get_dewpoint_openmeteo),
    ]
    for name, fn in sources:
        try:
            logger.info(f"💨 Trying {name}...")
            result = await fn(lat, lon)
            if result and result.get("dew_point_celsius") is not None:
                logger.info(f"✅ {name} success: {result['dew_point_celsius']}°C")
                return result
        except Exception as e:
            logger.warning(f"❌ {name} failed: {e}")
    logger.warning("⚠️ All dew point sources failed")
    return None


# ==============================
# ☁️ CLOUD COVER — MULTIPLE SOURCES
# ==============================

async def get_cloudcover_openmeteo(lat, lon):
    """Primary: Open-Meteo Cloud Cover"""
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=cloud_cover"
    res = await safe_get_json(url)
    if not res or "current" not in res:
        return None
    current = res["current"]
    cloud = current.get("cloud_cover")
    if cloud is None:
        return None
    return {
        "cloud_cover_percent": cloud,
        "source": "openmeteo"
    }


async def get_cloudcover(lat, lon):
    """Try cloud cover sources with fallback"""
    logger.info(f"☁️ Fetching cloud cover for: {lat}, {lon}")
    sources = [
        ("Open-Meteo Cloud Cover", get_cloudcover_openmeteo),
    ]
    for name, fn in sources:
        try:
            logger.info(f"☁️ Trying {name}...")
            result = await fn(lat, lon)
            if result and result.get("cloud_cover_percent") is not None:
                logger.info(f"✅ {name} success: {result['cloud_cover_percent']}%")
                return result
        except Exception as e:
            logger.warning(f"❌ {name} failed: {e}")
    logger.warning("⚠️ All cloud cover sources failed")
    return None


# ==============================
# ♻️ KEEP-ALIVE (Self-ping to prevent Render sleep)
# ==============================

SELF_URL = os.environ.get("SELF_URL", "https://your-new-server.onrender.com/tool")
KEEP_ALIVE_INTERVAL = 540  # 9 minutes

async def keep_alive_loop():
    logger.info(f"♻️ Keep-alive started — pinging {SELF_URL} every {KEEP_ALIVE_INTERVAL // 60} minutes")
    await asyncio.sleep(30)
    while True:
        try:
            async with httpx.AsyncClient(timeout=15) as ping_client:
                response = await ping_client.post(
                    SELF_URL,
                    json={"tool": "healthCheck"},
                    headers={"Content-Type": "application/json"}
                )
                if response.status_code == 200:
                    logger.info("♻️ Keep-alive ping successful")
                else:
                    logger.warning(f"♻️ Keep-alive ping returned status {response.status_code}")
        except Exception as e:
            logger.warning(f"♻️ Keep-alive ping failed: {e}")
        await asyncio.sleep(KEEP_ALIVE_INTERVAL)


@app.on_event("startup")
async def startup_event():
    asyncio.create_task(keep_alive_loop())
    logger.info("🚀 MCP Server V2 startup complete — keep-alive task registered")


# ==============================
# 🧠 TOOL HANDLER
# ==============================

@app.post("/tool")
async def tool_handler(request: Request):
    try:
        payload = await request.json()
        logger.info("🔥 MCP SERVER V2 HIT")
        logger.info(json.dumps(payload, indent=2))

        tool = payload.get("tool")
        city = payload.get("input")

        # ❤️ HEALTH CHECK
        if tool == "healthCheck":
            return {
                "status": "ok",
                "server": "MCP V2 ASYNC RUNNING",
                "version": "V2-NEW-FEATURES",
                "features": {
                    "coordinates": ["openmeteo", "nominatim"],
                    "humidity": ["openmeteo", "7timer"],
                    "uv_index": ["openmeteo", "openweather"],
                    "moon_phase": ["freeastro", "phaseoftoday"],
                    "solar_radiation": ["openmeteo"],
                    "pressure": ["openmeteo"],
                    "visibility": ["openmeteo"],
                    "dew_point": ["openmeteo"],
                    "cloud_cover": ["openmeteo"]
                }
            }

        if not city:
            return {"error": "No city provided"}

        coord = await get_coordinates(city)
        if not coord:
            return {"error": "City not found — tried Open-Meteo and Nominatim"}

        lat = coord["latitude"]
        lon = coord["longitude"]

        # 🚀 PARALLEL EXECUTION (KEY BOOST)
        humidity_task = get_humidity(lat, lon)
        uv_task = get_uv(lat, lon)
        moon_task = get_moon_phase()
        solar_task = get_solar_radiation(lat, lon)
        pressure_task = get_pressure(lat, lon)
        visibility_task = get_visibility(lat, lon)
        dewpoint_task = get_dewpoint(lat, lon)
        cloudcover_task = get_cloudcover(lat, lon)

        humidity, uv, moon, solar, pressure, visibility, dewpoint, cloudcover = await asyncio.gather(
            humidity_task,
            uv_task,
            moon_task,
            solar_task,
            pressure_task,
            visibility_task,
            dewpoint_task,
            cloudcover_task
        )

        result = {
            "source": "MCP_SERVER_V2_NEW_FEATURES",
            "city": coord["city"],
            "country": coord["country"],
            "latitude": lat,
            "longitude": lon
        }

        if humidity:
            result["humidity"] = humidity
        if uv:
            result["uv_index"] = uv
        if moon:
            result["moon_phase"] = moon
        if solar:
            result["solar_radiation"] = solar
        if pressure:
            result["pressure"] = pressure
        if visibility:
            result["visibility"] = visibility
        if dewpoint:
            result["dew_point"] = dewpoint
        if cloudcover:
            result["cloud_cover"] = cloudcover

        logger.info("✅ Final Response:")
        logger.info(json.dumps(result, indent=2))
        return result

    except Exception as e:
        logger.exception(f"💥 CRITICAL ERROR: {e}")
        return {"error": "Internal server error"}
