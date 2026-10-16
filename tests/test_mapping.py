#!/usr/bin/env python3
"""Vérifie la traduction capabilities -> entités MQTT Discovery, hors réseau.

Ne demande ni broker ni identifiants : le jeu d'essai est un four vapeur
synthétique qui réunit les cas que le mapper doit traiter correctement.

    python tests/test_mapping.py
"""

from __future__ import annotations

import configparser
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import electrolux2mqtt as e2m  # noqa: E402

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "steam_oven.json")

failures: list[str] = []


def check(condition: bool, message: str) -> None:
    if condition:
        print(f"  ok   {message}")
    else:
        failures.append(message)
        print(f"  FAIL {message}")


def build(**options: str):
    """Construit un pont et l'appareil d'essai, sans aucune connexion."""
    fixture = json.load(open(FIXTURE, encoding="utf-8"))
    config = configparser.ConfigParser()
    for section in ("electrolux", "mqtt", "log"):
        config.add_section(section)
    for key, value in options.items():
        config.set("electrolux", key, value)
    bridge = e2m.Bridge(config)
    state = fixture["_state"]
    app = e2m.ApplianceBridge(
        appliance_id=state["applianceId"],
        slug=e2m.slugify(state["applianceId"]),
        name="Four",
        model=fixture["applianceInfo"]["model"],
        brand=fixture["applianceInfo"]["brand"],
        base_topic="electrolux/four",
        availability_topic="electrolux/four/availability",
        capabilities=fixture["capabilities"],
    )
    app.reported = dict(state["properties"]["reported"])
    app.unit = bridge._resolve_unit(app)
    bridge._build_entities(app)
    return bridge, app


print("Construction des entités")
bridge, app = build()
entities = app.entities
for key in sorted(entities):
    entity = entities[key]
    value = entity.render(app.reported) if entity.render else None
    print(f"    {entity.component:14s} {key:38s} {entity.name:28s} = {value}")

print("\nStructure")
check("userselections_steamlevel" in entities,
      "un conteneur portant lui-même un access est bien parcouru")
check("userselections" not in entities,
      "le conteneur lui-même ne devient pas une entité")
check("keymodel" not in entities, "une propriété constante est ignorée")
check(not any(k.startswith("appliancecareandmaintenance") for k in entities),
      "les compteurs de maintenance sont écartés par défaut")
check("networkinterface_startupcommand" not in entities,
      "les commandes d'administration réseau sont écartées")
check("networkinterface_linkqualityindicator" in entities,
      "la qualité du lien réseau est conservée")

print("\nUnité de température")
check(app.unit == "CELSIUS", "l'unité suit temperatureRepresentation")
check("targettemperaturec" in entities and "targettemperaturef" not in entities,
      "une seule unité est publiée")
_, app_f = build(temperature_unit="F")
check("targettemperaturef" in app_f.entities and "targettemperaturec" not in app_f.entities,
      "temperature_unit=F force le Fahrenheit")

print("\nComposants")
check(entities["program"].component == "select", "une énumération inscriptible est un select")
check(entities["program"].config["options"] == ["TRUE_FAN", "STEAM_FULL", "GRILL"],
      "les valeurs désactivées sont retirées des options")
check(entities["appliancestate"].component == "sensor", "une énumération en lecture est un sensor")
check(entities["cavitylight"].component == "switch", "un booléen inscriptible est un switch")
check(entities["doorstate"].component == "binary_sensor", "la porte est un binaire")
check(entities["doorstate"].config.get("device_class") == "door",
      "la porte garde son device_class")
check(entities["targettemperaturec"].component == "number", "une consigne est un number")
check(entities["targettemperaturec"].config["max"] == 300, "les bornes viennent des capabilities")
check(entities["displaytemperaturec"].component == "sensor", "une mesure reste un sensor")
check(entities["displaytemperaturec"].build_command is None, "une mesure n'a pas de commande")

print("\nBoutons")
check(entities["executecommand_start"].name == "Démarrer", "START devient « Démarrer »")
check(entities["executecommand_stopreset"].name == "Arrêter", "STOPRESET devient « Arrêter »")
check(entities["executecommand_start"].has_state is False, "un bouton n'a pas d'état")

print("\nValeurs publiées")
check(entities["displaytemperaturec"].render(app.reported) == "172.4", "une mesure est arrondie")
check(entities["cavitylight"].render(app.reported) == "ON", "un booléen devient ON")
check(entities["doorstate"].render(app.reported) == "OFF", "porte fermée -> OFF")
check(entities["alerts"].render(app.reported) == "42", "les alertes sont mises à plat")
check(entities["timetoend_end"].render(app.reported) != "—", "l'heure de fin est calculée")
check(entities["timetoend_end"].render({"timeToEnd": 0}) == "—", "pas d'heure de fin à l'arrêt")

print("\nCommandes")
check(entities["targettemperaturec"].build_command("185") == {"targetTemperatureC": 185.0},
      "une consigne produit le bon corps de commande")
check(entities["userselections_steamlevel"].build_command("HIGH")
      == {"userSelections": {"steamLevel": "HIGH"}},
      "une propriété imbriquée produit un corps imbriqué")
check(entities["cavitylight"].build_command("OFF") == {"cavityLight": False},
      "un switch envoie un booléen JSON")
check(entities["targetduration"].build_command("1800") == {"targetDuration": 1800},
      "une durée reste entière")
check(entities["executecommand_start"].build_command("PRESS") == {"executeCommand": "START"},
      "un bouton envoie sa valeur fixe")

print("\nMode lecture seule")
_, app_ro = build(read_only="true")
check(all(e.build_command is None for e in app_ro.entities.values()),
      "read_only=true n'expose aucune commande")
check(app_ro.entities["program"].component == "sensor",
      "read_only=true dégrade les select en sensor")

print("\nDécouverte MQTT")
payload = json.loads(
    json.dumps(
        {
            "name": entities["program"].name,
            "unique_id": f"electrolux_{app.slug}_program",
            "device": bridge._device_block(app),
            **entities["program"].config,
        }
    )
)
check(payload["device"]["identifiers"] == [f"electrolux_{app.slug}"],
      "le bloc device regroupe les entités en un seul équipement")
check(bridge._discovery_topic(app, entities["program"])
      == f"homeassistant/select/electrolux_{app.slug}/program/config",
      "le topic de découverte est bien formé")
check(app.command_topic(entities["program"]) == "electrolux/four/program/set",
      "le topic de commande est bien formé")

print(f"\n{len(entities)} entités construites")
# --- Regroupement des commandes : un lot = une requête, ordre conservé --------
merged = e2m.merge_command_bodies([
    {"upperOven": {"program": "TRUE_FAN"}},
    {"upperOven": {"targetTemperatureC": 180}},
    {"upperOven": {"startTime": 120}},
    {"upperOven": {"executeCommand": "START"}},
    {"upperOven": {"targetTemperatureC": 200}},
])
check(merged == {"upperOven": {"program": "TRUE_FAN", "targetTemperatureC": 200,
                               "startTime": 120, "executeCommand": "START"}},
      "les corps de commande d'une même cavité fusionnent, dernière valeur gagnante")
check(list(merged["upperOven"]) == ["program", "targetTemperatureC", "startTime", "executeCommand"],
      "l'ordre de réception est conservé (START en dernier)")
check(e2m.merge_command_bodies([{"energySavingMode": "ON"}, {"upperOven": {"program": "BAKE"}}])
      == {"energySavingMode": "ON", "upperOven": {"program": "BAKE"}},
      "racine et cavité cohabitent dans un même lot")
check(e2m.merge_command_bodies([]) == {}, "lot vide")
check(0 < e2m.COMMAND_COALESCE_SECONDS <= 1, "fenêtre de regroupement courte")

# --- Évènements SSE : une propriété par évènement, chemin « a/b » ------------
check(e2m.property_delta("upperOven/cavityLight", False) == {"upperOven": {"cavityLight": False}},
      "un chemin à deux niveaux devient un dictionnaire imbriqué")
check(e2m.property_delta("applianceState", "RUNNING") == {"applianceState": "RUNNING"},
      "une propriété racine reste à la racine")
event = {"applianceId": app.appliance_id, "property": "upperOven/applianceState",
         "userId": "u", "value": "RUNNING"}
check(bridge._extract_reported(event) == {"upperOven": {"applianceState": "RUNNING"}},
      "l'évènement du flux est traduit en delta d'état (forme relevée le 2026-09-11)")
check(bridge._extract_reported({"properties": {"reported": {"upperOven": {"program": "BAKE"}}}})
      == {"upperOven": {"program": "BAKE"}},
      "l'enveloppe REST reste acceptée")
check(bridge._extract_reported({"applianceId": "x", "userId": "u", "timestamp": 1})
      == {}, "une enveloppe sans contenu ne pollue pas l'état")
before = dict(app.reported)
e2m.deep_merge(app.reported, bridge._extract_reported(event))
check(e2m.deep_get(app.reported, ("upperOven", "applianceState")) == "RUNNING"
      and "property" not in app.reported and "value" not in app.reported,
      "après fusion, l'état porte la propriété et aucune clé d'enveloppe")

# --- Capabilities déclarées à plat (lave-linge, issue #1) ---------------------
flat_app = e2m.ApplianceBridge(
    appliance_id="wm", slug="wm", name="Lave-linge", model="EW7F3816DB",
    brand="ELECTROLUX", base_topic="electrolux/wm",
    availability_topic="electrolux/wm/availability",
    capabilities={
        "userSelections": {"access": "readwrite", "type": "complex"},
        "userSelections/programUID": {"access": "read", "type": "string"},
        "fCMiscellaneousState/waterUsage": {"access": "read", "type": "number"},
    },
)
flat_app.reported = {"userSelections": {"programUID": "COTTON_PR_COTTONS"},
                     "fCMiscellaneousState": {"waterUsage": 46}}
bridge._build_entities(flat_app)
check(flat_app.entities["userselections_programuid"].render(flat_app.reported)
      == "COTTON_PR_COTTONS", "une clé « a/b » lit l'état imbriqué")
check(flat_app.entities["fcmiscellaneousstate_waterusage"].render(flat_app.reported)
      == "46", "une mesure sous une clé à plat est publiée")

# --- Premier démarrage sur refresh token seul (forum 151203) ------------------
import asyncio  # noqa: E402
import tempfile  # noqa: E402

with tempfile.TemporaryDirectory() as tmp:
    token_file = os.path.join(tmp, "electrolux_token.json")
    boot = build(api_key="k", refresh_token="R0", token_file=token_file)[0]

    async def fake_bootstrap(refresh):
        return "A1", "R1"

    boot._bootstrap_access_token = fake_bootstrap
    asyncio.run(boot._make_client())
    saved = json.load(open(token_file, encoding="utf-8"))
    check(saved["refresh_token"] == "R1",
          "l'échange initial garde le refresh token neuf, pas celui consommé")

# --- Renouvellement des jetons (forum 151307) --------------------------------
import time  # noqa: E402

import aiohttp  # noqa: E402
import jwt  # noqa: E402


def fake_jwt(seconds: float) -> str:
    return jwt.encode({"exp": int(time.time() + seconds)}, "k" * 32, algorithm="HS256")


def manager(access_seconds: float = -10) -> e2m.RotatingTokenManager:
    return e2m.RotatingTokenManager(
        access_token=fake_jwt(access_seconds), refresh_token="R1", api_key="k"
    )


calls: list[str] = []


async def fake_ok(method, url, json_body=None, headers=None):
    calls.append(json_body["refreshToken"])
    await asyncio.sleep(0.05)
    return {"accessToken": fake_jwt(43200), "refreshToken": f"R{len(calls) + 1}"}


async def fake_401(method, url, json_body=None, headers=None):
    calls.append(json_body["refreshToken"])
    raise aiohttp.ClientResponseError(
        request_info=None, history=(), status=401, message="Invalid grant provided"
    )


real_request = e2m.request


async def concurrent_refresh(tokens):
    return await asyncio.gather(tokens.refresh_token(), tokens.refresh_token())


try:
    e2m.request = fake_ok
    tokens = manager()
    results = asyncio.run(concurrent_refresh(tokens))
    check(results == [True, True] and calls == ["R1"],
          "deux renouvellements simultanés n'envoient qu'une requête")
    check(tokens._auth_data.refresh_token == "R2",
          "le refresh token neuf remplace le consommé")

    calls.clear()
    tokens = manager(access_seconds=3600)
    asyncio.run(tokens.renew_if_needed(e2m.TOKEN_RENEW_MARGIN))
    check(calls == [], "pas de renouvellement à plus de 5 min de l'expiration")
    tokens = manager(access_seconds=120)
    asyncio.run(tokens.renew_if_needed(e2m.TOKEN_RENEW_MARGIN))
    check(calls == ["R1"], "renouvellement anticipé à moins de 5 min de l'expiration")

    calls.clear()
    e2m.request = fake_401
    tokens = manager()
    first = asyncio.run(tokens.refresh_token())
    second = asyncio.run(tokens.refresh_token())
    check(not first and not second and tokens.rejected.is_set() and calls == ["R1"],
          "un refus définitif est retenu : plus aucun appel ensuite")
finally:
    e2m.request = real_request

with tempfile.TemporaryDirectory() as tmp:
    token_file = os.path.join(tmp, "electrolux_token.json")
    json.dump({"api_key": "k", "access_token": "A5", "refresh_token": "R5",
               "seed": e2m.token_fingerprint("R0")},
              open(token_file, "w", encoding="utf-8"))
    same = build(api_key="k", refresh_token="R0", token_file=token_file)[0]
    check(same._load_tokens()[2] == "R5",
          "le fichier l'emporte tant que la configuration n'a pas changé")
    fresh = build(api_key="k", refresh_token="R9", token_file=token_file)[0]
    check(fresh._load_tokens()[2] == "R9",
          "un nouveau couple saisi dans la configuration l'emporte sur le fichier")

    dead = build(api_key="k", refresh_token="R0", token_file=token_file)[0]
    dead._load_tokens()
    dead._tokens = e2m.RotatingTokenManager(
        access_token="A5", refresh_token="R5", api_key="k"
    )
    dead._mark_rejected("HTTP 401 : Invalid grant provided")
    try:
        build(api_key="k", refresh_token="R0", token_file=token_file)[0]._load_tokens()
        check(False, "un couple refusé n'est pas représenté au redémarrage")
    except e2m.TokensRejected:
        check(True, "un couple refusé n'est pas représenté au redémarrage")
    check(build(api_key="k", refresh_token="R9", token_file=token_file)[0]
          ._load_tokens()[2] == "R9",
          "un nouveau couple débloque un fichier marqué refusé")

if failures:
    print(f"\n{len(failures)} échec(s) :")
    for failure in failures:
        print(f"  - {failure}")
    sys.exit(1)
print("Tout est vert.")
